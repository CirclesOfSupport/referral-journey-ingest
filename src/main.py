"""
referral-journey-ingest — Cloud Run service.

Populates the `Flow` tab (subscriber journey through the yellow outreach flow)
and self-heals missing `Referrals` rows, by reading the TextIt runs API for the
outreach flow "Request Call for Support (Yellow)" (entry/response/provider) and
the two partner flows (ACMF / V4W) for the actual referral timestamp +
fired/logged status.

It ALSO fills the `outreach occured?` column on the `Referrals` tab from the
separate YES follow-up flow "Request Call for Support (Yellow) YES" — the
"did they reach out to you?" answer collected a few days after a referral. This
folds in two former hand-run one-off back-fills: a disposable nudge-flow
back-fill (now obsolete — the nudges live INSIDE the outreach flow and are read
natively from its runs) and a YES-flow outreach back-fill (now an ongoing
tracked source here).

Design notes:
- The runs API reports each run's FINAL recorded result state, so a late reply
  that TextIt still associates with the run is captured on the next pull. This
  is why the ingest reads runs instead of writing to the sheet from inside the
  flow (in-flow writes would miss late responses).
- Incremental by run `modified_on`, watermarked off the `Flow` tab's
  `last_modified` column (no state table). Full-rebuild via {"rebuild": true}.
- A yes/no answer collected across multiple nudge stages lands under DIFFERENT
  run-result keys (the initial wait vs. the post-second-nudge terminal wait).
  runs.json reports each run's final state, so both keys can be present; the
  extractor coalesces them, preferring the final concrete answer. See
  _coalesce_yesno.
- TextIt REST limit is 2,500 req/hr; every call goes through _textit_get(),
  which parses the 429 "available in N seconds" body and backs off.

Endpoints:
  GET  /health -> {"status":"ok"}
  POST /run    -> incremental (default) or {"rebuild": true}
"""
import os
import re
import time
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, request, jsonify

_ET = ZoneInfo("America/New_York")


def _to_et(iso_utc):
    """UTC ISO ('...Z' or '+00:00') -> ET display string 'YYYY-MM-DD HH:MM'
    (America/New_York, DST-correct). Blank stays blank. Used only for the
    human-facing referral_timestamp / Referrals `timestamp` columns — NOT for
    last_modified/modified_on/created_on, which stay raw UTC for the watermark."""
    if not iso_utc:
        return ""
    try:
        dt = datetime.fromisoformat(str(iso_utc).replace("Z", "+00:00"))
    except ValueError:
        return iso_utc
    return dt.astimezone(_ET).strftime("%Y-%m-%d %H:%M")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("referral-journey-ingest")

app = Flask(__name__)

# ---- config (console env vars, NOT cloudbuild.yaml) ----

# TextIt
TEXTIT_BASE = "https://textit.com/api/v2"
TEXTIT_TOKEN = os.environ.get("TEXTIT_TOKEN")  # set in console

# Flow UUIDs
YELLOW_FLOW = os.environ["YELLOW_FLOW"]  # consolidated outreach flow (yellow + nudges)
ACMF_FLOW = os.environ["ACMF_FLOW"]      # partner referral flow UUID
V4W_FLOW = os.environ["V4W_FLOW"]        # partner referral flow UUID
YES_FLOW = os.environ["YES_FLOW"]        # YES follow-up flow ("did they reach out to you?")

# sheet-service
SHEET_SERVICE = os.environ["SHEET_SERVICE"].rstrip("/")  # tolerate a trailing slash
SHEET_ID = os.environ["SHEET_ID"]
FLOW_TAB = os.environ.get("FLOW_TAB", "flow")
REFERRALS_TAB = os.environ.get("REFERRALS_TAB", "Referrals")
# EXACT header text on the Referrals tab (lowercase, "occured", trailing "?").
# sheet-service maps columns by literal header string, so this must match the
# tab verbatim; overridable in case the header is ever corrected.
OUTREACH_COL = os.environ.get("OUTREACH_COL", "outreach occured?")
SHEET_PASSWORD = os.environ.get("SHEET_PASSWORD")  # gappscriptapi value sheet-service checks

# runtime seatbelt
PAGE_CEILING = int(os.environ.get("PAGE_CEILING", "500"))
# Google Sheets API allows 60 read requests/min/user, and sheet-service reads the
# header row on every /write. Pace writes so a large rebuild cannot exceed it.
# 1.1s => ~54 writes/min, just under the limit. Incremental runs are far smaller
# and unaffected in practice.
WRITE_PACE_SEC = float(os.environ.get("WRITE_PACE_SEC", "1.1"))


# ---------------------------------------------------------------- TextIt reads
def _textit_get(url, params=None):
    """GET with the 2,500/hr rate limit handled: parse the 429 'available in N'
    body and sleep N+3, retry. Never dies on a throttle."""
    headers = {"Authorization": f"Token {TEXTIT_TOKEN}"}
    attempt = 0
    while True:
        attempt += 1
        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code == 429:
            wait = 60
            m = re.search(r"available in (\d+)", r.text)
            if m:
                wait = int(m.group(1)) + 3
            log.warning("textit 429; sleeping %ss (attempt %s)", wait, attempt)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()


def iter_runs(flow_uuid, after=None):
    """Yield runs for a flow, paginated, oldest-first not guaranteed by the API
    so callers must track their own max(modified_on). `after` uses the runs API
    ?after= filter (modified_on) to keep incremental pulls small."""
    params = {"flow": flow_uuid}
    if after:
        params["after"] = after
    url = f"{TEXTIT_BASE}/runs.json"
    pages = 0
    while url:
        pages += 1
        if pages > PAGE_CEILING:
            log.warning("page ceiling %s hit for flow %s", PAGE_CEILING, flow_uuid)
            break
        data = _textit_get(url, params=params)
        for run in data.get("results", []):
            yield run
        url = data.get("next")
        params = None  # next already encodes params


# ---------------------------------------------------------------- result helpers
def _cat(values, key):
    """`values[key].category` or '' — the Yes/No/Other bucket of a wait result.

    The consolidated outreach flow and the YES flow both name their wait-timeout
    branch with the literal category "No Response". That is a non-answer (the
    old separate flow left the category blank on timeout), so it is normalized to
    '' here — the Flow-tab `response` column is Yes / No / Other / blank, and a
    timeout must read as blank, never the text "No Response"."""
    c = (values.get(key) or {}).get("category", "") or ""
    return "" if c == "No Response" else c


def _coalesce_yesno(values, keys):
    """Coalesce a yes/no answer that may be recorded under any of several result
    keys, one per nudge stage. runs.json reports each run's FINAL state, so a
    subscriber who replied only after the second nudge has the EARLIER key blank
    and the answer under the LATER (terminal) key.

    `keys` is ordered EARLIEST-first (e.g. ["result_1", "result"] for the
    consolidated outreach flow, ["result_2", "result_3"] for the YES flow). We
    take a concrete Yes/No from the LATEST stage that has one — that is the
    subscriber's final recorded answer; if no stage has a Yes/No we keep any
    non-blank category (e.g. "Other"); blank across all stages = no response.
    Returns "" for no response.
    """
    answer = ""
    for k in keys:  # earliest -> latest; a later concrete Yes/No wins
        c = _cat(values, k)
        if c in ("Yes", "No"):
            answer = c
        elif c and answer not in ("Yes", "No"):
            answer = c  # hold an "Other"/etc. only until a real Yes/No appears
    return answer


# ---------------------------------------------------------------- watermark
def sheet_watermark():
    """Watermark derived from the sheet itself — max(last_modified) in the Flow tab.

    No state table: the Flow tab's `last_modified` column stores each run's
    TextIt `modified_on`. Using modified_on (not entry_timestamp) is what makes
    late responses work: a subscriber who entered Monday and replies Wednesday
    has an old entry time but a fresh modified_on, so they are still picked up.

    Returns None when the tab is empty (=> full pull, which is correct on first run).
    The same value is used as the `after` filter for the partner flows AND the YES
    flow. That can over-pull them slightly (their runs may have been modified before
    this point) but never under-pulls, so nothing is missed; every write is an
    idempotent update-in-place keyed on uuid, so a redundant write just re-stamps
    the same value.
    """
    r = requests.post(f"{SHEET_SERVICE}/read", json={
        "password": SHEET_PASSWORD,
        "sheetid": SHEET_ID,
        "tab": FLOW_TAB,
        "mode": "all",
        "columns": ["last_modified"],
    }, timeout=60)
    r.raise_for_status()
    rows = r.json().get("rows") or []
    vals = [str(x.get("last_modified") or "").strip() for x in rows]
    vals = [v for v in vals if v]
    return max(vals) if vals else None


def _max_iso(a, b):
    if not a:
        return b
    if not b:
        return a
    return a if a >= b else b


# ---------------------------------------------------------------- sheet writes
def _sheet_write(body, allow_404=False):
    """POST to sheet-service /write.

    `allow_404` is for update mode (`newrow:"no"`): sheet-service returns
    HTTP 404 {"message":"No row found where '<key>' = '<value>'"} when the key
    is not present — that is a normal 'nothing to update' answer, not an error.
    Callers use it to decide to append instead (or to skip).
    """
    body = dict(body)
    body["password"] = SHEET_PASSWORD
    body["sheetid"] = SHEET_ID
    r = requests.post(f"{SHEET_SERVICE}/write", json=body, timeout=60)
    if allow_404 and r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def existing_flow_uuids():
    """One read of the Flow tab -> {uuid: True}.

    Google's Sheets API allows 60 READ requests per minute per user, and
    sheet-service reads the header row on EVERY /write call. Probing each row
    with an update-then-insert pattern therefore burned the quota almost
    immediately (HttpError 429 ReadRequestsPerMinutePerUser, limit 60).

    Reading the tab once up front and deciding insert-vs-update locally removes
    the probe entirely: one read total, then exactly one write per row.
    """
    r = requests.post(f"{SHEET_SERVICE}/read", json={
        "password": SHEET_PASSWORD,
        "sheetid": SHEET_ID,
        "tab": FLOW_TAB,
        "mode": "all",
        "columns": ["uuid"],
    }, timeout=60)
    r.raise_for_status()
    rows = r.json().get("rows") or []
    return {str(x.get("uuid") or "").strip(): True
            for x in rows if str(x.get("uuid") or "").strip()}


def write_flow_row(row, exists):
    """Write one row: update in place when `exists`, else append. Exactly one
    sheet-service call — the caller already knows which, from existing_flow_uuids().

    `nudge` is included ONLY when it is "1"/"2". sheet-service writes each field
    present in the payload, so including an empty `nudge` would blank a cell that
    may hold a manually-entered value — omitting the key leaves column H exactly
    as-is for runs with no recorded nudge (old-flow runs, or no nudge sent)."""
    data = {
        "tab": FLOW_TAB,
        "key": "uuid",
        "uuid": row["uuid"],
        "entry_timestamp": row.get("entry_timestamp", ""),
        "provider": row.get("provider", ""),
        "response": row.get("response", ""),
        "referral_timestamp": row.get("referral_timestamp", ""),
        "referral_fired": row.get("referral_fired", ""),
        "last_modified": row.get("modified_on", ""),
    }
    if row.get("nudge"):  # "1"/"2" only; blank => key omitted, cell untouched
        data["nudge"] = row["nudge"]
    if exists:
        # 404 here would mean the row vanished between the read and now; treat as
        # append rather than failing the whole run.
        res = _sheet_write({**data, "newrow": "no"}, allow_404=True)
        if res is not None and res.get("matched", 0) > 0:
            return "updated"
        _sheet_write({**data, "newrow": "yes"})
        return "inserted"
    _sheet_write({**data, "newrow": "yes"})
    return "inserted"


def heal_referral_row(uuid, referral_ts, provider):
    """Write a missing Referrals row for a referral that fired but never logged.
    Only called when the partner run shows submission=Success & sheet_log!=Success
    AND no Referrals row exists for this uuid."""
    _sheet_write({
        "tab": REFERRALS_TAB,
        "newrow": "yes",
        "key": "uuid",
        "uuid": uuid,
        "timestamp": referral_ts,
        "provider": provider,
        "source": "self-heal (referral-journey-ingest)",
    })


def existing_referral_uuids():
    """One read of the Referrals tab -> {uuid: True}.

    Same reason as existing_flow_uuids(): checking each uuid individually would
    issue one read per candidate and blow the 60/min Sheets read quota.
    """
    r = requests.post(f"{SHEET_SERVICE}/read", json={
        "password": SHEET_PASSWORD,
        "sheetid": SHEET_ID,
        "tab": REFERRALS_TAB,
        "mode": "all",
        "columns": ["uuid"],
    }, timeout=60)
    r.raise_for_status()
    rows = r.json().get("rows") or []
    return {str(x.get("uuid") or "").strip(): True
            for x in rows if str(x.get("uuid") or "").strip()}


# ---------------------------------------------------------------- extraction
def yellow_flow_row(run):
    """Journey fields from a yellow-flow run, or None if out of scope.

    Scope: a run is logged if it reached the referral offer, established by
    `provider` OR (for runs predating that result node) `providerdescription`.
    Both are saved on the AZ/Other state branches, reachable ONLY down the
    Veteran path of the usertype split — so their presence means a veteran who
    reached the offer. runs.json does not expose contact.fields, so usertype
    cannot be read directly. The only population this misses is veterans who
    exited between the usertype split and the offer, who were never offered a
    referral.
    """
    values = run.get("values") or {}
    provider = (values.get("provider") or {}).get("value", "")
    if not provider:
        # Runs that predate the `provider` result node (added 2026-07-23) have no
        # provider value, but they DO carry providerdescription, whose text is
        # partner-specific. Derive from it rather than dropping the run — this is
        # recorded data, not an assumption, and it keeps the pilot cohort visible.
        desc = (values.get("providerdescription") or {}).get("value", "") or ""
        if "Vets4Warriors" in desc:
            provider = "Vets4Warriors"
        elif "Arizona" in desc:
            provider = "ACMF"
    if not provider:
        return None  # never reached the offer (no provider AND no description)
    # runs.json normalizes result names into snake_case keys — the flow's
    # "Result 1" is exposed as `result_1`. The consolidated flow records the
    # yes/no answer under `result_1` on the in-window / first-nudge path and
    # under `result` on the post-second-nudge terminal wait; a subscriber who
    # replies only after nudge 2 has result_1 blank and the answer in `result`.
    # Coalesce both so late-after-nudge-2 responders are not logged as silent.
    # nudge count: the consolidated flow increments `nudgecount` each nudge
    # (result value is a stringified int). Per Logan, surface only an actual
    # nudge count of 1 or 2 — anything else (0, blank, old-flow runs that never
    # had the result) leaves the `nudge` cell untouched, matching the column's
    # prior manual pattern so a rebuild never stamps 0s over blank cells.
    nudge_raw = (values.get("nudgecount") or {}).get("value", "")
    nudge = str(nudge_raw).strip() if str(nudge_raw).strip() in ("1", "2") else ""
    resp = _coalesce_yesno(values, ["result_1", "result"])
    return {
        "uuid": (run.get("contact") or {}).get("uuid"),
        "entry_timestamp": run.get("created_on", ""),
        "provider": provider,
        "response": resp,  # "" (blank cell) = no response
        "nudge": nudge,    # "1"/"2" only; "" leaves the manual cell untouched
        "modified_on": run.get("modified_on", ""),
    }


def partner_referral(run, submission_key):
    """From a partner run, return (referral_ts, fired_bool, sheet_logged_bool)."""
    values = run.get("values") or {}
    sub = values.get(submission_key) or {}
    slog = values.get("sheet_log") or {}
    fired = sub.get("category") == "Success"
    logged = slog.get("category") == "Success"
    ts = _to_et(sub.get("time", "")) if fired else ""
    return ts, fired, logged


def collect_partner(flow_uuid, submission_key, after):
    """Read a partner flow's runs; return {uuid: {ts, fired, logged, created_on}}
    keeping the LATEST run per uuid (max created_on). `after` is the shared
    sheet-derived watermark (None => full pull)."""
    latest = {}
    for run in iter_runs(flow_uuid, after=after):
        uuid = (run.get("contact") or {}).get("uuid")
        if not uuid:
            continue
        ts, fired, logged = partner_referral(run, submission_key)
        prev = latest.get(uuid)
        created = run.get("created_on", "")
        if prev is None or created >= prev["created_on"]:
            latest[uuid] = {"ts": ts, "fired": fired, "logged": logged, "created_on": created,
                            "provider": ("ACMF" if submission_key == "acmf_submission" else "Vets4Warriors")}
    return latest


def collect_yes_outreach(after):
    """Read the YES follow-up flow's runs; return {uuid: "Yes"|"No"|"Other"|""} —
    the "did they reach out to you?" answer, latest run per uuid (max modified_on).

    The YES flow asks the same yes/no across nudge stages, so the answer lands
    under `result_2` (initial wait) or `result_3` (post-second-nudge terminal
    wait). `result_1` on this flow is a DIFFERENT question (a re-offer that
    routes elsewhere) and is NOT the outreach answer — do not read it here.
    The caller writes only concrete Yes/No; blank/Other is counted and skipped.
    """
    latest = {}  # uuid -> (answer, modified_on)
    for run in iter_runs(YES_FLOW, after=after):
        uuid = (run.get("contact") or {}).get("uuid")
        if not uuid:
            continue
        values = run.get("values") or {}
        answer = _coalesce_yesno(values, ["result_2", "result_3"])
        mod = run.get("modified_on", "")
        prev = latest.get(uuid)
        if prev is None or mod >= prev[1]:
            latest[uuid] = (answer, mod)
    return {u: a for u, (a, _) in latest.items()}


def write_outreach_occured(uuid, answer):
    """Update the `outreach occured?` column on the Referrals tab in place, keyed
    on uuid. Only this one column is written (partial update). Returns "updated",
    "skipped_404" (no Referrals row for this uuid — a YES-flow responder should
    already have one from the partner POST, so a 404 is a signal, not an append),
    or "noop"."""
    res = _sheet_write({
        "tab": REFERRALS_TAB,
        "newrow": "no",
        "key": "uuid",
        "uuid": uuid,
        OUTREACH_COL: answer,
    }, allow_404=True)
    if res is None:
        return "skipped_404"
    return "updated" if res.get("matched", 0) > 0 else "noop"


def run_ingest(rebuild=False):
    # Single watermark derived from the Flow tab (max last_modified). Used for all
    # flows; None on a rebuild or an empty tab => full pull.
    after = None if rebuild else sheet_watermark()

    # 1) partner runs first — build the referral lookup used to enrich journey rows
    acmf = collect_partner(ACMF_FLOW, "acmf_submission", after)
    v4w = collect_partner(V4W_FLOW, "v4w_submission", after)

    # merged partner lookup: latest across whichever partner the uuid hit
    partner = {}
    for src in (acmf, v4w):
        for uuid, rec in src.items():
            prev = partner.get(uuid)
            if prev is None or rec["created_on"] >= prev["created_on"]:
                partner[uuid] = rec

    # 2) yellow outreach flow runs -> journey rows (scoped to provider-present)
    # One read of the tab, so each row costs exactly one write (Sheets API allows
    # only 60 reads/min/user and sheet-service reads the header on every write).
    existing = existing_flow_uuids()
    stats = {"flow_rows": 0, "inserted": 0, "updated": 0, "self_heals": 0,
             "skipped_no_provider": 0, "outreach_updated": 0, "outreach_404": 0,
             "outreach_blank": 0}
    for run in iter_runs(YELLOW_FLOW, after=after):
        row = yellow_flow_row(run)
        if row is None:
            stats["skipped_no_provider"] += 1
            continue
        uuid = row["uuid"]
        # enrich with referral timestamp/fired from the latest partner run for this uuid
        prec = partner.get(uuid)
        if prec and prec["fired"]:
            row["referral_timestamp"] = prec["ts"]
            row["referral_fired"] = "yes"
        else:
            row["referral_timestamp"] = ""
            row["referral_fired"] = "no" if row["response"] == "Yes" else ""
        result = write_flow_row(row, row["uuid"] in existing)
        time.sleep(WRITE_PACE_SEC)
        stats["flow_rows"] += 1
        stats[result] += 1

    # 3) self-heal: partner referral fired but sheet_log did NOT succeed AND
    #    no Referrals row exists -> write the missing Referrals row
    referral_uuids = existing_referral_uuids()
    for uuid, rec in partner.items():
        if rec["fired"] and not rec["logged"]:
            if uuid not in referral_uuids:
                heal_referral_row(uuid, rec["ts"], rec["provider"])
                stats["self_heals"] += 1

    # 4) YES follow-up flow -> `outreach occured?` on the Referrals tab.
    #    Update-in-place on the one column, keyed on uuid. A responder should
    #    already have a Referrals row (from the partner POST); a 404 means none
    #    exists and is surfaced as a count, not silently appended.
    yes = collect_yes_outreach(after)
    for uuid, answer in yes.items():
        if answer not in ("Yes", "No"):
            stats["outreach_blank"] += 1
            continue
        outcome = write_outreach_occured(uuid, answer)
        time.sleep(WRITE_PACE_SEC)
        if outcome == "updated":
            stats["outreach_updated"] += 1
        elif outcome == "skipped_404":
            stats["outreach_404"] += 1

    return stats


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/run", methods=["POST"])
def run_endpoint():
    body = request.get_json(silent=True) or {}
    rebuild = bool(body.get("rebuild"))
    started = datetime.now(timezone.utc).isoformat()
    try:
        stats = run_ingest(rebuild=rebuild)
    except Exception as e:
        log.exception("ingest failed")
        return jsonify({"status": "error", "error": str(e), "started": started}), 500
    return jsonify({"status": "ok", "mode": "rebuild" if rebuild else "incremental",
                    "started": started, **stats})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))

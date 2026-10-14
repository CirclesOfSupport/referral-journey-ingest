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
  POST /run-testimonial, /run-survey, /run-acmf-survey -> one module only
"""
import os
import re
import time
import logging
from datetime import datetime, timedelta, timezone
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

# ---- testimonial log (SEPARATE concern, SEPARATE workbook) ----
# The video-testimonial yes/no log is a distinct program (Senja video testimonials,
# Dina's 2026-08-11 ask) that happens to reuse the same runs-API plumbing. It lives
# in a DIFFERENT workbook from the referral log and is written as a self-contained
# module so it lifts out into its own `testimonial-log` service later without a
# rewrite (the testimonial log is a distinct program tracked to graduate to its
# own service). All three vars are optional so the referral service
# runs unchanged if the testimonial log is not configured; when TESTIMONIAL_FLOW
# is unset, the testimonial pass is skipped entirely.
TESTIMONIAL_FLOW = os.environ.get("TESTIMONIAL_FLOW")        # Video Testimonial Request flow UUID
TESTIMONIAL_SHEET_ID = os.environ.get("TESTIMONIAL_SHEET_ID")  # its own workbook (NOT SHEET_ID)
TESTIMONIAL_TAB = os.environ.get("TESTIMONIAL_TAB", "Sheet1")  # tab name; set to the actual tab

# ---- follow-up survey logging (columns F and G of the SAME testimonial tab) ----
# Two survey flows sent 2026-08-25 to the testimonial cohort, split by their
# original Yes/No answer. They write F/G on the tab the testimonial module already
# owns A-D of, so they reuse TESTIMONIAL_SHEET_ID / TESTIMONIAL_TAB. Optional:
# unset = the survey pass is skipped and nothing else changes.
#
# Result names are NOT shared between the two flows (verified against the live
# definitions 2026-09-14) — reading a single `result` key would silently miss the
# NO/NR flow entirely, so each flow carries its own option/free-text key pair.
SURVEY_YES_FLOW = os.environ.get("SURVEY_YES_FLOW")    # "...Request - YES"
SURVEY_NONR_FLOW = os.environ.get("SURVEY_NONR_FLOW")  # "...Request - NO and NR"

# (flow_uuid_env_value, option_result_key, freetext_result_key, other_node_uuid)
# option key   -> the numbered-choice router; its `category` is the label for F.
# freetext key -> the "Something Else" elaboration catcher (All Responses).
# other_node   -> the "humble chatbot" reprompt node on the `Other` branch. Its
#                 presence in a run's `path` is the ONLY reliable evidence that an
#                 unparseable reply happened, because the loop-back overwrites the
#                 result and erases it from `values`.
SURVEY_FLOWS = [
    (SURVEY_YES_FLOW, "result", "result_3",
     "59fe4410-41f1-4a73-95e5-6afa3f08ccce"),
    (SURVEY_NONR_FLOW, "result_4", "result_5",
     "2414fb1c-9823-4bb3-86fb-6d79f45578df"),
]

SURVEY_RESPONSE_COL = "Survey Response (Survey Field)"
SURVEY_FREETEXT_COL = "Open-Ended Response (Concat: Other + Free Text)"
SURVEY_CONCAT_SEP = " | "

# runtime seatbelt
PAGE_CEILING = int(os.environ.get("PAGE_CEILING", "500"))
# Google Sheets API allows 60 read requests/min/user, and sheet-service reads the
# header row on every /write. Pace writes so a large rebuild cannot exceed it.
# 1.1s => ~54 writes/min, just under the limit. Incremental runs are far smaller
# and unaffected in practice.
WRITE_PACE_SEC = float(os.environ.get("WRITE_PACE_SEC", "1.1"))

# TextIt transient-error retry (5xx + connection/read timeouts). Bounded, unlike
# the 429 rate-limit loop: a persistent 5xx is a real outage to surface, not spin
# on. Backoff is exponential: base, 2*base, 4*base, ... 5 retries at base=5s
# spans ~5+10+20+40+80 = 155s of retrying before giving up.
TEXTIT_MAX_RETRIES = int(os.environ.get("TEXTIT_MAX_RETRIES", "5"))
TEXTIT_BACKOFF_BASE = float(os.environ.get("TEXTIT_BACKOFF_BASE", "5"))


# ---------------------------------------------------------------- TextIt reads
def _textit_get(url, params=None):
    """GET with TextIt's transient failures handled:
      - 429 (rate limit): parse the 'available in N seconds' body and sleep N+3,
        retry indefinitely. A throttle is not an error — never dies on it.
      - 5xx (transient TextIt-side error) and connection/read timeouts: retry a
        BOUNDED number of times with exponential backoff, then give up and raise.
        A single upstream 500 on one runs.json page previously killed the whole
        nightly ingest (2026-08-25: the V4W flow's runs endpoint 500'd, the
        pipeline HALTed the chain). 5xx retries are bounded (unlike 429) because
        a persistent 5xx is a real outage we should surface, not spin on.

    Rate-limit sleeps do NOT count against the 5xx retry budget."""
    headers = {"Authorization": f"Token {TEXTIT_TOKEN}"}
    attempt = 0          # total loop iterations (for logging)
    err_retries = 0      # bounded 5xx / connection-error retries consumed
    while True:
        attempt += 1
        try:
            r = requests.get(url, headers=headers, params=params, timeout=60)
        except (requests.ConnectionError, requests.Timeout) as e:
            if err_retries >= TEXTIT_MAX_RETRIES:
                log.error("textit connection error, retries exhausted (%s): %s",
                          err_retries, e)
                raise
            err_retries += 1
            wait = TEXTIT_BACKOFF_BASE * (2 ** (err_retries - 1))
            log.warning("textit connection error; sleeping %ss (retry %s/%s): %s",
                        wait, err_retries, TEXTIT_MAX_RETRIES, e)
            time.sleep(wait)
            continue

        if r.status_code == 429:
            wait = 60
            m = re.search(r"available in (\d+)", r.text)
            if m:
                wait = int(m.group(1)) + 3
            log.warning("textit 429; sleeping %ss (attempt %s)", wait, attempt)
            time.sleep(wait)
            continue

        if r.status_code >= 500:
            if err_retries >= TEXTIT_MAX_RETRIES:
                log.error("textit %s, retries exhausted (%s) for %s",
                          r.status_code, err_retries, r.url)
                r.raise_for_status()
            err_retries += 1
            wait = TEXTIT_BACKOFF_BASE * (2 ** (err_retries - 1))
            log.warning("textit %s; sleeping %ss (retry %s/%s) for %s",
                        r.status_code, wait, err_retries, TEXTIT_MAX_RETRIES, r.url)
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


# ================================================================ TESTIMONIAL LOG
# Self-contained module — its own workbook (TESTIMONIAL_SHEET_ID), its own read,
# its own row builder, its own write. Deliberately does NOT reuse the referral
# sheet helpers above, because those are hardwired to SHEET_ID (the referral
# workbook). Keeping this block credential-and-workbook-isolated is what lets it
# be moved to a standalone `testimonial-log` service by copying this section plus
# the TESTIMONIAL_* env vars — nothing else in the file depends on it.
#
# Flow shape (verified against the flow JSON 2026-08-24): a single Wait for
# Response node, result name "Result" (runs.json key `result`). The "Other" branch
# reprompts and loops BACK INTO THE SAME wait node, so a subscriber who first says
# something unparseable and then replies Yes/No overwrites `result` in place.
# runs.json reports only the run's FINAL result state, so the last answer is the
# one read — the "Other gets overtaken by a later Yes/No" requirement needs no
# special handling; there is exactly one result key, not the yellow flow's
# result_1/result split.
#
# `TESTIMONIAL_TESTIMONIAL_*` columns written (4-col tab):
#   uuid                -> run contact.uuid           (every run == a message was sent)
#   timestamp           -> _to_et(run created_on)     (ET display 'YYYY-MM-DD HH:MM'; when the ask went out)
#   Response            -> values.result.category     (Yes/No/Other; blank = never answered)
#   Response timestamp  -> _to_et(values.result.time) (ET display; when that result was set == the reply moment; blank if unanswered)

def _testimonial_write(body, allow_404=False):
    """POST to sheet-service /write against the TESTIMONIAL workbook. Mirror of
    _sheet_write but bound to TESTIMONIAL_SHEET_ID — kept separate so the
    testimonial module has no dependency on the referral SHEET_ID."""
    body = dict(body)
    body["password"] = SHEET_PASSWORD
    body["sheetid"] = TESTIMONIAL_SHEET_ID
    r = requests.post(f"{SHEET_SERVICE}/write", json=body, timeout=60)
    if allow_404 and r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def existing_testimonial_uuids():
    """One read of the testimonial tab -> {uuid: True}. Same 60-read/min-quota
    reasoning as existing_flow_uuids(): read once, decide insert-vs-update locally,
    one write per row."""
    r = requests.post(f"{SHEET_SERVICE}/read", json={
        "password": SHEET_PASSWORD,
        "sheetid": TESTIMONIAL_SHEET_ID,
        "tab": TESTIMONIAL_TAB,
        "mode": "all",
        "columns": ["uuid"],
    }, timeout=60)
    r.raise_for_status()
    rows = r.json().get("rows") or []
    return {str(x.get("uuid") or "").strip(): True
            for x in rows if str(x.get("uuid") or "").strip()}


def testimonial_row(run):
    """Build the 4-column testimonial row from a run. Every run of the flow is a
    sent message, so every run produces a row (no scope filter like the yellow
    flow's provider-presence). `result` is the single wait-node result; its
    `category` is the final Yes/No/Other and its `time` is when that answer was
    set. Blank category/time = the subscriber never replied."""
    values = run.get("values") or {}
    result = values.get("result") or {}
    category = result.get("category", "") or ""
    # "No Response" is TextIt's wait-timeout category on some flows; normalize any
    # such non-answer to blank so the Response column reads Yes/No/Other/blank.
    if category == "No Response":
        category = ""
    # This flow's category names are uppercase (YES/NO/Other); title-case so the
    # column matches the referral log's Yes/No convention. "" stays "".
    if category:
        category = category.title()
    resp_time = result.get("time", "") or "" if category else ""
    return {
        "uuid": (run.get("contact") or {}).get("uuid"),
        "timestamp": _to_et(run.get("created_on", "")),
        "Response": category,
        "Response timestamp": _to_et(resp_time),
    }


def write_testimonial_row(row, exists):
    """Write one testimonial row: update in place when `exists`, else append.
    Exactly one sheet-service call (the caller knows which from
    existing_testimonial_uuids()). All four columns are always written — unlike
    the Flow tab's conditional `nudge`, none of these columns is ever
    hand-maintained, so a blank is authoritative (means 'no response yet')."""
    data = {
        "tab": TESTIMONIAL_TAB,
        "key": "uuid",
        "uuid": row["uuid"],
        "timestamp": row.get("timestamp", ""),
        "Response": row.get("Response", ""),
        "Response timestamp": row.get("Response timestamp", ""),
    }
    if exists:
        res = _testimonial_write({**data, "newrow": "no"}, allow_404=True)
        if res is not None and res.get("matched", 0) > 0:
            return "updated"
        _testimonial_write({**data, "newrow": "yes"})
        return "inserted"
    _testimonial_write({**data, "newrow": "yes"})
    return "inserted"


def run_testimonial():
    """Read the testimonial flow's runs and upsert the 4-column log.

    No watermark: the testimonial tab has no last_modified column and the pilot
    cohort is tiny (n≈35), so every run is a full pull keyed on uuid. Because a
    subscriber's answer can arrive late (Other→loop, or a delayed reply), a full
    pull each time is what guarantees the final answer lands — the same
    late-response property the referral service gets from its watermark, achieved
    here by simply re-reading all runs. Idempotent uuid-keyed upsert, so
    re-stamping an unchanged row is inert.

    Returns {} (a no-op) when TESTIMONIAL_FLOW is unconfigured, so the referral
    service is unaffected if the testimonial log is not deployed here.
    """
    if not (TESTIMONIAL_FLOW and TESTIMONIAL_SHEET_ID):
        return {}
    existing = existing_testimonial_uuids()
    stats = {"testimonial_rows": 0, "testimonial_inserted": 0, "testimonial_updated": 0}
    for run in iter_runs(TESTIMONIAL_FLOW):  # no `after` — full pull, tiny cohort
        row = testimonial_row(run)
        if not row["uuid"]:
            continue
        result = write_testimonial_row(row, row["uuid"] in existing)
        time.sleep(WRITE_PACE_SEC)
        stats["testimonial_rows"] += 1
        stats["testimonial_" + result] += 1
    return stats


# ================================================================================


# ============================================================== SURVEY LOG (F/G)
# Fills columns F and G of the SAME tab the testimonial module owns A-D of. It is
# a sibling module, not an extension: own reads, own row builder, own write, so it
# lifts out alongside run_testimonial() when the testimonial log graduates to its
# own service.
#
# COLUMN TARGETS ARE RESOLVED BY HEADER NAME, NEVER BY LETTER. The 2026-08-25 call
# specified "E = survey response, F = open-ended", but a hand-maintained
# `Test #2 Sent? ` column was inserted at E afterwards, shifting both targets one
# right to F and G (verified live 2026-09-14). Writing by letter would have
# overwritten the DM's send-tracking column. sheet-service matches headers exactly
# — note the live E header carries a TRAILING SPACE.
#
# WHAT THIS MODULE MUST NOT TOUCH: columns A-D (owned by run_testimonial) and
# column E (hand-maintained). Every write here names ONLY uuid + F + G, and is
# UPDATE-ONLY (`newrow: "no"`) — a survey run whose uuid is not already on the tab
# is counted and reported, never inserted as a partial row.
#
# F  <- the option router's final `category` (the label TEXT, e.g. "Busy", not "2";
#       the numbers map to DIFFERENT content between the two flows, which is why
#       the label rather than the number is stored).
# G  <- concatenation of the contact's incoming messages in that flow, chronological.
#
# WHY G COMES FROM messages.json AND NOT runs.json:
# The `Other` (unparseable) branch reprompts and loops BACK into the same option
# router, so TextIt OVERWRITES the result in place — which is exactly the override
# the DM asked for ("if they get routed back and give a valid response, E gets
# overwritten... keep their original full response in F"), and it needs no logic
# here because runs.json reports only final state. But that same overwrite DESTROYS
# the original unparseable text inside `values`, and runs.json carries no message
# text at all (`path` holds node + time only). The original text is precisely what
# the DM asked to keep. So G is built from the messages endpoint instead.
#
# NOTE the override applies to the `Other` branch ONLY. `Something Else` (the last
# numbered option) routes to the free-text catcher and TERMINATES — there is no path
# back to the option router, so a "Something Else then a valid number" override is
# not possible in the flow as built and is not attempted here.


def iter_contact_messages(contact_uuid):
    """Yield a contact's messages, paginated. Reuses _textit_get so this inherits
    the 429 and bounded-5xx handling. messages.json is filterable by `contact`
    (NOT by flow — there is no flow query param), so flow scoping is done
    client-side by the caller on `.flow.uuid`."""
    params = {"contact": contact_uuid}
    url = f"{TEXTIT_BASE}/messages.json"
    pages = 0
    while url:
        pages += 1
        if pages > PAGE_CEILING:
            log.warning("page ceiling %s hit for messages of %s",
                        PAGE_CEILING, contact_uuid)
            break
        data = _textit_get(url, params=params)
        for msg in data.get("results", []):
            yield msg
        url = data.get("next")
        params = None  # next already encodes params


def survey_other_texts(contact_uuid, flow_uuid, before_iso, selection_text=""):
    """The contact's UNPARSEABLE (`Other`) replies on one flow, oldest-first.

    Two filters, because either alone leaks:

    1. TIME. The `Other` branch reprompts and loops back into the same option
       router, so every failed attempt arrives before the moment the final option
       result was set.

    2. THE SELECTION TEXT ITSELF. A message's `created_on` is stamped when TextIt
       RECEIVES it, while the result's `time` is stamped when the flow PROCESSES
       it — a fraction of a second later. So the valid selection is genuinely
       "before" the result time and slips past filter 1. Dropping messages whose
       text equals the raw value that set the result removes it deterministically.
       Observed live: a contact sent "4,3" (unparseable -> Other), then "4"; the
       time filter alone left G as "4,3 | 4" instead of "4,3".

    `before_iso` empty means the final category was itself `Other` — nothing the
    contact sent ever parsed, so every incoming message qualifies."""
    sel = (selection_text or "").strip()
    texts = []
    for msg in iter_contact_messages(contact_uuid):
        if (msg.get("direction") or "").lower() not in ("incoming", "in"):
            continue
        if ((msg.get("flow") or {}).get("uuid")) != flow_uuid:
            continue
        created = msg.get("created_on") or ""
        if before_iso and created >= before_iso:
            continue
        t = (msg.get("text") or "").strip()
        if not t:
            continue
        if sel and t == sel:
            continue  # the valid selection — already represented in column F
        texts.append((created, t))
    texts.sort(key=lambda x: x[0])  # oldest-first; API returns newest-first
    out = []
    for _, t in texts:
        if not out or out[-1] != t:
            out.append(t)
    return out


def survey_row(run, option_key, freetext_key, flow_uuid, other_node):
    """Build {uuid, F, G} for one survey run.

    F: the option router's final category. A wait-timeout ("No Response") is
    normalized to blank by _cat, so F reads as a real label or blank.

    G: "other plus free text" — the two UNSTRUCTURED sources the DM asked for, and
    nothing else:
      1. the contact's unparseable `Other` replies, and
      2. the `Something Else` elaboration captured by the free-text catcher.
    A contact who picked a valid option first try has NO unstructured content, so
    G is correctly blank — their answer is fully represented by F's label. Menu
    digits are never carried into G; F already encodes that choice, and the two
    flows map the same digit to different labels anyway.

    An `Other` is detected from the run's `path`: the loop-back overwrites the
    result, so `values` cannot show that it happened, but the reprompt node's
    presence in the path is definitive. Messages are fetched only in that case."""
    values = run.get("values") or {}
    contact_uuid = (run.get("contact") or {}).get("uuid")
    category = _cat(values, option_key)

    parts = []
    hit_other = any((step or {}).get("node") == other_node
                    for step in (run.get("path") or []))
    if contact_uuid and hit_other and run.get("responded") is not False:
        # When the FINAL category is itself `Other`, the contact never landed a
        # valid selection, so nothing they sent parsed and every incoming message
        # is unstructured — including the last one, whose timestamp IS the result
        # time. Cutting strictly before that time would drop it. Otherwise the
        # final result was set by a valid selection, which is excluded.
        result_time = ("" if category == "Other"
                       else (values.get(option_key) or {}).get("time", "") or "")
        selection = ("" if category == "Other"
                     else (values.get(option_key) or {}).get("value", "") or "")
        parts.extend(survey_other_texts(contact_uuid, flow_uuid,
                                        result_time, selection))

    elaboration = ((values.get(freetext_key) or {}).get("value", "") or "").strip()
    if elaboration and (not parts or parts[-1] != elaboration):
        parts.append(elaboration)

    return {
        "uuid": contact_uuid,
        SURVEY_RESPONSE_COL: category,
        SURVEY_FREETEXT_COL: SURVEY_CONCAT_SEP.join(parts),
    }


def write_survey_row(row):
    """UPDATE-ONLY write of F and G, keyed on uuid.

    `newrow: "no"` so a uuid absent from the tab is NEVER appended — this module
    does not own row creation and must not emit a row with blank A-D. Only uuid,
    F and G are sent, so A-D (testimonial module) and E (hand-maintained) are
    untouched by construction. Returns 'updated', or 'unmatched' when the uuid is
    not on the tab (a 404 from sheet-service, or a zero-match response)."""
    data = {
        "tab": TESTIMONIAL_TAB,
        "key": "uuid",
        "uuid": row["uuid"],
        SURVEY_RESPONSE_COL: row.get(SURVEY_RESPONSE_COL, ""),
        SURVEY_FREETEXT_COL: row.get(SURVEY_FREETEXT_COL, ""),
        "newrow": "no",
    }
    res = _testimonial_write(data, allow_404=True)
    if res is not None and res.get("matched", 0) > 0:
        return "updated"
    return "unmatched"


def run_survey():
    """Read both survey flows' runs and fill F/G on the testimonial tab.

    No watermark: tiny cohort (~34), full pull each time, idempotent uuid-keyed
    update. A late reply therefore lands on the next run, same property the
    testimonial pass relies on.

    Returns {} when neither survey flow is configured, so the rest of the service
    is unaffected if this is not deployed.
    """
    configured = [(f, o, x, n) for (f, o, x, n) in SURVEY_FLOWS if f]
    if not configured or not TESTIMONIAL_SHEET_ID:
        return {}
    stats = {"survey_rows": 0, "survey_updated": 0, "survey_unmatched": 0}
    for flow_uuid, option_key, freetext_key, other_node in configured:
        for run in iter_runs(flow_uuid):  # no `after` — full pull, tiny cohort
            row = survey_row(run, option_key, freetext_key, flow_uuid, other_node)
            if not row["uuid"]:
                continue
            outcome = write_survey_row(row)
            time.sleep(WRITE_PACE_SEC)
            stats["survey_rows"] += 1
            stats["survey_" + outcome] += 1
    if stats["survey_unmatched"]:
        log.warning("survey: %s run(s) had no matching uuid on the tab",
                    stats["survey_unmatched"])
    return stats


# ================================================================================


# ======================================================= ACMF SURVEY ANSWER LOG
# Self-contained module: one row per contact who received either ACMF follow-up
# survey flow ("Declined" and "No Response"), written to its own tab of the referral-log
# workbook (SHEET_ID). Own reads, own row builder, own writes; it touches no
# other tab. Optional: when its three env vars are unset the pass is skipped.
#
# Every answer and every path is captured ("too much info rather than not enough");
# the tab shows 14 readable columns and the rest of the run sits in `details`:
# timing, the number
# question (with any rejected replies), the random message variant, the
# free-text answer and what the inbound catch-all did with it (classifier
# bucket, humble line, crisis numbers, triage review, counselor determination),
# the yes/no question (with the re-referral it starts), the 6-hour reminder,
# the closing message, replies after the run ended, and the full step trail.
#
# HOW NODES ARE IDENTIFIED. No node uuid is hard-coded. Both flow definitions
# are read live (definitions.json, dependencies=none) on every call and each
# node's role is derived from its shape:
#   - waits:          routers with a message wait, named by their result key
#   - humble re-prompt: the message on a wait's "Other" branch that loops back
#                     into the same wait
#   - window check:   a router whose operand calls format_datetime; its exit to
#                     a message node is the reminder, its exit to a wait is a
#                     hold (out of the daytime window, back to waiting)
#   - re-referral:    the Enter-a-Flow node on the yes/no wait's "yes" branch
# so editing message text or moving nodes in TextIt does not break the log.
#
# Result keys (runs.json snake_cases the flow's result names) are per flow and
# were verified against the live definitions; the number question is
# `result_2` in both flows.
#
# WHY messages.json AS WELL AS runs.json. A run's `values` keep only the LAST
# answer per wait (a humble re-prompt overwrites the rejected reply), and runs
# carry no message text. Rejected replies, the first-reply time, the messages
# the flow sent, what the catch-all sent and replies after the run ended come
# from the contact's messages, scoped client-side (messages.json has no `flow`
# filter). The catch-all, crisis-numbers, triage and request-call activity come
# from the contact's other runs in the same window.
#
# WRITES. The tab is read once; rows are upserted keyed on `uuid` (the contact) — a
# new contact is appended, a changed row is updated in place, an unchanged row is
# not written. A contact's latest survey run is the row; earlier runs are named in
# `details`. One write per row, paced WRITE_PACE_SEC. Before any write the header
# row is checked: a missing column stops the pass and names every missing one.

ACMF_DECLINED_FLOW = os.environ.get("ACMF_DECLINED_FLOW")
ACMF_NR_FLOW = os.environ.get("ACMF_NR_FLOW")
ACMF_SURVEY_TAB = os.environ.get("ACMF_SURVEY_TAB")

ACMF_RUN_FIELDS = [
    "run uuid", "uuid", "name", "survey", "started", "first reply at", "ended",
    "minutes to first reply", "how it ended", "interrupted by", "messages sent",
    "q1 reply", "q1 choice", "q1 choice meaning", "q1 answered at",
    "q1 rejected replies", "message variant", "free-text question",
    "free-text answer", "free-text answered at", "catch-all category",
    "humble line sent", "crisis numbers sent", "triage started",
    "counselor determination", "determination at", "yes/no question",
    "yes/no reply", "yes/no answer", "yes/no rejected replies",
    "yes/no answered at", "re-referral step", "ACMF form result",
    "Referrals sheet result", "reminder sent", "reminder after",
    "reminder sent at", "window holds", "closing message", "replies after end",
    "step trail",
]
# The tab's 14 columns, A to N (Logan, 2026-10-14: the 41-field row was too much for
# the program lead to read). Every other field of ACMF_RUN_FIELDS goes into
# `details`, one "label: value" per line.
ACMF_SURVEY_COLUMNS = [
    "name", "uuid", "survey", "sent", "status", "option chosen", "message shown",
    "their words", "triage", "wants follow-up", "re-referred", "reminder",
    "replied after survey", "details",
]
ACMF_DETAIL_FIELDS = [
    "run uuid", "started", "first reply at", "ended", "minutes to first reply",
    "how it ended", "interrupted by", "messages sent", "q1 reply", "q1 answered at",
    "q1 rejected replies", "free-text question", "free-text answered at",
    "catch-all category", "humble line sent", "crisis numbers sent", "triage started",
    "yes/no question", "yes/no reply", "yes/no rejected replies", "yes/no answered at",
    "re-referral step", "ACMF form result", "Referrals sheet result",
    "reminder sent at", "window holds", "closing message", "step trail",
]
ACMF_DETAILS_MAX = 45000
# One row per contact: the tab has no run column, so rows are keyed on `uuid` and a
# contact's latest survey run is the row; any earlier run is named in `details`.
ACMF_KEY_COL = "uuid"
ACMF_REPLIES_AFTER_DAYS = 3
ACMF_JOIN = " | "


def _acmf_flow_configs():
    """(flow uuid, survey label, q1 key, yes/no key, free-text keys, variant key)
    for each configured flow. Free-text keys are in the order of the q1 options
    that lead to them."""
    out = []
    if ACMF_DECLINED_FLOW:
        out.append((ACMF_DECLINED_FLOW, "Declined", "result_2", "result_6",
                    ["result_3", "result_4", "result_5"], "message_variant"))
    if ACMF_NR_FLOW:
        out.append((ACMF_NR_FLOW, "No Response", "result_2", "result_5",
                    ["result_3", "result_4"], None))
    return out


def _acmf_configured():
    return bool(ACMF_SURVEY_TAB and (ACMF_DECLINED_FLOW or ACMF_NR_FLOW))


# ---------------------------------------------------------------- time helpers
def _parse_iso(s):
    """TextIt ISO timestamp -> aware datetime, or None. Comparisons go through
    datetimes, never strings: TextIt mixes precisions ('...58Z', '...58.123Z')."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def _et_short(iso_utc):
    """'MM-DD HH:MM' Eastern, for the step trail."""
    dt = _parse_iso(iso_utc)
    return dt.astimezone(_ET).strftime("%m-%d %H:%M") if dt else ""


def _result_key(name):
    """A flow's result name as runs.json keys it ('Result 2' -> 'result_2')."""
    return re.sub(r"[^a-z0-9]+", "_", (name or "").strip().lower()).strip("_")


def _first_words(text, n=8):
    words = (text or "").split()
    return " ".join(words[:n]) + ("…" if len(words) > n else "")


# ---------------------------------------------------------------- reads
def fetch_flow_definition(flow_uuid):
    """The live definition of exactly this flow (dependencies=none, matched on
    uuid — never flows[0], which can be a dependency)."""
    data = _textit_get(f"{TEXTIT_BASE}/definitions.json",
                       params={"flow": flow_uuid, "dependencies": "none"})
    for fl in data.get("flows", []):
        if fl.get("uuid") == flow_uuid:
            return fl
    raise RuntimeError(f"definition for flow {flow_uuid} not returned")


def _iter_pages(path, params):
    url = f"{TEXTIT_BASE}/{path}"
    pages = 0
    while url:
        pages += 1
        if pages > PAGE_CEILING:
            log.warning("page ceiling %s hit for %s %s", PAGE_CEILING, path, params)
            break
        data = _textit_get(url, params=params)
        for item in data.get("results", []):
            yield item
        url = data.get("next")
        params = None


def contact_activity(contact_uuid, since_iso):
    """A contact's messages and runs from `since_iso` on (both endpoints filter
    `after`), oldest-first."""
    msgs = list(_iter_pages("messages.json",
                            {"contact": contact_uuid, "after": since_iso}))
    runs = list(_iter_pages("runs.json",
                            {"contact": contact_uuid, "after": since_iso}))
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    msgs.sort(key=lambda m: _parse_iso(m.get("created_on")) or epoch)
    runs.sort(key=lambda r: _parse_iso(r.get("created_on")) or epoch)
    return msgs, runs


# ---------------------------------------------------------------- flow shape
def flow_shape(definition):
    """Derive every node role the row builder needs from a live definition."""
    nodes = {n["uuid"]: n for n in definition.get("nodes", [])}
    shape = {
        "nodes": nodes,
        "msg_text": {},       # node -> message text
        "waits": {},          # node -> result key
        "wait_by_key": {},    # result key -> node
        "humble": {},         # humble node -> the wait it loops back to
        "window": set(),      # window-check router nodes
        "reminder": set(),    # message nodes reached from a window check
        "enter": {},          # node -> (flow uuid, flow name)
        "rereferral": None,   # enter node on the yes/no "yes" branch
        "kind": {},           # node -> short label for the step trail
    }
    for uid, n in nodes.items():
        for a in n.get("actions", []):
            t = a.get("type")
            if t == "send_msg":
                shape["msg_text"][uid] = a.get("text", "")
            elif t == "enter_flow":
                fl = a.get("flow") or {}
                shape["enter"][uid] = (fl.get("uuid"), fl.get("name", ""))
        r = n.get("router") or {}
        if r.get("wait") and r.get("result_name"):
            key = _result_key(r["result_name"])
            shape["waits"][uid] = key
            shape["wait_by_key"][key] = uid
        if "format_datetime" in (r.get("operand") or "") and not r.get("wait"):
            shape["window"].add(uid)

    def dest(node, exit_uuid):
        for e in node.get("exits", []):
            if e.get("uuid") == exit_uuid:
                return e.get("destination_uuid")
        return None

    for uid in shape["waits"]:
        r = nodes[uid]["router"]
        for c in r.get("categories", []):
            d = dest(nodes[uid], c.get("exit_uuid"))
            if c.get("name") == "Other" and d in shape["msg_text"]:
                back = [e.get("destination_uuid") for e in nodes[d].get("exits", [])]
                if uid in back:
                    shape["humble"][d] = uid
    for uid in shape["window"]:
        for e in nodes[uid].get("exits", []):
            d = e.get("destination_uuid")
            if d in shape["msg_text"]:
                shape["reminder"].add(d)
    return shape


def _yesno_branch_enter(shape, yesno_key):
    """The Enter-a-Flow node on the yes/no wait's 'yes' branch (the re-referral)."""
    wnode = shape["wait_by_key"].get(yesno_key)
    if not wnode:
        return None
    n = shape["nodes"][wnode]
    for c in n["router"].get("categories", []):
        if (c.get("name") or "").strip().lower() == "yes":
            for e in n.get("exits", []):
                if e.get("uuid") == c.get("exit_uuid"):
                    d = e.get("destination_uuid")
                    return d if d in shape["enter"] else None
    return None


def _node_label(shape, uid, cfg):
    _, _, q1_key, yesno_key, ft_keys, _ = cfg
    n = shape["nodes"].get(uid) or {}
    if uid in shape["waits"]:
        k = shape["waits"][uid]
        if k == q1_key:
            return "q1 wait"
        if k == yesno_key:
            return "yes/no wait"
        if k in ft_keys:
            return f"free-text wait ({k})"
        return f"wait ({k})"
    if uid in shape["humble"]:
        return "humble re-prompt"
    if uid in shape["reminder"]:
        return "reminder"
    if uid in shape["window"]:
        return "window check"
    if uid in shape["enter"]:
        return "enter " + shape["enter"][uid][1]
    if uid in shape["msg_text"]:
        return 'msg "' + _first_words(shape["msg_text"][uid], 6) + '"'
    r = n.get("router") or {}
    if r.get("type") == "random":
        return "random split"
    for a in n.get("actions", []):
        if a.get("type") == "set_contact_field":
            key = (a.get("field") or {}).get("key", "")
            val = a.get("value")
            return f"{key}={val}" if val else f"{key} cleared"
    return uid[:8]


def choice_meanings(text):
    """{'1': 'I found other support', ...} from the number question's message."""
    out = {}
    for line in (text or "").splitlines():
        m = re.match(r"\s*(\d+)\)\s*(.+?)\s*$", line)
        if m:
            out[m.group(1)] = m.group(2)
    return out


# ---------------------------------------------------------------- row builder
def _value(values, key, field="value"):
    return ((values.get(key) or {}).get(field) or "")


def _answered(values, key):
    """A wait's result only counts as an answer when its category is a real one
    (not the timeout's 'No Response', not a still-pending 'Other')."""
    cat = _value(values, key, "category")
    return bool(cat) and cat not in ("No Response", "Other")


def _rejected(flow_msgs, after_dt, before_dt, selection):
    """Incoming replies to this flow in (after_dt, before_dt) that are not the
    accepted selection — the replies a humble re-prompt turned away. The
    selection text is dropped explicitly because a message is stamped on
    receipt, a fraction before the result time that bounds the window."""
    sel = (selection or "").strip()
    out = []
    for m in flow_msgs:
        if m["_dir"] != "in":
            continue
        dt = m["_dt"]
        if after_dt and dt and dt <= after_dt:
            continue
        if before_dt and dt and dt >= before_dt:
            continue
        t = (m.get("text") or "").strip()
        if not t or (sel and t == sel):
            continue
        if not out or out[-1] != t:
            out.append(t)
    return ACMF_JOIN.join(out)


def _dir(m):
    d = (m.get("direction") or "").lower()
    return "in" if d in ("in", "incoming") else "out"


def acmf_survey_row(run, cfg, shape, msgs, other_runs, now=None):
    """Build the full row for one survey run. `msgs` / `other_runs` are the
    contact's messages and runs since shortly before the run started."""
    flow_uuid, label, q1_key, yesno_key, ft_keys, variant_key = cfg
    now = now or datetime.now(timezone.utc)
    values = run.get("values") or {}
    contact = run.get("contact") or {}
    path = run.get("path") or []
    path_nodes = [p.get("node") for p in path]
    created_dt = _parse_iso(run.get("created_on"))
    exited_dt = _parse_iso(run.get("exited_on"))
    end_dt = exited_dt or now

    for m in msgs:
        m["_dt"] = _parse_iso(m.get("created_on"))
        m["_dir"] = _dir(m)
    flow_msgs = [m for m in msgs
                 if ((m.get("flow") or {}).get("uuid")) == flow_uuid
                 and m["_dt"] and created_dt and m["_dt"] >= created_dt
                 and m["_dt"] <= end_dt]
    first_in = next((m for m in flow_msgs if m["_dir"] == "in"), None)

    row = {c: "" for c in ACMF_RUN_FIELDS}
    row["_q1 timed out"] = False
    row["_reminder after kind"] = ""
    row["_started utc"] = run.get("created_on") or ""
    row["run uuid"] = run.get("uuid", "")
    row["uuid"] = contact.get("uuid", "")
    row["name"] = contact.get("name") or ""
    row["survey"] = label
    row["started"] = _to_et(run.get("created_on"))
    row["ended"] = _to_et(run.get("exited_on")) if run.get("exited_on") else ""
    row["how it ended"] = run.get("exit_type") or "waiting"
    row["messages sent"] = str(sum(1 for m in flow_msgs if m["_dir"] == "out"))
    if first_in:
        row["first reply at"] = _to_et(first_in.get("created_on"))
        if created_dt:
            row["minutes to first reply"] = (
                f"{(first_in['_dt'] - created_dt).total_seconds() / 60:.1f}")

    if row["how it ended"] == "interrupted" and exited_dt:
        nxt = [r for r in other_runs
               if ((r.get("flow") or {}).get("uuid")) != flow_uuid
               and _parse_iso(r.get("created_on"))
               and _parse_iso(r.get("created_on")) >= exited_dt - timedelta(seconds=5)]
        if nxt:
            row["interrupted by"] = (nxt[0].get("flow") or {}).get("name", "")

    # --- q1, the number question
    meanings = choice_meanings(shape["msg_text"].get(path_nodes[0], "")) if path_nodes else {}
    q1_cat = _value(values, q1_key, "category")
    q1_dt = _parse_iso(_value(values, q1_key, "time"))
    if _answered(values, q1_key):
        row["q1 reply"] = _value(values, q1_key)
        row["q1 choice"] = q1_cat
        row["q1 choice meaning"] = meanings.get(q1_cat, "")
        row["q1 answered at"] = _to_et(_value(values, q1_key, "time"))
    elif q1_cat == "Other":
        row["q1 reply"] = _value(values, q1_key)
    q1_wait = shape["wait_by_key"].get(q1_key)
    if not _answered(values, q1_key):
        row["_q1 timed out"] = q1_cat == "No Response" or any(
            n == q1_wait and i + 1 < len(path_nodes) and path_nodes[i + 1] in shape["window"]
            for i, n in enumerate(path_nodes))
    q1_humble = [h for h, w in shape["humble"].items() if w == q1_wait]
    if any(h in path_nodes for h in q1_humble):
        before = q1_dt if _answered(values, q1_key) else None
        row["q1 rejected replies"] = _rejected(
            flow_msgs, None, before,
            _value(values, q1_key) if _answered(values, q1_key) else "")

    if variant_key:
        row["message variant"] = _value(values, variant_key, "category")

    # --- free text (at most one per run: one per q1 option)
    for k in ft_keys:
        wnode = shape["wait_by_key"].get(k)
        if wnode not in path_nodes:
            continue
        idx = path_nodes.index(wnode)
        q = next((path_nodes[i] for i in range(idx - 1, -1, -1)
                  if path_nodes[i] in shape["msg_text"]
                  and path_nodes[i] not in shape["humble"]), None)
        row["free-text question"] = shape["msg_text"].get(q, "") if q else ""
        if _value(values, k):
            row["free-text answer"] = _value(values, k)
            row["free-text answered at"] = _to_et(_value(values, k, "time"))
            _catch_all(row, run, values, k, shape, path, msgs, other_runs, flow_uuid)
        break

    # --- yes/no
    yn_wait = shape["wait_by_key"].get(yesno_key)
    if yn_wait in path_nodes:
        idx = path_nodes.index(yn_wait)
        q = next((path_nodes[i] for i in range(idx - 1, -1, -1)
                  if path_nodes[i] in shape["msg_text"]
                  and path_nodes[i] not in shape["humble"]), None)
        row["yes/no question"] = shape["msg_text"].get(q, "") if q else ""
        yn_cat = _value(values, yesno_key, "category")
        if _answered(values, yesno_key):
            row["yes/no reply"] = _value(values, yesno_key)
            row["yes/no answer"] = yn_cat.strip().capitalize()
            row["yes/no answered at"] = _to_et(_value(values, yesno_key, "time"))
        else:
            row["yes/no reply"] = _value(values, yesno_key) if yn_cat == "Other" else ""
            row["yes/no answer"] = "no response"
        yn_humble = [h for h, w in shape["humble"].items() if w == yn_wait]
        if any(h in path_nodes for h in yn_humble):
            yn_dt = _parse_iso(_value(values, yesno_key, "time"))
            row["yes/no rejected replies"] = _rejected(
                flow_msgs, q1_dt if _answered(values, q1_key) else None,
                yn_dt if _answered(values, yesno_key) else None,
                _value(values, yesno_key) if _answered(values, yesno_key) else "")

    # --- re-referral (the request-call step on "yes")
    rr = _yesno_branch_enter(shape, yesno_key)
    if rr and rr in path_nodes:
        t = _parse_iso(path[path_nodes.index(rr)].get("time"))
        child_flow = shape["enter"][rr][0]
        child = next((r for r in other_runs
                      if ((r.get("flow") or {}).get("uuid")) == child_flow
                      and t and _parse_iso(r.get("created_on"))
                      and abs((_parse_iso(r.get("created_on")) - t).total_seconds()) <= 120),
                     None)
        if child:
            row["re-referral step"] = child.get("exit_type") or "running"
            cv = child.get("values") or {}
            for col, key in (("ACMF form result", "acmf_submission"),
                             ("Referrals sheet result", "sheet_log")):
                if cv.get(key):
                    cat = cv[key].get("category") or ""
                    val = cv[key].get("value") or ""
                    row[col] = f"{cat} ({val})" if val else cat
        else:
            row["re-referral step"] = "entered (child run not found)"

    # --- reminder and window holds
    holds = 0
    for i, n in enumerate(path_nodes):
        if n in shape["window"]:
            nxt = path_nodes[i + 1] if i + 1 < len(path_nodes) else None
            if nxt in shape["waits"]:
                holds += 1
        if n in shape["reminder"]:
            row["reminder sent"] = "yes"
            row["reminder sent at"] = _to_et(path[i].get("time"))
            w = next((path_nodes[j] for j in range(i - 1, -1, -1)
                      if path_nodes[j] in shape["waits"]), None)
            row["reminder after"] = _node_label(shape, w, cfg) if w else ""
            wk = shape["waits"].get(w, "")
            row["_reminder after kind"] = (
                "the number question" if wk == q1_key else
                "the yes/no question" if wk == yesno_key else
                "the free-text question" if wk in ft_keys else "")
    if not row["reminder sent"]:
        row["reminder sent"] = "no"
    row["window holds"] = str(holds)

    # --- ending
    last_msg = next((n for n in reversed(path_nodes) if n in shape["msg_text"]), None)
    if run.get("exit_type") and last_msg:
        row["closing message"] = _first_words(shape["msg_text"][last_msg])
    if exited_dt:
        horizon = exited_dt + timedelta(days=ACMF_REPLIES_AFTER_DAYS)
        after = [(m.get("text") or "").strip() for m in msgs
                 if m["_dir"] == "in" and m["_dt"]
                 and exited_dt < m["_dt"] <= horizon and (m.get("text") or "").strip()]
        row["replies after end"] = ACMF_JOIN.join(after)
    row["step trail"] = " › ".join(
        f"{_et_short(p.get('time'))} {_node_label(shape, p.get('node'), cfg)}"
        for p in path)
    return row


def _catch_all(row, run, values, key, shape, path, msgs, other_runs, flow_uuid):
    """What the inbound catch-all did with a free-text answer. The survey enters
    it as a subflow right after the answer; its runs and messages fall between
    the answer and the survey's next step after the Enter-a-Flow node."""
    path_nodes = [p.get("node") for p in path]
    wnode = shape["wait_by_key"].get(key)
    i = path_nodes.index(wnode)
    enter_i = next((j for j in range(i + 1, len(path_nodes))
                    if path_nodes[j] in shape["enter"]), None)
    start = _parse_iso(_value(values, key, "time"))
    if enter_i is None or not start:
        row["catch-all category"] = "not entered"
        return
    end_i = enter_i + 1
    end = _parse_iso(path[end_i].get("time")) if end_i < len(path) else None
    lo = start - timedelta(seconds=2)
    hi = (end or datetime.now(timezone.utc)) + timedelta(seconds=2)

    window_runs = [r for r in other_runs
                   if _parse_iso(r.get("created_on"))
                   and lo <= _parse_iso(r.get("created_on")) <= hi]
    umf = next((r for r in window_runs if "bucket" in (r.get("values") or {})
                or ((r.get("values") or {}).get("referring_flow_id") or {}).get("value")
                == flow_uuid and "message_id" in (r.get("values") or {})), None)
    umf_flow = ((umf or {}).get("flow") or {}).get("uuid")
    uv = (umf or {}).get("values") or {}
    row["catch-all category"] = (uv.get("bucket") or {}).get("value") or (
        "not classified" if umf else "catch-all run not found")

    window_out = [m for m in msgs if m["_dir"] == "out" and m["_dt"]
                  and lo <= m["_dt"] <= hi
                  and ((m.get("flow") or {}).get("uuid")) != flow_uuid]
    row["humble line sent"] = "yes" if any(
        ((m.get("flow") or {}).get("uuid")) == umf_flow
        and "humble chatbot" in (m.get("text") or "").lower()
        for m in window_out) else "no"
    row["crisis numbers sent"] = "yes" if any(
        ((m.get("flow") or {}).get("uuid")) != umf_flow
        and "988" in (m.get("text") or "")
        for m in window_out) else "no"
    row["triage started"] = "yes" if any(
        "triagerequestid" in (r.get("values") or {}) for r in window_runs) else "no"

    msg_id = (uv.get("message_id") or {}).get("value")
    if msg_id:
        det = [r for r in other_runs
               if ((r.get("values") or {}).get("message_id") or {}).get("value") == msg_id
               and "determination" in (r.get("values") or {})]
        if det:
            d = det[-1]
            row["counselor determination"] = d["values"]["determination"].get("value", "")
            row["determination at"] = _to_et(d.get("created_on"))


# ---------------------------------------------------------------- the tab's row
def _form_ok(result):
    """'Success (200)' -> True when the code in brackets is 2xx."""
    m = re.search(r"\((\d{3})\)\s*$", result or "")
    return bool(m) and m.group(1).startswith("2")


def acmf_details(full, other_runs=()):
    """Every field not given its own column, one 'label: value' per line, empty
    ones omitted, under ACMF_DETAILS_MAX characters — the step trail is cut
    first, and the cell says so."""
    lines = [f"{f}: {full.get(f)}" for f in ACMF_DETAIL_FIELDS
             if f != "step trail" and str(full.get(f, "") or "").strip()]
    if other_runs:
        lines.append("other survey runs: " + "; ".join(other_runs))
    trail = str(full.get("step trail", "") or "")
    head = "\n".join(lines)
    if not trail:
        return head
    line = f"step trail: {trail}"
    room = ACMF_DETAILS_MAX - len(head) - 1
    if len(line) > room:
        steps = trail.split(" › ")
        kept = []
        for st in steps:
            note = (f"step trail (cut to fit the cell: first {len(kept) + 1} of "
                    f"{len(steps)} steps): ")
            if len(note) + len(" › ".join(kept + [st])) > room:
                break
            kept.append(st)
        line = (f"step trail (cut to fit the cell: first {len(kept)} of {len(steps)} "
                f"steps): " + " › ".join(kept))
    return (head + "\n" + line) if head else line


def acmf_sheet_row(full, other_runs=()):
    """The 14-column row from the full field set acmf_survey_row() builds."""
    ended = full.get("how it ended")
    if ended == "waiting":
        status = "Waiting"
    elif ended == "interrupted":
        status = ("Cut short by " + full["interrupted by"]) if full.get("interrupted by") \
            else "Cut short"
    elif ended == "expired":
        status = "Expired"
    elif full.get("reminder sent") == "yes":
        status = "No reply, reminder sent"
    else:
        status = "Answered"
    if full.get("q1 choice"):
        option = full["q1 choice"] + " – " + (full.get("q1 choice meaning") or "")
    elif full.get("_q1 timed out"):
        option = "No reply"
    else:
        option = ""
    triage = ""
    if full.get("counselor determination"):
        triage = full["counselor determination"] + (
            ", " + full["determination at"] if full.get("determination at") else "")
    rr = ""
    if full.get("re-referral step"):
        form = full.get("ACMF form result", "")
        if full["re-referral step"] == "completed" and _form_ok(form):
            rr = "sent to ACMF"
        else:
            rr = f"failed ({form or 'no form result'})"
    reminder = ""
    if full.get("reminder sent") == "yes":
        reminder = "after " + (full.get("_reminder after kind") or full.get("reminder after") or "")
    return {
        "name": full.get("name", ""),
        "uuid": full.get("uuid", ""),
        "survey": full.get("survey", ""),
        "sent": full.get("started", ""),
        "status": status,
        "option chosen": option,
        "message shown": full.get("message variant", ""),
        "their words": full.get("free-text answer", ""),
        "triage": triage,
        "wants follow-up": full.get("yes/no answer", ""),
        "re-referred": rr,
        "reminder": reminder,
        "replied after survey": full.get("replies after end", ""),
        "details": acmf_details(full, other_runs),
    }


def acmf_rows_by_contact(full_rows):
    """One tab row per contact: the latest run (by start), earlier runs named."""
    by = {}
    for r in full_rows:
        if r.get("uuid"):
            by.setdefault(r["uuid"], []).append(r)
    out = []
    for runs in by.values():
        runs.sort(key=lambda r: _parse_iso(r.get("_started utc")) or
                  datetime.min.replace(tzinfo=timezone.utc))
        latest = runs[-1]
        others = [f"{r['survey']} run {r['run uuid']} started {r['started']}"
                  for r in runs[:-1]]
        out.append(acmf_sheet_row(latest, others))
    return out


# ---------------------------------------------------------------- sheet side
def _acmf_sheet_post(route, body, allow_404=False):
    body = dict(body)
    body["password"] = SHEET_PASSWORD
    body["sheetid"] = SHEET_ID
    r = requests.post(f"{SHEET_SERVICE}/{route}", json=body, timeout=60)
    if allow_404 and r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def acmf_missing_headers():
    """Every expected column the tab's header row lacks. Read with /coordinates
    on row 1 (a fresh grid read, nocache), which matches header text the same
    way /write does (case-insensitive, trimmed) — so a column this reports as
    present is one /write will fill. /read cannot answer this on an empty tab."""
    coords = [{"sheet": ACMF_SURVEY_TAB, "column": c, "row": "_1",
               "result_name": f"h{i}"} for i, c in enumerate(ACMF_SURVEY_COLUMNS)]
    res = _acmf_sheet_post("coordinates", {"coordinates": coords, "nocache": True})
    return [c for i, c in enumerate(ACMF_SURVEY_COLUMNS) if f"h{i}" not in res]


def acmf_existing_rows():
    """One read of the tab -> {run uuid: {column: displayed value}}."""
    res = _acmf_sheet_post("read", {"tab": ACMF_SURVEY_TAB, "mode": "all"})
    out = {}
    for r in res.get("rows") or []:
        low = {str(k).strip().lower(): v for k, v in r.items()}
        key = str(low.get(ACMF_KEY_COL) or "").strip()
        if key:
            out[key] = {c: str(low.get(c.lower(), "") or "") for c in ACMF_SURVEY_COLUMNS}
    return out


def _sheet_safe(v):
    """Writes go in as USER_ENTERED, so a reply beginning with = + - @ would be
    taken as a formula. A leading apostrophe keeps it literal text."""
    v = "" if v is None else str(v)
    return "'" + v if v[:1] in ("=", "+", "-", "@") else v


_SHEET_DT_FORMATS = ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
                     "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M")


def _norm_cell(v):
    """Compare what we would write with what the sheet displays: dates and
    numbers are re-rendered by Sheets, so compare them by value."""
    s = str(v or "").strip()
    if s.startswith("'"):
        s = s[1:]
    for f in _SHEET_DT_FORMATS:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d %H:%M")
        except ValueError:
            pass
    try:
        return repr(float(s))
    except ValueError:
        return s


def acmf_row_changed(row, existing):
    return any(_norm_cell(row.get(c, "")) != _norm_cell(existing.get(c, ""))
               for c in ACMF_SURVEY_COLUMNS)


def write_acmf_row(row, exists):
    data = {"tab": ACMF_SURVEY_TAB, "key": ACMF_KEY_COL}
    for c in ACMF_SURVEY_COLUMNS:
        data[c] = _sheet_safe(row.get(c, ""))
    if exists:
        res = _acmf_sheet_post("write", {**data, "newrow": "no"}, allow_404=True)
        if res is not None and res.get("matched", 0) > 0:
            return "updated"
    _acmf_sheet_post("write", {**data, "newrow": "yes"})
    return "inserted"


class AcmfHeaderError(RuntimeError):
    pass


def build_acmf_rows():
    """Read both flows and build the tab's rows (one per contact). Reads only —
    no sheet call."""
    full = []
    for cfg in _acmf_flow_configs():
        flow_uuid = cfg[0]
        shape = flow_shape(fetch_flow_definition(flow_uuid))
        for run in iter_runs(flow_uuid):  # no `after` — full pull, tiny cohort
            contact_uuid = (run.get("contact") or {}).get("uuid")
            created = _parse_iso(run.get("created_on"))
            if not contact_uuid or not created:
                continue
            since = (created - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            msgs, others = contact_activity(contact_uuid, since)
            full.append(acmf_survey_row(run, cfg, shape, msgs, others))
    return acmf_rows_by_contact(full)


def run_acmf_survey(dry_run=False):
    """Upsert one row per contact (latest survey run) into ACMF_SURVEY_TAB. Returns {} when not
    configured. dry_run builds the rows and returns them without any sheet call."""
    if not _acmf_configured():
        return {}
    if not dry_run:
        missing = acmf_missing_headers()
        if missing:
            raise AcmfHeaderError(
                f"tab '{ACMF_SURVEY_TAB}' is missing column(s): " + ", ".join(missing))
    rows = build_acmf_rows()
    if dry_run:
        return {"acmf_survey_rows": len(rows), "rows": rows}
    existing = acmf_existing_rows()
    stats = {"acmf_survey_rows": len(rows), "acmf_survey_inserted": 0,
             "acmf_survey_updated": 0, "acmf_survey_unchanged": 0}
    for row in rows:
        key = row[ACMF_KEY_COL]
        if key in existing and not acmf_row_changed(row, existing[key]):
            stats["acmf_survey_unchanged"] += 1
            continue
        outcome = write_acmf_row(row, key in existing)
        time.sleep(WRITE_PACE_SEC)
        stats["acmf_survey_" + outcome] += 1
    return stats


# ================================================================================


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

    # 5) testimonial log — SEPARATE workbook, no-op unless configured. Runs after
    #    the referral funnel; its stats merge in under testimonial_* keys. A
    #    failure here would surface as a 500 like any other step; the testimonial
    #    write set is tiny so this is acceptable for the pilot. When it graduates
    #    to its own service, delete this call and the module above.
    stats.update(run_testimonial())

    # 6) follow-up survey log — fills F/G on the testimonial tab. Runs AFTER the
    #    testimonial pass so A-D exist for any row it needs to match; it is
    #    update-only and reports unmatched uuids rather than inserting them.
    stats.update(run_survey())

    # 7) ACMF follow-up survey answer log — its own tab of the referral-log
    #    workbook; no-op unless configured. A missing header raises, so the
    #    step fails visibly instead of writing a partial log.
    stats.update(run_acmf_survey())

    return stats


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/run-testimonial", methods=["POST"])
def run_testimonial_endpoint():
    """Testimonial log only — the referral funnel is untouched. Exists so the
    testimonial pass can be scheduled/run on its own cadence, and so it lifts out
    cleanly when it graduates to a standalone service (this endpoint becomes that
    service's `/run`)."""
    started = datetime.now(timezone.utc).isoformat()
    if not (TESTIMONIAL_FLOW and TESTIMONIAL_SHEET_ID):
        return jsonify({"status": "error",
                        "error": "TESTIMONIAL_FLOW / TESTIMONIAL_SHEET_ID not configured",
                        "started": started}), 400
    try:
        stats = run_testimonial()
    except Exception as e:
        log.exception("testimonial ingest failed")
        return jsonify({"status": "error", "error": str(e), "started": started}), 500
    return jsonify({"status": "ok", "started": started, **stats})


@app.route("/run-survey", methods=["POST"])
def run_survey_endpoint():
    """Survey log (F/G) only — nothing else touched. Lets the survey pass be run
    and verified on its own, and becomes the standalone service's `/run` if the
    testimonial log graduates."""
    started = datetime.now(timezone.utc).isoformat()
    if not (SURVEY_YES_FLOW or SURVEY_NONR_FLOW) or not TESTIMONIAL_SHEET_ID:
        return jsonify({"status": "error",
                        "error": "SURVEY_YES_FLOW / SURVEY_NONR_FLOW / "
                                 "TESTIMONIAL_SHEET_ID not configured",
                        "started": started}), 400
    try:
        stats = run_survey()
    except Exception as e:
        log.exception("survey ingest failed")
        return jsonify({"status": "error", "error": str(e), "started": started}), 500
    return jsonify({"status": "ok", "started": started, **stats})


@app.route("/run-acmf-survey", methods=["POST"])
def run_acmf_survey_endpoint():
    """ACMF survey answer log only — nothing else touched. Body {"dry_run": true}
    builds and returns the rows without any sheet call."""
    started = datetime.now(timezone.utc).isoformat()
    if not _acmf_configured():
        return jsonify({"status": "error",
                        "error": "ACMF_SURVEY_TAB / ACMF_DECLINED_FLOW / ACMF_NR_FLOW "
                                 "not configured",
                        "started": started}), 400
    body = request.get_json(silent=True) or {}
    try:
        stats = run_acmf_survey(dry_run=bool(body.get("dry_run")))
    except AcmfHeaderError as e:
        log.error("acmf survey: %s", e)
        return jsonify({"status": "error", "error": str(e), "started": started}), 409
    except Exception as e:
        log.exception("acmf survey ingest failed")
        return jsonify({"status": "error", "error": str(e), "started": started}), 500
    return jsonify({"status": "ok", "started": started, **stats})


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

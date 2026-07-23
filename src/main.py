"""
referral-journey-ingest — Cloud Run service.

Populates the `flow` tab (subscriber journey through the yellow outreach flow)
and self-heals missing `Referrals` rows, by reading the TextIt runs API for
three flows: the DKJ yellow flow (entry/response/provider) and the two partner
flows (ACMF / V4W) for the actual referral timestamp + fired/logged status.

Design notes:
- The runs API reports each run's FINAL recorded result state, so a late reply
  that TextIt still associates with the run is captured on the next pull. This
  is why the ingest reads runs instead of writing to the sheet from inside the
  flow (in-flow writes would miss late responses).
- Incremental by run `modified_on` per flow, stored in BigQuery. Full-rebuild
  available via {"rebuild": true}.
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

import requests
from flask import Flask, request, jsonify
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("referral-journey-ingest")

app = Flask(__name__)

# ---- config (console env vars, NOT cloudbuild.yaml) ----
PROJECT = os.environ["GCP_PROJECT"]
SECRET_PROJECT = os.environ.get("SECRET_PROJECT", PROJECT)
WATERMARK_TABLE = os.environ.get("WATERMARK_TABLE", "RESPONSES.referral_journey_watermark")

# TextIt
TEXTIT_BASE = "https://textit.com/api/v2"
TEXTIT_TOKEN = os.environ.get("TEXTIT_TOKEN")  # set in console; or Secret Manager (see README)

# Flow UUIDs
DKJ_FLOW = os.environ["DKJ_FLOW"]      # outreach ("yellow") flow UUID
ACMF_FLOW = os.environ["ACMF_FLOW"]    # partner referral flow UUID
V4W_FLOW = os.environ["V4W_FLOW"]      # partner referral flow UUID

# sheet-service
SHEET_SERVICE = os.environ["SHEET_SERVICE"]
SHEET_ID = os.environ["SHEET_ID"]
FLOW_TAB = os.environ.get("FLOW_TAB", "flow")
REFERRALS_TAB = os.environ.get("REFERRALS_TAB", "Referrals")
SHEET_PASSWORD = os.environ.get("SHEET_PASSWORD")  # gappscriptapi value; from Secret Manager in prod

# runtime seatbelt
PAGE_CEILING = int(os.environ.get("PAGE_CEILING", "500"))

bq = bigquery.Client(project=PROJECT)


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


# ---------------------------------------------------------------- watermark
def get_watermark(flow_key):
    q = f"SELECT watermark FROM `{PROJECT}.{WATERMARK_TABLE}` WHERE flow_key=@k"
    job = bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("k", "STRING", flow_key)]))
    for row in job.result():
        return row["watermark"]
    return None


def set_watermark(flow_key, value):
    # MERGE upsert (single-row) — table is (flow_key STRING, watermark STRING)
    q = f"""
    MERGE `{PROJECT}.{WATERMARK_TABLE}` T
    USING (SELECT @k AS flow_key, @v AS watermark) S
    ON T.flow_key = S.flow_key
    WHEN MATCHED THEN UPDATE SET watermark = S.watermark
    WHEN NOT MATCHED THEN INSERT (flow_key, watermark) VALUES (S.flow_key, S.watermark)
    """
    bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("k", "STRING", flow_key),
        bigquery.ScalarQueryParameter("v", "STRING", value),
    ])).result()


def _max_iso(a, b):
    if not a:
        return b
    if not b:
        return a
    return a if a >= b else b


# ---------------------------------------------------------------- sheet writes
def _sheet_write(body):
    body = dict(body)
    body["password"] = SHEET_PASSWORD
    body["sheetid"] = SHEET_ID
    r = requests.post(f"{SHEET_SERVICE}/write", json=body, timeout=60)
    r.raise_for_status()
    return r.json()


def upsert_flow_row(row):
    """Update the contact's `flow` row in place if present, else append.
    Keyed on uuid. Two-step: try newrow:no; if matched==0, newrow:yes."""
    data = {
        "tab": FLOW_TAB,
        "key": "uuid",
        "uuid": row["uuid"],
        "entry_timestamp": row.get("entry_timestamp", ""),
        "provider": row.get("provider", ""),
        "response": row.get("response", ""),
        "referral_timestamp": row.get("referral_timestamp", ""),
        "referral_fired": row.get("referral_fired", ""),
    }
    upd = _sheet_write({**data, "newrow": "no"})
    if upd.get("matched", 0) == 0:
        _sheet_write({**data, "newrow": "yes"})
        return "inserted"
    return "updated"


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


def referrals_has_uuid(uuid):
    """Check the Referrals tab for an existing row via sheet-service /read match."""
    r = requests.post(f"{SHEET_SERVICE}/read", json={
        "password": SHEET_PASSWORD,
        "sheetid": SHEET_ID,
        "tab": REFERRALS_TAB,
        "mode": "match",
        "key": "uuid",
        "uuid": uuid,
        "columns": ["uuid"],
    }, timeout=60)
    r.raise_for_status()
    data = r.json()
    # /read match returns {"status":"success","rows":[{...}]}; empty rows => not present
    return bool(data.get("rows"))


# ---------------------------------------------------------------- extraction
def dkj_row(run):
    """Journey fields from a DKJ run, or None if out of scope.

    Scope (option 1): a run is logged only if `provider` is set. `provider` is
    saved on the AZ/Other state branches, which are reachable ONLY down the
    Veteran path of the usertype split — so provider-present == a veteran who
    reached the referral offer. runs.json does not expose contact.fields, so
    usertype cannot be read directly; provider-presence is the proxy. The only
    population this misses is veterans who exited between the usertype split and
    the offer, who were never actually offered a referral.
    """
    values = run.get("values") or {}
    provider = (values.get("provider") or {}).get("value", "")
    if not provider:
        return None  # out of scope: never reached the offer
    resp = (values.get("Result 1") or {}).get("category", "")  # Yes / No / Other / "" (no response)
    return {
        "uuid": (run.get("contact") or {}).get("uuid"),
        "entry_timestamp": run.get("created_on", ""),
        "provider": provider,
        "response": "" if resp in ("", None) else resp,  # blank cell = no response (Dina's encoding)
        "modified_on": run.get("modified_on", ""),
    }


def partner_referral(run, submission_key):
    """From a partner run, return (referral_ts, fired_bool, sheet_logged_bool)."""
    values = run.get("values") or {}
    sub = values.get(submission_key) or {}
    slog = values.get("sheet_log") or {}
    fired = sub.get("category") == "Success"
    logged = slog.get("category") == "Success"
    ts = sub.get("time", "") if fired else ""
    return ts, fired, logged


def collect_partner(flow_uuid, submission_key, wm_key, rebuild):
    """Read a partner flow's runs; return {uuid: {ts, fired, logged, created_on}}
    keeping the LATEST run per uuid (max created_on). Advances its watermark."""
    after = None if rebuild else get_watermark(wm_key)
    latest = {}
    new_wm = after
    for run in iter_runs(flow_uuid, after=after):
        new_wm = _max_iso(new_wm, run.get("modified_on"))
        uuid = (run.get("contact") or {}).get("uuid")
        if not uuid:
            continue
        ts, fired, logged = partner_referral(run, submission_key)
        prev = latest.get(uuid)
        created = run.get("created_on", "")
        if prev is None or created >= prev["created_on"]:
            latest[uuid] = {"ts": ts, "fired": fired, "logged": logged, "created_on": created,
                            "provider": ("ACMF" if submission_key == "acmf_submission" else "Vets4Warriors")}
    if new_wm and not rebuild:
        set_watermark(wm_key, new_wm)
    elif rebuild and new_wm:
        set_watermark(wm_key, new_wm)
    return latest


def run_ingest(rebuild=False):
    # 1) partner runs first — build the referral lookup used to enrich journey rows
    acmf = collect_partner(ACMF_FLOW, "acmf_submission", "acmf", rebuild)
    v4w = collect_partner(V4W_FLOW, "v4w_submission", "v4w", rebuild)

    # merged partner lookup: latest across whichever partner the uuid hit
    partner = {}
    for src in (acmf, v4w):
        for uuid, rec in src.items():
            prev = partner.get(uuid)
            if prev is None or rec["created_on"] >= prev["created_on"]:
                partner[uuid] = rec

    # 2) DKJ runs -> journey rows (scoped to provider-present)
    after = None if rebuild else get_watermark("dkj")
    new_wm = after
    stats = {"flow_rows": 0, "inserted": 0, "updated": 0, "self_heals": 0, "skipped_no_provider": 0}
    for run in iter_runs(DKJ_FLOW, after=after):
        new_wm = _max_iso(new_wm, run.get("modified_on"))
        row = dkj_row(run)
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
        result = upsert_flow_row(row)
        stats["flow_rows"] += 1
        stats[result] += 1
    if new_wm:
        set_watermark("dkj", new_wm)

    # 3) self-heal: partner referral fired but sheet_log did NOT succeed AND
    #    no Referrals row exists -> write the missing Referrals row
    for uuid, rec in partner.items():
        if rec["fired"] and not rec["logged"]:
            if not referrals_has_uuid(uuid):
                heal_referral_row(uuid, rec["ts"], rec["provider"])
                stats["self_heals"] += 1

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

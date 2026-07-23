# referral-journey-ingest

Cloud Run service that populates the **`flow`** tab (subscriber journey through the
"yellow" outreach flow `Call for Support (DKJ)`) in the External Referral Log workbook,
and **self-heals** missing `Referrals` rows.

It reads the TextIt **runs API** for three flows and writes to Google Sheets via
`sheet-service`. It does NOT write from inside any TextIt flow — reading runs means a
late reply that TextIt still associates with the run is captured on the next pull,
which in-flow sheet writes would miss.

## What it writes

**`flow` tab** — one row per veteran who reached the referral offer, keyed on `uuid`:

| Column | Source |
|---|---|
| `uuid` | DKJ run `contact.uuid` |
| `entry_timestamp` | DKJ run `created_on` |
| `provider` | DKJ run `values.provider.value` (`ACMF` / `Vets4Warriors`) |
| `response` | DKJ run `values."Result 1".category` (`Yes`/`No`/`Other`; **blank = no response**) |
| `referral_timestamp` | latest partner run `values.acmf_submission.time` / `values.v4w_submission.time` |
| `referral_fired` | `yes` if the partner submission `category=Success`; `no` if response was Yes but no successful partner submission; blank otherwise |

**Scope (option 1):** only DKJ runs where `provider` is set. `provider` is saved on the
AZ/Other state branches, reachable only down the Veteran path of the usertype split, so
provider-present == a veteran who reached the offer. `runs.json` does NOT expose
`contact.fields`, so `usertype` can't be read directly — provider-presence is the proxy.
The only miss is veterans who exited between the usertype split and the offer (never
actually offered a referral).

**`Referrals` tab (self-heal only):** if a partner run shows the referral fired
(`acmf_submission`/`v4w_submission` `category=Success`) but its `sheet_log` result did
NOT succeed, the in-flow sheet-log write failed at runtime. If no `Referrals` row exists
for that uuid, this service writes the missing row (`uuid`, submission `time`,
`provider`, `source="self-heal (referral-journey-ingest)"`). The partner run reveals the
failure directly via its own `sheet_log` result — no sheet-diffing required.

## One-time setup

1. **`flow` tab header row** — create the tab with exactly these headers in row 1:
   `uuid | entry_timestamp | provider | response | referral_timestamp | referral_fired`
   (sheet-service `newrow:yes` scans the `uuid` key column for the first empty row; the
   header must exist or column mapping fails.)

2. **Watermark table** (BigQuery):
   ```sql
   CREATE TABLE IF NOT EXISTS `early-alert-responses.RESPONSES.referral_journey_watermark` (
     flow_key STRING, watermark STRING
   );
   ```
   Keys used: `dkj`, `acmf`, `v4w`. Empty table => first run behaves like a full pull
   (no `after` filter) and then records watermarks.

## Endpoints

```
GET  /health -> {"status":"ok"}
POST /run     -> incremental (default). Body {} .
POST /run     -> full rebuild:  body {"rebuild": true}   (ignores watermarks, re-reads all runs)
```

`/run` response: `{"status","mode","flow_rows","inserted","updated","self_heals","skipped_no_provider"}`.

## Config — Cloud Run CONSOLE env vars (NOT cloudbuild.yaml)

The continuous-deploy trigger ignores env/memory/timeout flags in `cloudbuild.yaml`
(the contacts-sync OOM lesson). Set in the console (Edit & deploy revision):

- `GCP_PROJECT=early-alert-responses`
- `TEXTIT_TOKEN` — TextIt API token (or wire Secret Manager; see below)
- `SHEET_PASSWORD` — the `gappscriptapi` value sheet-service checks
- `SHEET_ID=1CquixL95khVlhSrzWjSGetAevFX2-_kl2RgjFe0Q1hI`
- `DKJ_FLOW`, `ACMF_FLOW`, `V4W_FLOW` — flow UUIDs (defaults baked in)
- `PAGE_CEILING=500` — runtime seatbelt on pagination
- **Request timeout = 3600** (Container tab) — a rebuild pages many runs; 300s default 504s mid-crawl.

Secrets (`TEXTIT_TOKEN`, `SHEET_PASSWORD`) should live in Secret Manager and be mounted
as env vars via the console rather than pasted as plain env values, matching the rest of
the stack. Grant the runtime SA `secretmanager.versions.access` at project IAM (Editor
does NOT include it — the documented Secret Accessor gotcha).

## Rate limit

Every TextIt call goes through `_textit_get()`, which on HTTP 429 parses the
`"available in N seconds"` body and sleeps N+3 before retrying. TextIt's REST limit is
2,500 requests/hour per token; incremental pulls are small (only runs modified since the
last watermark), but a rebuild can be large — run rebuilds off-hours and never inside the
3-5 AM CT Meridian ETL window. This service shares the token budget with every other
TextIt-hitting job (see the `2026-07-22_textit_rate_limit_audit` handoff).

## Deploy

Standard Early Alert Cloud Run service pattern (see `cloud_run_service_deploy` runbook):
public repo `CirclesOfSupport/referral-journey-ingest`, Cloud Build continuous-deploy trigger
on push to `main`, `--no-allow-unauthenticated`. Cloud Scheduler job calls `/run` with an
OIDC token on the chosen cadence (hourly suggested, staggered off the top-of-hour if it
would collide with `webhook-log-ingest-hourly`).

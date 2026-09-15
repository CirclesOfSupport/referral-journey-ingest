# referral-journey-ingest

Cloud Run service that populates the **`Flow`** tab in the External Referral Log
workbook — the subscriber journey through the outreach flow
**`Request Call for Support (Yellow)`** (the "yellow", non-crisis referral offer) —
**self-heals** missing `Referrals` rows, and fills the **`outreach occured?`** column on
the `Referrals` tab from the separate **YES follow-up flow**.

It reads the TextIt **runs API** for four flows (the outreach flow, the two partner flows,
and the YES follow-up flow) and writes to Google Sheets via `sheet-service`. It does NOT
write from inside any TextIt flow — reading runs means a late reply that TextIt still
associates with the run is captured on the next pull, which in-flow sheet writes would
miss.

The outreach flow now carries its own nudges internally, and the YES follow-up answer is
tracked here — so this service is the single logger for the whole referral funnel. Two
former hand-run one-off back-fills (a disposable nudge-flow back-fill and a YES-flow
outreach back-fill) are retired: the nudges are read natively from the outreach flow's
runs, and the YES answer is an ongoing source below.

## What it writes

**`Flow` tab** — one row per veteran who reached the referral offer, keyed on `uuid`:

| Column | Source |
|---|---|
| `uuid` | outreach flow run `contact.uuid` |
| `entry_timestamp` | outreach flow run `created_on` |
| `provider` | outreach flow run `values.provider.value` (`ACMF` / `Vets4Warriors`) |
| `response` | outreach flow run yes/no, coalesced across `values.result_1.category` (in-window / first-nudge wait) and `values.result.category` (post-second-nudge terminal wait); `Yes`/`No`/`Other`; **blank = no response** |
| `referral_timestamp` | latest partner run `values.acmf_submission.time` / `values.v4w_submission.time` |
| `referral_fired` | `yes` if the partner submission `category=Success`; `no` if response was Yes but no successful partner submission; blank otherwise |
| `last_modified` | outreach flow run `modified_on` — **this column is the watermark source** |
| `nudge` | consolidated outreach flow `values.nudgecount.value`, written **only** when it is `1` or `2`; any other value (0/blank/old-flow runs without the result) leaves the cell untouched, preserving manually-entered values |

**Scope:** only outreach flow runs where `provider` is set. `provider` is saved on the
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

**`Referrals` tab (`outreach occured?` column):** the YES follow-up flow asks referred
subscribers "did they reach out to you?" a few days later. This service reads that flow's
runs and writes the `Yes`/`No` answer into the `outreach occured?` column (exact header
text, lowercase, "occured", trailing "?"), update-in-place keyed on `uuid`, one column
only. The answer is coalesced across `values.result_2.category` (initial wait) and
`values.result_3.category` (post-second-nudge terminal wait) — a subscriber who replies
only after the second nudge has the earlier key blank. `result_1` on the YES flow is a
different question (a re-offer) and is deliberately NOT read. A `uuid` with no `Referrals`
row is counted as `outreach_404` and skipped, never appended — a responder should already
have a row from the partner referral.

## Testimonial log (separate program, separate workbook)

This service also carries a **self-contained testimonial-log module**: a distinct program
(video testimonials collected via Senja) that reuses the same runs-API-not-in-flow-writes
plumbing but is otherwise independent — its own flow, its own workbook, its own read and
write. It is written as a lift-out module and is expected to graduate to its own
`testimonial-log` service as the testimonial ask scales; nothing in the referral funnel
depends on it, and it is a no-op unless configured.

It reads the **Video Testimonial Request** flow's runs and upserts a 4-column tab in its
own workbook, keyed on `uuid`:

| Column | Source |
|---|---|
| `uuid` | run `contact.uuid` (every run == a testimonial ask was sent) |
| `timestamp` | run `created_on` (when the ask went out) |
| `Response` | `values.result.category` — `Yes`/`No`/`Other`; **blank = never answered** |
| `Response timestamp` | `values.result.time` — when that answer was set (the reply moment); blank if unanswered |

The flow has a single wait node (result name `Result`, runs key `result`). Its "Other"
branch reprompts and loops back into the **same** wait node, so a subscriber who first
sends something unparseable and then replies Yes/No overwrites the result in place;
`runs.json` reports only the final state, so the last answer is what lands — no special
handling needed.

**No watermark:** the pilot cohort is tiny, so every run is a full pull each time. This is
what guarantees a late answer (Other→loop, or a delayed reply) lands. Idempotent
uuid-keyed upsert, so re-stamping an unchanged row is inert.

**Endpoints:** the testimonial pass runs at the end of `/run` (stats merge in under
`testimonial_*` keys), and also has its own `POST /run-testimonial` for running it alone.
When `TESTIMONIAL_FLOW`/`TESTIMONIAL_SHEET_ID` are unset, both are no-ops (the dedicated
endpoint returns 400).

**One-time setup:** create the testimonial tab with exactly these headers in row 1:
`uuid | timestamp | Response | Response timestamp`.

## Follow-up survey log (columns F and G)

Fills two further columns on the **same tab** the testimonial log owns A-D of, from two
follow-up survey flows sent to that cohort and split by their original Yes/No answer.
Sibling module to the testimonial log — own reads, own write, lifts out with it.

| Column | Header (exact) | Source |
|---|---|---|
| F | `Survey Response (Survey Field)` | the option router's final `category` — the label TEXT ("Busy", not "2"), since the numbers map to different content in each flow |
| G | `Open-Ended Response (Concat: Other + Free Text)` | "other plus free text" — the contact's unparseable `Other` replies plus the `Something Else` elaboration, chronological, joined ` \| ` |

**Targets resolve by HEADER NAME, never by column letter.** A hand-maintained
`Test #2 Sent? ` column sits at E (note the trailing space in that header). Writing by
letter would overwrite it.

**This module never touches A-D or E.** Every write names only `uuid`, F and G, and is
**update-only** (`newrow: "no"`): a survey run whose uuid is not already on the tab is
counted as `survey_unmatched` and logged, never appended as a partial row.

**Result names are per-flow, not shared** — reading a single `result` key would silently
miss the second flow:

| Flow | Option result | Free-text result |
|---|---|---|
| `SURVEY_YES_FLOW` (…Request - YES) | `result` | `result_3` |
| `SURVEY_NONR_FLOW` (…Request - NO and NR) | `result_4` | `result_5` |

**Why G comes from `messages.json`, not runs:** the `Other` (unparseable) branch reprompts
and loops back into the same option router, so TextIt overwrites the result in place. That
overwrite IS the desired override behaviour — the final valid choice ends up in F with no
logic needed here — but it also destroys the original unparseable text inside `values`, and
runs.json carries no message text (`path` is node + time only). That original text is what
the log is meant to keep, so G is built from the messages endpoint. Note the endpoint has no
`flow` query param: it is filtered by `contact` and scoped to the flow client-side.

**G holds ONLY unstructured content — never menu digits.** A contact who picked a valid
option on the first try has nothing unstructured to record, so G is correctly BLANK; their
answer is fully represented by F's label. Echoing the digit into G would duplicate F and, since
the two flows map the same digit to different labels, would be actively misleading.

Scoping is by TIME, not by inspecting text. Every failed `Other` attempt arrives strictly
BEFORE the moment the final option result was set, so messages at/after that instant (the valid
selection) are excluded. When the final category is itself `Other` the contact never landed a
valid selection, so nothing parsed and every incoming message qualifies — including the last,
whose timestamp equals the result time.

An `Other` is detected from the run's `path`: the loop-back overwrites the result, so `values`
cannot reveal that it happened, but the reprompt node's presence in the path is definitive.
The messages endpoint is therefore consulted ONLY for runs that actually hit `Other` — the
common first-try-clean case costs no request at all.

The override applies to `Other` only. `Something Else` (the last numbered option) routes to
the free-text catcher and terminates, with no path back to the router.

**Endpoints:** runs at the end of `/run` (stats merge under `survey_rows` / `survey_updated`
/ `survey_unmatched`), and standalone `POST /run-survey`. No-op when unconfigured.

**Accepted risk:** the option routers use `has_number_eq`, so a free-text reply containing a
digit can misroute (a complaint containing "2" lands in the option-2 bucket). Kept
deliberately for a small run; revisit for larger campaigns.

## One-time setup

1. **`Flow` tab header row** — create the tab with exactly these headers in row 1:
   `uuid | entry_timestamp | provider | response | referral_timestamp | referral_fired | last_modified | nudge`
   (sheet-service `newrow:yes` scans the `uuid` key column for the first empty row; the
   header must exist or column mapping fails.)

2. **No watermark table needed.** The watermark is derived from the sheet itself:
   `max(last_modified)` across the `Flow` tab, read via sheet-service
   `/read` `mode:"all"` with `columns:["last_modified"]`. An empty tab => no
   watermark => full pull, which is correct on first run.

## Endpoints

```
GET  /health -> {"status":"ok"}
POST /run     -> incremental (default). Body {} .
POST /run     -> full rebuild:  body {"rebuild": true}   (ignores watermarks, re-reads all runs)
```

`/run` response: `{"status","mode","flow_rows","inserted","updated","self_heals","skipped_no_provider","outreach_updated","outreach_404","outreach_blank"}`, plus `testimonial_rows`/`testimonial_inserted`/`testimonial_updated` when the testimonial module is configured (see below).

`POST /run-testimonial` — the testimonial log only (referral funnel untouched); returns `{"status","testimonial_rows","testimonial_inserted","testimonial_updated"}`, or 400 if the testimonial vars are unset.

## Config — Cloud Run CONSOLE env vars (NOT cloudbuild.yaml)

The continuous-deploy trigger does not read env, memory, or timeout flags from
`cloudbuild.yaml`. Set them in the console (Edit & deploy revision):

- `TEXTIT_TOKEN` — TextIt API token
- `SHEET_PASSWORD` — the `gappscriptapi` value sheet-service checks
- `SHEET_ID` — the External Referral Log workbook id
- `SHEET_SERVICE` — sheet-service base URL
- `FLOW_TAB=Flow` — **must be set**; the tab is `Flow` (capital F) and the code default is lowercase `flow`
- `YELLOW_FLOW`, `ACMF_FLOW`, `V4W_FLOW`, `YES_FLOW` — flow UUIDs. `YELLOW_FLOW` is the
  CONSOLIDATED outreach flow (yellow + nudges in one); `YES_FLOW` is the separate YES
  follow-up flow.
- `OUTREACH_COL` — optional; the Referrals-tab column header for the outreach answer
  (default `outreach occured?`, matching the tab's exact spelling). Only set to override.
- `PAGE_CEILING=500` — runtime seatbelt on pagination
- **Request timeout = 3600** (Container tab) — a rebuild pages many runs; 300s default 504s mid-crawl.

Testimonial-log module (all optional — unset = the testimonial pass is skipped, referral
funnel unaffected):

- `TESTIMONIAL_FLOW` — Video Testimonial Request flow UUID
- `TESTIMONIAL_SHEET_ID` — the testimonial workbook id (a DIFFERENT workbook from `SHEET_ID`)
- `TESTIMONIAL_TAB` — the testimonial tab name (default `Sheet1`; set to the actual tab)

Follow-up survey module (optional — unset = survey pass skipped):

- `SURVEY_YES_FLOW` — "…Video Testimonial Request - YES" flow UUID
- `SURVEY_NONR_FLOW` — "…Video Testimonial Request - NO and NR" flow UUID

## Rate limit

Every TextIt call goes through `_textit_get()`, which on HTTP 429 parses the
`"available in N seconds"` body and sleeps N+3 before retrying. TextIt's REST limit is
2,500 requests/hour per token. Incremental pulls are small (only runs modified since the
last watermark), but a rebuild can be large, so run rebuilds off-hours. The budget is per
token, so it is shared with any other job using the same one — schedule accordingly.

## Deploy

Cloud Build continuous-deploy trigger on push to `main`; Cloud Run service deployed with
`--no-allow-unauthenticated`. A Cloud Scheduler job calls `/run` with an OIDC token on the
chosen cadence (hourly suggested; stagger it off the top of the hour if other scheduled
jobs share the TextIt token).

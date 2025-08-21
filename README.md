## Internal Helena Tool for Juniper Square → Affinity Sync

A secure, serverless tool to sync prospect pipeline statuses from a daily Juniper Square CSV export into Affinity.

- Upload a CSV via a protected UI
- We match names by type (Organization vs Person), exact → fuzzy, and via cross-field associations
- We derive the best Affinity pipeline status from Subscription/Data Room signals (with business rules)
- We write updates to an Affinity ranked-dropdown field without downgrading and never overwriting "Passed"

### Repo layout
- `index.html` — Single-page UI (Helena branding) to upload CSV, provide API key, and choose dry-run
- `api/upload.js` — Serverless function (Vercel/Node 18) that parses CSV, matches rows to Affinity list entries, derives statuses, and updates Affinity
- `package.json` — Minimal deps for the API (`axios`, `csv-parse`, `string-similarity`)

## Quick start

### Prerequisites
- Node.js ≥ 18 (for local dev or Vercel CLI)
- Affinity v2 API token with access to the target list
- The Affinity list ID you want to update (configured in code as `LIST_ID`)

### Install
```bash
npm install
```

### Run locally (with Vercel)
```bash
# Set environment variables (see below)
vercel dev
# UI at http://localhost:3000
# API at http://localhost:3000/api/upload
```

### Deploy
- Recommended: Vercel
  - Push to GitHub, connect project in Vercel
  - Add environment variables in Vercel Project Settings → Environment Variables
  - Deploy; the UI and API will be served from your Vercel domain

## Environment variables
- Required
  - `AFFINITY_V2_TOKEN` — Affinity v2 Bearer token
- Recommended
  - `CSV_SYNC_API_KEY` — API key required by the UI/API (`x-api-key`, or Basic/Bearer). Prevents unauthorized uploads
- Optional (behavior/config)
  - `STATUS_FIELD_NAME` — Name of the dropdown field to update in Affinity list. Default: `Status`. If your field is `Pipeline Status`, set this accordingly
  - `MIN_STATUS_LABEL` — Minimum pipeline stage required to act. Default: `Invited to Data Room`
  - `MAX_CSV_BYTES` — Upload size limit in bytes. Default: `2000000`
  - `REDACT_RESPONSE` — If `1`, omits names/IDs from response payload
  - `PREFER_ORGANIZATIONS` — If `1`, bias ambiguous matches toward org entries
- Optional (labels/mapping)
  - `STATUS_LABEL_TO_ID_JSON` — Explicit mapping from status label → dropdown option ID (JSON object)
  - `STATUS_LABEL_ALIASES_JSON` — Mapping of incoming labels/aliases → canonical labels (JSON object)
  - `SUB_STATUS_TO_STAGE_JSON` — Mapping from Juniper Square Subscription Status phrases → Affinity stage labels (JSON object)
  - `NICKNAME_ALIASES_JSON` — Mapping of canonical first names → nickname variants (JSON object: `{ "matthew": ["matt"] }`)
- Optional (auth alternatives)
  - `BASIC_AUTH_USER`, `BASIC_AUTH_PASS` — If set, Basic auth is accepted in addition to `x-api-key`

## CSV expectations
The tool is robust to column names, but these are primarily used:
- Names
  - `Organization`
  - `Contacts` (semicolon- or bullet-separated allowed)
- Status signals
  - `Subscription Status`
  - `Data room granted`
  - `Data room last accessed`
  - `Data room access detail` (semicolon-separated segments, e.g., "Name: not yet accessed" or "Name: Aug 19, 2025")
  - `Prospect Status` (low-resolution; used as a fallback only)
  - `Latest update` (free text; used for hints)

## Matching logic (type-safe and association-aware)
We avoid org↔person mismatches and handle common naming issues.

- Indexes
  - Organizations: normalized `entity.name` with legal suffixes stripped (Inc, LLC, Ltd, LLP, BV, GmbH, SARL, etc.)
  - People: `first_name`/`last_name` when available, otherwise `entity.name`. Also index nickname variants (e.g., Matthew↔Matt)
  - Associations: We fetch People/Organization fields from the Affinity list and index associated names per entry (and we add the entry’s own name into its association set)
- Matching order
  1) Pair match (CSV Organization + Contacts) against Affinity associations (exact → fuzzy)
  2) Association fallback: if the contact exists only in People field of an org entry (or vice-versa), resolve via association indices
  3) Type-only matching (organization-first, then people) exact → fuzzy
- Fuzzy thresholds
  - Org: 0.88 (or 0.90+ when validating pairs)
  - Person: 0.85 (or 0.90+ when validating pairs)
- Normalization
  - Lowercase, accent removal, punctuation/whitespace cleanup
  - Legal suffix stripping for orgs
  - Nickname expansions for first names (overridable via `NICKNAME_ALIASES_JSON`)

## Status derivation (prioritized)
We derive a single Affinity pipeline stage per row with the following priority:

1) Subscription Status (highest priority)
   - Examples (customizable via `SUB_STATUS_TO_STAGE_JSON`):
     - `Countersigned`, `Fully executed`, `Signed` → `Sub Docs Signed`
     - `Awaiting investor signature`, `Staff review: pending` → `Ready for Sub Docs`
     - `Started`, `Draft`, `Invited` → `Sub Docs Sent`
2) Data room signals
   - `Data room access detail` → if any segment is not "not yet accessed" and is date-like, set `Data Room Accessed / NDA Executed`
   - `Data room last accessed` → if present and not "not yet accessed", set `Data Room Accessed / NDA Executed`
   - `Data room granted` → if granted/yes, set `Invited to Data Room`
3) Fallback hints
   - Ignore `soft-circled` as authoritative
   - Use `Latest update`/`Prospect Status` for early stages like `Intro/First Meeting`, `Deck & PPM Sent`, etc.

### Business rules
- Never downgrade: we won’t move backwards in the ordered pipeline
- Hard lock: if current status is `Passed` (or configured synonyms), do not change
- Minimum threshold: skip updates below `MIN_STATUS_LABEL` (default `Invited to Data Room`)

## Writing to Affinity
- We update a ranked-dropdown list field via v2 API: `POST /v2/lists/{listId}/list-entries/{entryId}/fields/{fieldId}`
- Payload format:
```json
{
  "value": {
    "type": "ranked-dropdown",
    "data": { "dropdownOptionId": 123456 }
  }
}
```
- The correct `dropdownOptionId` is resolved by:
  - Reading the field’s options from the list (or learning options by scanning entries),
  - Applying `STATUS_LABEL_ALIASES_JSON`, and/or
  - Using `STATUS_LABEL_TO_ID_JSON` overrides when necessary

## UI usage
1) Navigate to your deployed domain
2) Enter the API key (required)
3) Toggle Dry Run for a preview (no writes)
4) Drag & drop or choose your CSV file, then Upload & Sync
5) Review the result card for `ok`, `total`, and per-row outcomes (includes `matchType`, `score`, and `reason`)

## API usage (cURL)
```bash
# Dry run
curl -X POST \
  -H "Content-Type: text/csv" \
  -H "x-api-key: $CSV_SYNC_API_KEY" \
  --data-binary @/path/to/export.csv \
  "https://<your-vercel-domain>/api/upload?dry=1"

# Actual write
curl -X POST \
  -H "Content-Type: text/csv" \
  -H "x-api-key: $CSV_SYNC_API_KEY" \
  --data-binary @/path/to/export.csv \
  "https://<your-vercel-domain>/api/upload"
```

## Troubleshooting
- 401 Unauthorized
  - Set `CSV_SYNC_API_KEY` in the server environment and include it as `x-api-key` header (or configure Basic/Bearer)
- 413 CSV too large
  - Reduce CSV size or increase `MAX_CSV_BYTES`
- "Unknown status '<label>' for field '<name>'"
  - Ensure `STATUS_FIELD_NAME` matches your actual Affinity field (e.g., `Pipeline Status`)
  - Provide `STATUS_LABEL_TO_ID_JSON` mapping, e.g.:
```json
{
  "Invited to Data Room": 19878021,
  "Data Room Accessed / NDA Executed": 19878022,
  "Ready for Sub Docs": 19878023,
  "Sub Docs Sent": 19878024,
  "Sub Docs Signed": 19878025,
  "Committed": 19878026,
  "Passed": 19878035
}
```
  - Optionally add `STATUS_LABEL_ALIASES_JSON` to translate synonyms to exact labels
- Match failures (`matched: false`, `No suitable org/person match`)
  - Verify the CSV `Organization` and `Contacts` values
  - Confirm the Affinity list entries contain People/Organization associations
  - Tune thresholds or add nickname aliases via `NICKNAME_ALIASES_JSON`
- "The I/O read operation failed" on Vercel
  - Ensure `Content-Type: text/csv` is set and the upload size is within `MAX_CSV_BYTES`

## Security & privacy
- API requires an API key (recommended) and sets `Cache-Control: no-store`
- Size-limited uploads (`MAX_CSV_BYTES`)
- Optional response redaction (`REDACT_RESPONSE=1`)
- Keep `AFFINITY_V2_TOKEN` and `CSV_SYNC_API_KEY` in your server env (Vercel project settings)

## Notes
- This tool is designed for one Affinity list (`LIST_ID` is set in code). If you need to support multiple lists, consider parameterizing the list and field IDs per deployment
- If you change your pipeline labels or add stages in Affinity, update `STATUS_LABEL_TO_ID_JSON` (or rely on auto-learn + aliases) to keep mapping accurate

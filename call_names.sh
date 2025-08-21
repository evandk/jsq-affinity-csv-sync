#!/usr/bin/env bash
set -euo pipefail

# load .env if present (v2 token)
set -a; [ -f ./.env ] && source ./.env; set +a
AFFINITY_V2_TOKEN=${AFFINITY_V2_TOKEN:-}
if [ -z "${AFFINITY_V2_TOKEN}" ]; then
  echo "Missing AFFINITY_V2_TOKEN"; exit 1
fi

LIST_ID=300305
STATUS_FIELD='field-5140811'  # Pipeline Status

URL="https://api.affinity.co/v2/lists/$LIST_ID/list-entries?fieldIds[]=$STATUS_FIELD"
while [ -n "$URL" ]; do
  RESP=$(curl -s -H "Authorization: Bearer $AFFINITY_V2_TOKEN" "$URL")
  # If .data is null or not an array, skip to next page (or break if no nextUrl)
  if ! echo "$RESP" | jq -e '.data | type == "array"' >/dev/null; then
    URL=$(echo "$RESP" | jq -r '.pagination.nextUrl // ""')
    [ -z "$URL" ] && break
    continue
  fi
  # Only iterate if .data is an array
  echo "$RESP" | jq -r --arg FIELD "$STATUS_FIELD" '
    .data[]
    | .entity.name as $n
    | (.fields // [])
    | map(select(.id==$FIELD))[]
    | [$n, .value.data.text, .value.data.dropdownOptionId] | @tsv
  '
  URL=$(echo "$RESP" | jq -r '.pagination.nextUrl // ""')
done | column -t
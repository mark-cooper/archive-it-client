#!/usr/bin/env bash
set -euo pipefail

: "${ARCHIVE_IT_USERNAME:?ARCHIVE_IT_USERNAME must be set}"
: "${ARCHIVE_IT_PASSWORD:?ARCHIVE_IT_PASSWORD must be set}"

cd "$(dirname "$0")"
mkdir -p fixtures fixtures.local

# Step 1: fetch raw responses into fixtures.local/ (gitignored).
curl -fsSL \
  "https://partner.archive-it.org/api/" \
  | jq > fixtures.local/api.json
curl -fsSL \
  "https://partner.archive-it.org/api/account" \
  | jq > fixtures.local/api_accounts_public.json
curl -fsSL -u "$ARCHIVE_IT_USERNAME:$ARCHIVE_IT_PASSWORD" \
  "https://partner.archive-it.org/api/account" \
  | jq > fixtures.local/api_accounts_authenticated.json
curl -fsSL \
  "https://partner.archive-it.org/api/account/484" \
  | jq > fixtures.local/api_account_public.json
curl -fsSL -u "$ARCHIVE_IT_USERNAME:$ARCHIVE_IT_PASSWORD" \
  "https://partner.archive-it.org/api/account/484" \
  | jq > fixtures.local/api_account_authenticated.json
curl -fsSL \
  "https://partner.archive-it.org/api/collection?account=484&limit=100" \
  | jq > fixtures.local/api_collections_public.json
curl -fsSL -u "$ARCHIVE_IT_USERNAME:$ARCHIVE_IT_PASSWORD" \
  "https://partner.archive-it.org/api/collection?account=484&limit=100" \
  | jq > fixtures.local/api_collections_authenticated.json
curl -fsSL \
  "https://partner.archive-it.org/api/collection/2135" \
  | jq > fixtures.local/api_collection_public.json
curl -fsSL -u "$ARCHIVE_IT_USERNAME:$ARCHIVE_IT_PASSWORD" \
  "https://partner.archive-it.org/api/collection/2135" \
  | jq > fixtures.local/api_collection_authenticated.json
curl -fsSL -u "$ARCHIVE_IT_USERNAME:$ARCHIVE_IT_PASSWORD" \
  "https://partner.archive-it.org/wasapi/v1/webdata?collection=4472" \
  | jq > fixtures.local/wasapi_collection.json

# Step 2: derive committed fake fixtures in fixtures/ from local raw data.
fake() { jq -f fakeify.jq "fixtures.local/$1.json" > "fixtures/$1.json"; }

fake api
fake api_accounts_public
fake api_accounts_authenticated
fake api_account_public
fake api_account_authenticated
fake api_collections_public
fake api_collections_authenticated
fake api_collection_public
fake api_collection_authenticated
fake wasapi_collection

# Sanity check: fail loudly if real-looking data leaks into committed fixtures/.
leaks="$(
  jq -r '
    .. | strings
    | select(
        (test("[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}"; "i") and . != "fake@example.invalid")
        or (test("https?://"; "i") and . != "https://example.invalid/fake")
        or test("UA-[0-9]+-[0-9]+")
        or test("[a-f0-9]{32,}"; "i")
      )
  ' fixtures/*.json
)"

if [[ -n "$leaks" ]]; then
  echo "ERROR: real-looking data leaked into fixtures/" >&2
  echo "$leaks" >&2
  exit 1
fi

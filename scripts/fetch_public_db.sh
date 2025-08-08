#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/fetch_public_db.sh
#   TAG=v2025.08.08 scripts/fetch_public_db.sh

OWNER="tom-mcmillan"
REPO="nwsl_data"
DB_PATH="data/processed/nwsldata.db"
CHECKSUM_PATH="nwsldata.db.sha256"

mkdir -p "$(dirname "$DB_PATH")"

have_cmd() { command -v "$1" >/dev/null 2>&1; }

sha256_file() {
  if have_cmd sha256sum; then
    sha256sum "$1" | awk '{print $1}'
  elif have_cmd shasum; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "No sha256 tool found (install sha256sum or shasum)."; exit 2
  fi
}

if [[ -z "${TAG:-}" ]]; then
  if have_cmd gh; then
    TAG="$(gh release view --repo "$OWNER/$REPO" --json tagName -q .tagName)"
  else
    TAG="$(curl -fsSL "https://api.github.com/repos/$OWNER/$REPO/releases/latest" \
      | sed -n 's/.*"tag_name": *"\(.*\)".*/\1/p' | head -n1)"
  fi
fi

if [[ -z "${TAG:-}" ]]; then
  echo "Could not determine release tag. Set TAG=... and retry."; exit 3
fi

echo "==> Fetching nwsl_data DB from release: $TAG"

BASE="https://github.com/$OWNER/$REPO/releases/download/$TAG"
DB_URL="$BASE/nwsldata.db"
SUM_URL="$BASE/nwsldata.db.sha256"

curl -fL --progress-bar -o "$DB_PATH" "$DB_URL"
curl -fL --progress-bar -o "$CHECKSUM_PATH" "$SUM_URL"

EXPECTED="$(awk '{print $1}' "$CHECKSUM_PATH")"
ACTUAL="$(sha256_file "$DB_PATH")"

if [[ "$EXPECTED" != "$ACTUAL" ]]; then
  echo "Checksum mismatch!"
  echo "Expected: $EXPECTED"
  echo "Actual:   $ACTUAL"
  exit 4
fi

echo "✅ Downloaded & verified: $DB_PATH"

#!/bin/bash
# test_api.sh
# Tests the three deployed API Gateway endpoints.
# Usage: bash test_api.sh <api-base-url>
# Example: bash test_api.sh https://e7f45mcg68.execute-api.us-east-2.amazonaws.com/dev

set -e

BASE_URL="${1:-}"

if [ -z "$BASE_URL" ]; then
  echo "Usage: bash test_api.sh <api-base-url>"
  echo "Example: bash test_api.sh https://e7f45mcg68.execute-api.us-east-2.amazonaws.com/dev"
  exit 1
fi

SESSION_KEY="9158"

# Find a working Python (skips Windows Store fake aliases)
PYTHON=""
for candidate in python3 python; do
  p=$(command -v "$candidate" 2>/dev/null || true)
  if [ -n "$p" ] && "$p" -c "import sys" 2>/dev/null; then
    PYTHON="$p"
    break
  fi
done

if [ -z "$PYTHON" ]; then
  echo "Error: Python not found. Please install Python 3."
  exit 1
fi

echo ""
echo "=== Testing API: $BASE_URL ==="

echo ""
echo "--- [1] GET /sessions ---"
curl -sf "$BASE_URL/sessions" | $PYTHON -m json.tool

echo ""
echo "--- [2] GET /sessions/$SESSION_KEY ---"
curl -sf "$BASE_URL/sessions/$SESSION_KEY" | $PYTHON -m json.tool

echo ""
echo "--- [3] POST /sessions/$SESSION_KEY/ingest ---"
curl -sf -X POST "$BASE_URL/sessions/$SESSION_KEY/ingest" | $PYTHON -m json.tool

echo ""
echo "=== All tests passed ==="

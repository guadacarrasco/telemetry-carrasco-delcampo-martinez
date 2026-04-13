#!/bin/bash
# deploy.sh
# Builds and deploys the Week 2 API Gateway SAM application.
# Usage: bash deploy.sh
#
# Prerequisites:
#   - AWS CLI configured (aws configure)
#   - SAM CLI installed (pip install aws-sam-cli)
#   - Python 3.11 installed

set -e

echo "=== Week 02 - API Gateway: Build & Deploy ==="

# 1. Build
echo ""
echo "[1/2] Building SAM application..."
sam build

# 2. Deploy (uses saved samconfig.toml — no prompts)
echo ""
echo "[2/2] Deploying to AWS..."
sam deploy

echo ""
echo "=== Deployment complete! ==="
echo "Run 'make test URL=<your-api-url>' to verify the endpoints."

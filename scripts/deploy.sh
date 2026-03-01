#!/usr/bin/env bash
set -euo pipefail

echo "Deploying bundle..."
echo "DATABRICKS_HOST=${DATABRICKS_HOST:-<not set>}"
echo "Target=dev"

# Validate bundle config, then deploy to target (default: dev)
databricks bundle validate
databricks bundle deploy -t dev
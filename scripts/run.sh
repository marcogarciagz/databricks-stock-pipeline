#!/usr/bin/env bash
set -euo pipefail

echo "Running job from bundle..."
echo "DATABRICKS_HOST=${DATABRICKS_HOST:-<not set>}"
echo "Target=dev"

# Run the job resource key from databricks.yml
databricks bundle run stock_demo_pipeline -t dev
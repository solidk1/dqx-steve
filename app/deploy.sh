#!/usr/bin/env bash
set -euo pipefail

APP_NAME="dqx-steve"
TARGET="${1:-dev}"
SOURCE_CODE_PATH="/Workspace/Users/steve.shao@databricks.com/.bundle/${APP_NAME}/${TARGET}/files/.build"

# 1. Build + upload files to workspace
echo "==> Building and uploading files..."
databricks bundle deploy --force-lock -t "$TARGET"

# 2. Create app if it doesn't exist
if ! databricks apps get "$APP_NAME" -t "$TARGET" &>/dev/null; then
  echo "==> App '${APP_NAME}' not found, creating..."
  databricks apps create --name "$APP_NAME" --description "Databricks DQX App" -t "$TARGET" --no-wait
fi

# 3. Ensure the app requests the valid OBO scopes supported by Databricks Apps.
echo "==> Updating app user API scopes..."
databricks apps update "$APP_NAME" -t "$TARGET" --json '{
  "description": "DQX Builder App",
  "user_api_scopes": [
    "sql",
    "files.files",
    "catalog.catalogs:read",
    "catalog.schemas:read",
    "catalog.tables:read",
    "catalog.connections",
    "dashboards.genie",
    "sql.warehouses",
    "sql.statement-execution"
  ]
}'

# 4. Deploy latest code to the app
echo "==> Deploying app..."
databricks apps deploy "$APP_NAME" --source-code-path "$SOURCE_CODE_PATH" -t "$TARGET"

echo "==> Done."

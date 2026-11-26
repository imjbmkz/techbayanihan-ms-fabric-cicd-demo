#!/usr/bin/env bash
# Point the Fabric items in this repo at YOUR dev workspace and lakehouse.
#
# The notebooks (default lakehouse metadata) and the semantic model (Direct Lake
# on OneLake source) contain the workspace/lakehouse IDs of the original dev
# environment. After you've created your own dev workspace and lakehouse, run:
#
#   scripts/rebind-fabric-ids.sh <your-dev-workspace-id> <your-dev-lakehouse-id>
#
# then commit and push, and run Source control > Updates > Update all in Fabric.
# Find the IDs in the lakehouse URL:
#   https://app.fabric.microsoft.com/groups/<workspace-id>/lakehouses/<lakehouse-id>
set -euo pipefail

OLD_WORKSPACE_ID="${OLD_WORKSPACE_ID:-e1127f86-76fc-45a2-8c21-350a41e41a8d}"
OLD_LAKEHOUSE_ID="${OLD_LAKEHOUSE_ID:-a05dbecc-1c93-44b9-8c73-8b6bff7026b1}"

if [ $# -ne 2 ]; then
  echo "usage: $0 <new-dev-workspace-id> <new-dev-lakehouse-id>" >&2
  exit 1
fi
NEW_WORKSPACE_ID="$1"
NEW_LAKEHOUSE_ID="$2"
uuid='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
for v in "$NEW_WORKSPACE_ID" "$NEW_LAKEHOUSE_ID"; do
  [[ "$v" =~ $uuid ]] || { echo "not a GUID: $v" >&2; exit 1; }
done

cd "$(dirname "$0")/../fabric"
files=$(grep -rlE "$OLD_WORKSPACE_ID|$OLD_LAKEHOUSE_ID" --include=notebook-content.py --include='*.tmdl' . || true)
if [ -z "$files" ]; then
  echo "Nothing to replace: no file contains the old IDs (already rebound?)."
  exit 0
fi

for f in $files; do
  # perl -pi works the same on macOS and Linux (unlike sed -i)
  perl -pi -e "s/\Q$OLD_WORKSPACE_ID\E/$NEW_WORKSPACE_ID/g; s/\Q$OLD_LAKEHOUSE_ID\E/$NEW_LAKEHOUSE_ID/g" "$f"
  echo "updated fabric/${f#./}"
done
echo
echo "Done. Review with 'git diff', then commit, push, and 'Update all' in the dev workspace."

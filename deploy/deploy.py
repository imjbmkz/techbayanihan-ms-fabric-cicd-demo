"""Deploy Fabric items from this repo to a target environment with fabric-cicd.

    python deploy/deploy.py --environment qa [--run-pipeline] [--dry-run]

What it does
1. Resolves the target workspace and lakehouse IDs *by name* (deploy/environments.json),
   so no environment IDs are hard-coded in the repo.
2. Reads the dev lakehouse/workspace IDs the notebooks are bound to (notebook metadata)
   and generates fabric/parameter.yml so fabric-cicd rebinds every notebook's default
   lakehouse to the target lakehouse.
3. Publishes the in-scope items (Notebook, DataPipeline) and removes orphans of those
   types. Lakehouses are NOT deployed: Terraform owns them.
4. Optionally runs the end-to-end pipeline in the target workspace and waits for it.

Auth: azure-identity. In GitHub Actions `azure/login` (OIDC, service principal) signs in
the Azure CLI, which AzureCliCredential picks up. Locally, `az login` works the same way.
"""
import argparse
import json
import re
import sys
import time
from pathlib import Path

import requests
from azure.identity import AzureCliCredential, ChainedTokenCredential, DefaultAzureCredential

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = Path(__file__).resolve().parent / "environments.json"

META_LAKEHOUSE = re.compile(r'#\s*META\s+"default_lakehouse":\s*"([0-9a-f-]{36})"')
META_LAKEHOUSE_NAME = re.compile(r'#\s*META\s+"default_lakehouse_name":\s*"([^"]+)"')
META_WORKSPACE = re.compile(r'#\s*META\s+"default_lakehouse_workspace_id":\s*"([0-9a-f-]{36})"')


def log(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# Fabric REST helpers
# ---------------------------------------------------------------------------
class FabricApi:
    def __init__(self, credential):
        self.credential = credential

    def _headers(self) -> dict:
        token = self.credential.get_token(FABRIC_SCOPE).token
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def get_all(self, url: str) -> list:
        items, params = [], {}
        while True:
            r = requests.get(url, headers=self._headers(), params=params, timeout=60)
            r.raise_for_status()
            body = r.json()
            items.extend(body.get("value", []))
            token = body.get("continuationToken")
            if not token:
                return items
            params = {"continuationToken": token}

    def workspace_id(self, name: str) -> str:
        matches = [w for w in self.get_all(f"{FABRIC_API}/workspaces") if w["displayName"] == name]
        if not matches:
            raise SystemExit(f"Workspace '{name}' not found, or the deploying identity has no access to it.")
        return matches[0]["id"]

    def item_id(self, workspace_id: str, item_type: str, name: str) -> str:
        items = self.get_all(f"{FABRIC_API}/workspaces/{workspace_id}/items?type={item_type}")
        matches = [i for i in items if i["displayName"] == name]
        if not matches:
            raise SystemExit(f"{item_type} '{name}' not found in workspace {workspace_id}.")
        return matches[0]["id"]

    def run_pipeline(self, workspace_id: str, pipeline_id: str, timeout_min: int) -> str:
        url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
        r = requests.post(url, headers=self._headers(), timeout=60)
        if r.status_code != 202:
            raise SystemExit(f"Could not start pipeline: HTTP {r.status_code} {r.text}")
        status_url = r.headers["Location"]
        deadline = time.time() + timeout_min * 60
        status = "NotStarted"
        while time.time() < deadline:
            time.sleep(30)
            s = requests.get(status_url, headers=self._headers(), timeout=60)
            s.raise_for_status()
            body = s.json()
            status = body.get("status", "Unknown")
            log(f"  pipeline status: {status}")
            if status in ("Completed", "Failed", "Cancelled", "Deduped"):
                if status != "Completed":
                    log(f"  failure reason: {json.dumps(body.get('failureReason'))}")
                return status
        return f"TimedOut (last status: {status})"


# ---------------------------------------------------------------------------
# parameter.yml generation
# ---------------------------------------------------------------------------
def source_bindings(repo_dir: Path) -> dict:
    """Collect the dev lakehouse/workspace IDs and lakehouse names notebooks are bound to."""
    found = {"lakehouse_ids": set(), "workspace_ids": set(), "lakehouse_names": set()}
    for nb in repo_dir.rglob("notebook-content.py"):
        text = nb.read_text(encoding="utf-8")
        found["lakehouse_ids"].update(META_LAKEHOUSE.findall(text))
        found["workspace_ids"].update(META_WORKSPACE.findall(text))
        found["lakehouse_names"].update(META_LAKEHOUSE_NAME.findall(text))
    return found


def build_parameter_yml(env: str, src: dict, target: dict) -> str:
    """fabric-cicd find_replace rules: dev binding -> target binding, for notebooks only."""
    rules = []
    for value in sorted(src["lakehouse_ids"]):
        rules.append((value, target["lakehouse_id"], "dev lakehouse id -> target lakehouse id"))
    for value in sorted(src["workspace_ids"]):
        rules.append((value, target["workspace_id"], "dev workspace id -> target workspace id"))
    for value in sorted(src["lakehouse_names"]):
        rules.append((value, target["lakehouse_name"], "dev lakehouse name -> target lakehouse name"))

    lines = ["# GENERATED by deploy/deploy.py - do not edit or commit", "find_replace:"]
    for find, replace, comment in rules:
        lines += [
            f"  # {comment}",
            f'  - find_value: "{find}"',
            "    replace_value:",
            f'      {env}: "{replace}"',
            '    item_type: "Notebook"',
        ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--environment", "-e", required=True, help="Target environment key in environments.json")
    parser.add_argument("--run-pipeline", action="store_true", help="Run the configured pipeline(s) after deploying")
    parser.add_argument("--pipeline-timeout-min", type=int, default=120)
    parser.add_argument("--dry-run", action="store_true", help="Resolve IDs and write parameter.yml, but don't publish")
    args = parser.parse_args()

    config = json.loads(CONFIG_FILE.read_text())
    env = args.environment.lower()
    if env not in config["environments"]:
        raise SystemExit(f"Unknown environment '{env}'. Known: {sorted(config['environments'])}")
    env_cfg = config["environments"][env]
    repo_dir = REPO_ROOT / config["repository_directory"]

    credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential(exclude_cli_credential=True))
    api = FabricApi(credential)

    log(f"== Target environment: {env}")
    workspace_id = api.workspace_id(env_cfg["workspace_name"])
    lakehouse_id = api.item_id(workspace_id, "Lakehouse", env_cfg["lakehouse_name"])
    log(f"   workspace {env_cfg['workspace_name']} = {workspace_id}")
    log(f"   lakehouse {env_cfg['lakehouse_name']} = {lakehouse_id}")

    src = source_bindings(repo_dir)
    log(f"   notebooks bound to lakehouse(s) {sorted(src['lakehouse_ids'])} "
        f"in workspace(s) {sorted(src['workspace_ids'])}")
    if len(src["lakehouse_ids"]) > 1:
        log("   WARNING: notebooks are bound to more than one lakehouse; all will point to the target lakehouse.")

    param_file = repo_dir / "parameter.yml"
    param_file.write_text(build_parameter_yml(env, src, {
        "workspace_id": workspace_id, "lakehouse_id": lakehouse_id, "lakehouse_name": env_cfg["lakehouse_name"],
    }), encoding="utf-8")
    log(f"   wrote {param_file.relative_to(REPO_ROOT)}")

    if args.dry_run:
        log(param_file.read_text())
        log("== Dry run: nothing published.")
        return 0

    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    log(f"== Publishing {config['item_types_in_scope']} from {config['repository_directory']}/")
    target = FabricWorkspace(
        workspace_id=workspace_id,
        environment=env,
        repository_directory=str(repo_dir),
        item_type_in_scope=config["item_types_in_scope"],
        token_credential=credential,
    )
    publish_all_items(target)
    unpublish_all_orphan_items(target)
    log("== Publish complete")

    if args.run_pipeline:
        for name in env_cfg.get("pipelines_to_run", []):
            pipeline_id = api.item_id(workspace_id, "DataPipeline", name)
            log(f"== Running pipeline {name} ({pipeline_id})")
            status = api.run_pipeline(workspace_id, pipeline_id, args.pipeline_timeout_min)
            log(f"   {name}: {status}")
            if status != "Completed":
                return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

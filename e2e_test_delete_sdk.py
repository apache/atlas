#!/usr/bin/env python3
"""E2E test for DataContract delete using pyatlan SDK on dq-dev FCT_ORDERS."""
import sys
import time
import json
import subprocess

import requests as req
req.packages.urllib3.disable_warnings()

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import DataContract, Table
from pyatlan.model.contract import DataContractSpec
from pyatlan.model.fluent_search import FluentSearch

# --- Config ---
BASE_URL = "https://dq-dev.atlan.com"
ASSET_QN = "default/snowflake/1762364509/ANALYTICS/ATLAN_DEV/AD_SPEND"
ASSET_GUID = "75dd11fb-7a0d-4faf-a8c5-d35cf3ca7c7b"


def get_token():
    """Fetch Keycloak token via kubectl."""
    result = subprocess.run(
        ["kubectl", "-n", "atlas", "exec", "atlas-0", "-c", "atlas-main", "--",
         "curl", "-s", "-X", "POST",
         "http://keycloak-http.keycloak:80/auth/realms/default/protocol/openid-connect/token",
         "-d", "client_id=atlan-backend&client_secret=e751c44a-07d0-87fa-95b2-b501ed7bd81f&grant_type=client_credentials"],
        capture_output=True, text=True
    )
    for line in result.stdout.strip().split('\n'):
        line = line.strip()
        if line.startswith('{'):
            return json.loads(line)["access_token"]
    raise RuntimeError(f"Token fetch failed: {result.stderr}")


# --- Initialize SDK client ---
token = get_token()
client = AtlanClient(base_url=BASE_URL, api_key=token)
HEADERS = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def create_contract(status="DRAFT", description_suffix=""):
    """Create a contract using DataContract.creator()."""
    asset = Table.updater(qualified_name=ASSET_QN, name="FCT_ORDERS")
    spec = client.contracts.generate_initial_spec(asset)

    contract_spec = DataContractSpec.from_yaml(spec)
    contract_spec.description = f"E2E test - {status} {description_suffix}".strip()

    contract = DataContract.creator(
        asset_qualified_name=ASSET_QN,
        contract_spec=contract_spec,
    )

    response = client.asset.save(contract)
    created = response.assets_created(DataContract)
    updated = response.assets_updated(DataContract)

    if created:
        print(f"  Created {status}: {created[0].guid}")
        return created[0].guid
    elif updated:
        print(f"  Updated {status}: {updated[0].guid}")
        return updated[0].guid
    else:
        print(f"  Response: {response}")
        return None


def publish_contract(description_suffix=""):
    """Publish (VERIFIED) the current draft by updating the spec status."""
    asset = Table.updater(qualified_name=ASSET_QN, name="FCT_ORDERS")
    spec = client.contracts.generate_initial_spec(asset)

    contract_spec = DataContractSpec.from_yaml(spec)
    contract_spec.status = "VERIFIED"
    contract_spec.description = f"E2E test - VERIFIED {description_suffix}".strip()

    contract = DataContract.creator(
        asset_qualified_name=ASSET_QN,
        contract_spec=contract_spec,
    )

    response = client.asset.save(contract)
    updated = response.assets_updated(DataContract)
    created = response.assets_created(DataContract)

    if updated:
        print(f"  Published: {updated[0].guid}")
        return updated[0].guid
    elif created:
        print(f"  Published (new): {created[0].guid}")
        return created[0].guid
    else:
        print(f"  Response: {response}")
        return None


def delete_all(guid):
    """Delete all contract versions using SDK."""
    response = client.contracts.delete(guid)
    deleted = response.assets_deleted(DataContract) if hasattr(response, 'assets_deleted') else []
    print(f"  Delete ALL: {len(deleted)} entities deleted")
    return response


def delete_latest(guid):
    """Delete latest version only using SDK."""
    response = client.contracts.delete_latest_version(guid)
    deleted = response.assets_deleted(DataContract) if hasattr(response, 'assets_deleted') else []
    print(f"  Delete LATEST: {len(deleted)} entities deleted")
    return response


def check_asset():
    """Check asset's contract attributes via REST API (relationship attrs need this)."""
    r = req.get(
        f"{BASE_URL}/api/meta/entity/guid/{ASSET_GUID}",
        params={"minExtInfo": "false", "ignoreRelationships": "false"},
        headers=HEADERS, verify=False
    )
    data = r.json()
    entity = data.get("entity", {})
    attrs = entity.get("attributes", {})
    rel_attrs = entity.get("relationshipAttributes", {})

    has_contract = attrs.get("hasContract", False)
    latest = rel_attrs.get("dataContractLatest")
    certified = rel_attrs.get("dataContractLatestCertified")

    latest_guid = latest.get("guid") if latest and isinstance(latest, dict) else None
    certified_guid = certified.get("guid") if certified and isinstance(certified, dict) else None

    print(f"  hasContract: {has_contract}")
    print(f"  dataContractLatest: {latest_guid}")
    print(f"  dataContractLatestCertified: {certified_guid}")
    return has_contract, latest_guid, certified_guid


def wait_es(seconds=3):
    print(f"  (waiting {seconds}s for ES indexing)")
    time.sleep(seconds)


if __name__ == "__main__":
    stage = sys.argv[1] if len(sys.argv) > 1 else "check"

    if stage == "1":
        print("\n=== STAGE 1: Create V1 DRAFT ===")
        create_contract("DRAFT")
        print("\nAsset state:")
        check_asset()

    elif stage == "2":
        print("\n=== STAGE 2: Publish V1 (VERIFIED) ===")
        publish_contract("v1")
        print("\nAsset state:")
        check_asset()

    elif stage == "3":
        print("\n=== STAGE 3: Create V2 DRAFT ===")
        create_contract("DRAFT", "v2")
        print("\nAsset state:")
        check_asset()

    elif stage == "4":
        print("\n=== STAGE 4: Delete latest — BUG FIX TEST ===")
        _, latest_guid, _ = check_asset()
        if latest_guid:
            print(f"\nDeleting latest: {latest_guid}")
            delete_latest(latest_guid)
            wait_es()
            print("\nAsset state after delete:")
            check_asset()
        else:
            print("ERROR: No latest contract found")

    elif stage == "5":
        print("\n=== STAGE 5: Setup multiple versions ===")
        # V1 already exists or create fresh
        print("Creating V1 DRAFT...")
        create_contract("DRAFT", "v1")
        wait_es(2)
        print("Publishing V1...")
        publish_contract("v1")
        wait_es(2)

        for v in range(2, 11):
            print(f"Creating V{v} DRAFT...")
            create_contract("DRAFT", f"v{v}")
            wait_es(2)
            print(f"Publishing V{v}...")
            publish_contract(f"v{v}")
            wait_es(2)

        print("Creating V11 DRAFT...")
        create_contract("DRAFT", "v11")
        wait_es(2)

        print("\nFinal asset state:")
        check_asset()

    elif stage == "6":
        print("\n=== STAGE 6: Delete ALL versions ===")
        _, latest_guid, _ = check_asset()
        if latest_guid:
            print(f"\nDeleting all via: {latest_guid}")
            delete_all(latest_guid)
            wait_es()
            print("\nAsset state after delete-all:")
            check_asset()
        else:
            print("ERROR: No latest contract found")

    elif stage == "delete-latest":
        print("\n=== Delete latest version ===")
        _, latest_guid, _ = check_asset()
        if latest_guid:
            print(f"\nDeleting latest: {latest_guid}")
            delete_latest(latest_guid)
            wait_es()
            print("\nAsset state after delete:")
            check_asset()
        else:
            print("ERROR: No latest contract found")

    elif stage == "delete-all":
        print("\n=== Delete ALL versions ===")
        _, latest_guid, _ = check_asset()
        if latest_guid:
            print(f"\nDeleting all via: {latest_guid}")
            delete_all(latest_guid)
            wait_es()
            print("\nAsset state after delete-all:")
            check_asset()
        else:
            print("ERROR: No latest contract found")

    elif stage == "check":
        print("\n=== Asset state ===")
        check_asset()

    else:
        print(f"Unknown stage: {stage}")
        print("Usage: python e2e_test_delete_sdk.py [1|2|3|4|5|6|check|delete-latest|delete-all]")

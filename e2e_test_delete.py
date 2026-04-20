#!/usr/bin/env python3
"""E2E test for DataContract delete on dq-dev FCT_ORDERS."""
import os, sys, json, subprocess

# Get token from env or fetch fresh
def get_token():
    result = subprocess.run(
        ["kubectl", "-n", "atlas", "exec", "atlas-0", "-c", "atlas-main", "--",
         "curl", "-s", "-X", "POST",
         "http://keycloak-http.keycloak:80/auth/realms/default/protocol/openid-connect/token",
         "-d", "client_id=atlan-backend&client_secret=e751c44a-07d0-87fa-95b2-b501ed7bd81f&grant_type=client_credentials"],
        capture_output=True, text=True
    )
    # Find the JSON in stdout (may have non-JSON prefix lines)
    for line in result.stdout.strip().split('\n'):
        line = line.strip()
        if line.startswith('{'):
            return json.loads(line)["access_token"]
    raise RuntimeError(f"Token fetch failed: {result.stdout} {result.stderr}")

TOKEN = get_token()
BASE_URL = "https://dq-dev.atlan.com"
ASSET_QN = "default/snowflake/1762364509/ANALYTICS/ATLAN_DEV/FCT_ORDERS"
ASSET_GUID = "1071079c-f805-43e9-89a4-0f6ff0986b36"

import requests
requests.packages.urllib3.disable_warnings()

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

def create_contract(status="DRAFT"):
    """Create a contract via bulk entity API."""
    spec = json.dumps({
        "type": "Table",
        "status": status,
        "kind": "DataContract",
        "dataset": "FCT_ORDERS",
        "data_source": "snowflake",
        "description": f"E2E test contract - {status}",
        "columns": [{"name": "ORDER_ID", "data_type": "NUMBER", "description": "Order ID"}]
    })
    payload = {
        "entities": [{
            "typeName": "DataContract",
            "attributes": {
                "dataContractJson": spec,
                "name": "Data contract for FCT_ORDERS",
                "qualifiedName": f"{ASSET_QN}/contract"
            }
        }]
    }
    r = requests.post(f"{BASE_URL}/api/meta/entity/bulk", json=payload, headers=HEADERS, verify=False)
    print(f"Create {status}: {r.status_code}")
    data = r.json()
    if "mutatedEntities" in data:
        created = data["mutatedEntities"].get("CREATE", [])
        updated = data["mutatedEntities"].get("UPDATE", [])
        for e in created:
            if e.get("typeName") == "DataContract":
                print(f"  Contract GUID: {e['guid']}")
                return e["guid"]
        for e in updated:
            if e.get("typeName") == "DataContract":
                print(f"  Contract GUID (updated): {e['guid']}")
                return e["guid"]
    print(f"  Response: {json.dumps(data, indent=2)[:500]}")
    return None

def delete_all(guid):
    """Hard delete all versions."""
    r = requests.delete(
        f"{BASE_URL}/api/meta/entity/bulk",
        params={"deleteType": "HARD", "guid": guid},
        headers=HEADERS, verify=False
    )
    print(f"Delete ALL: {r.status_code}")
    data = r.json()
    deleted = data.get("mutatedEntities", {}).get("DELETE", [])
    print(f"  Deleted {len(deleted)} entities")
    return data

def delete_latest(guid):
    """Hard delete latest version only."""
    h = {**HEADERS, "x-atlan-contract-delete-scope": "single"}
    r = requests.delete(
        f"{BASE_URL}/api/meta/entity/bulk",
        params={"deleteType": "HARD", "guid": guid},
        headers=h, verify=False
    )
    print(f"Delete LATEST: {r.status_code}")
    data = r.json()
    deleted = data.get("mutatedEntities", {}).get("DELETE", [])
    print(f"  Deleted {len(deleted)} entities")
    return data

def check_asset():
    """Check asset's contract attributes."""
    r = requests.get(
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
    import time
    print(f"  (waiting {seconds}s for ES indexing)")
    time.sleep(seconds)

if __name__ == "__main__":
    stage = sys.argv[1] if len(sys.argv) > 1 else "1"

    if stage == "1":
        print("\n=== STAGE 1: Create V1 DRAFT ===")
        guid = create_contract("DRAFT")
        print("\nAsset state:")
        check_asset()

    elif stage == "2":
        print("\n=== STAGE 2: Publish V1 (VERIFIED) ===")
        guid = create_contract("VERIFIED")
        print("\nAsset state:")
        check_asset()

    elif stage == "3":
        print("\n=== STAGE 3: Create V2 DRAFT ===")
        guid = create_contract("DRAFT")
        print("\nAsset state:")
        check_asset()

    elif stage == "4":
        print("\n=== STAGE 4: Delete latest (V2) — BUG FIX TEST ===")
        _, latest_guid, _ = check_asset()
        if latest_guid:
            delete_latest(latest_guid)
            wait_es()
            print("\nAsset state after delete:")
            check_asset()
        else:
            print("ERROR: No latest contract found")

    elif stage == "5":
        print("\n=== STAGE 5: Setup for delete-all (V1 VERIFIED + V2 VERIFIED + V3 DRAFT) ===")
        print("Creating V1 VERIFIED...")
        create_contract("VERIFIED")
        wait_es()
        print("Creating V2 VERIFIED...")
        create_contract("VERIFIED")
        wait_es()
        print("Creating V3 DRAFT...")
        guid = create_contract("DRAFT")
        print("\nAsset state:")
        check_asset()

    elif stage == "6":
        print("\n=== STAGE 6: Delete ALL versions ===")
        _, latest_guid, _ = check_asset()
        if latest_guid:
            delete_all(latest_guid)
            wait_es()
            print("\nAsset state after delete-all:")
            check_asset()
        else:
            print("ERROR: No latest contract found")

    elif stage == "check":
        print("\n=== Asset state ===")
        check_asset()

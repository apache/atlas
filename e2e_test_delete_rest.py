#!/usr/bin/env python3
"""E2E test for DataContract delete via REST (curl) on dq-dev FCT_ORDERS.
Same flow as the SDK-based script: create versions, delete-latest, delete-all."""
import sys
import time
import json
import subprocess
import yaml

BASE_URL = "https://dq-dev.atlan.com"
ASSET_QN = "default/snowflake/1762364509/ANALYTICS/ATLAN_DEV/FCT_ORDERS"
ASSET_GUID = "1071079c-f805-43e9-89a4-0f6ff0986b36"
CONTRACT_TYPE = "DataContract"


def get_token():
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


token = get_token()


def api_call(method, path, params=None, body=None, extra_headers=None):
    url = f"{BASE_URL}{path}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"
    cmd = ["curl", "-sk", "-X", method, url,
           "-H", f"Authorization: Bearer {token}",
           "-H", "Content-Type: application/json"]
    if extra_headers:
        for k, v in extra_headers.items():
            cmd += ["-H", f"{k}: {v}"]
    if body:
        cmd += ["-d", json.dumps(body)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout.strip():
        return json.loads(result.stdout)
    return {}


def create_contract(status="DRAFT", description_suffix=""):
    spec = {
        "kind": "DataContract",
        "status": status,
        "description": f"E2E test - {status} {description_suffix}".strip(),
        "dataset": ASSET_QN,
        "type": "Table",
    }
    spec_str = yaml.dump(spec, default_flow_style=False)
    contract_qn = f"{ASSET_QN}/Table/contract"

    payload = {
        "entities": [{
            "typeName": CONTRACT_TYPE,
            "attributes": {
                "qualifiedName": contract_qn,
                "name": contract_qn,
                "dataContractJson": spec_str,
                "dataContractAssetGuid": ASSET_GUID,
            },
            "relationshipAttributes": {
                "dataContractAsset": {"typeName": "Table", "guid": ASSET_GUID}
            }
        }]
    }

    data = api_call("POST", "/api/meta/entity/bulk", body=payload)
    if "errorCode" in data:
        print(f"  ERROR: {data.get('errorMessage', data)}")
        return None

    created = data.get("mutatedEntities", {}).get("CREATE", [])
    updated = data.get("mutatedEntities", {}).get("UPDATE", [])
    contracts_created = [e for e in created if e.get("typeName") == CONTRACT_TYPE]
    contracts_updated = [e for e in updated if e.get("typeName") == CONTRACT_TYPE]

    if contracts_created:
        guid = contracts_created[0]["guid"]
        print(f"  Created {status}: {guid}")
        return guid
    elif contracts_updated:
        guid = contracts_updated[0]["guid"]
        print(f"  Updated {status}: {guid}")
        return guid
    else:
        print(f"  No contract in response: {json.dumps(data, indent=2)[:300]}")
        return None


def delete_all(guid):
    data = api_call("DELETE", "/api/meta/entity/bulk",
                    params={"deleteType": "PURGE", "guid": guid})
    deleted = data.get("mutatedEntities", {}).get("DELETE", [])
    print(f"  Delete ALL: {len(deleted)} entities deleted")
    for e in deleted:
        print(f"    - {e.get('typeName')}: {e.get('guid')}")
    return data


def delete_latest(guid):
    data = api_call("DELETE", "/api/meta/entity/bulk",
                    params={"deleteType": "PURGE", "guid": guid},
                    extra_headers={"x-atlan-contract-delete-scope": "single"})
    if "errorCode" in data:
        print(f"  ERROR: {data.get('errorMessage', data)}")
        return data
    deleted = data.get("mutatedEntities", {}).get("DELETE", [])
    print(f"  Delete LATEST: {len(deleted)} entities deleted")
    for e in deleted:
        print(f"    - {e.get('typeName')}: {e.get('guid')}")
    return data


def check_asset():
    data = api_call("GET", f"/api/meta/entity/guid/{ASSET_GUID}",
                    params={"minExtInfo": "false", "ignoreRelationships": "false"})
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

    if stage == "5":
        print("\n=== STAGE 5: Setup V1-V10 VERIFIED + V11 DRAFT ===")
        print("Creating V1 DRAFT...")
        create_contract("DRAFT", "v1")
        wait_es(2)
        print("Publishing V1...")
        create_contract("VERIFIED", "v1")
        wait_es(2)

        for v in range(2, 11):
            print(f"Creating V{v} DRAFT...")
            create_contract("DRAFT", f"v{v}")
            wait_es(2)
            print(f"Publishing V{v}...")
            create_contract("VERIFIED", f"v{v}")
            wait_es(2)

        print("Creating V11 DRAFT...")
        create_contract("DRAFT", "v11")
        wait_es(2)

        print("\nFinal asset state:")
        check_asset()

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
        print("Usage: python e2e_test_delete_rest.py [5|check|delete-latest|delete-all]")

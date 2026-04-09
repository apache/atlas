#!/usr/bin/env python3
"""
Standalone script to inspect type hierarchy on staging/preprod.

Fetches typedef details for Catalog, DataSet, Process, and Asset,
then attempts to create a simple Catalog entity.

Usage:
    python3 check_types.py --tenant preprod
    python3 check_types.py --tenant staging
"""

import argparse
import json
import os
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Auth helpers (mirrors core/auth.py logic)
# ---------------------------------------------------------------------------

def load_creds(creds_file, tenant):
    """Load client_secret from creds.yaml for a given tenant."""
    try:
        import yaml
    except ImportError:
        sys.exit("pyyaml required: pip install pyyaml")

    if not os.path.exists(creds_file):
        sys.exit(f"Creds file not found: {creds_file}")

    with open(creds_file, "r") as f:
        creds = yaml.safe_load(f)

    creds_list = creds.get("creds", []) if isinstance(creds, dict) else []
    if isinstance(creds_list, list):
        for entry in creds_list:
            entry_tenant = entry.get("tenant", "")
            if entry_tenant == tenant or entry_tenant == f"{tenant}.atlan.com":
                secret = entry.get("client_secret") or entry.get("secret")
                if secret:
                    return secret

    if isinstance(creds, dict) and tenant in creds:
        entry = creds[tenant]
        if isinstance(entry, dict):
            return entry.get("client_secret") or entry.get("secret")

    available = [e.get("tenant") for e in creds_list] if isinstance(creds_list, list) else []
    sys.exit(f"No credentials for tenant '{tenant}' in {creds_file}. Available: {available}")


def get_access_token(tenant, client_secret):
    """Fetch an OAuth2 access token via client-credentials grant."""
    token_url = (
        f"https://{tenant}.atlan.com"
        f"/auth/realms/default/protocol/openid-connect/token"
    )
    payload = {
        "client_id": "atlan-argo",
        "grant_type": "client_credentials",
        "client_secret": client_secret,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(token_url, headers=headers, data=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()["access_token"]


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def make_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def get_typedef(api_base, headers, type_name):
    """GET /types/typedef/name/{type_name} and return the response."""
    url = f"{api_base}/types/typedef/name/{type_name}"
    resp = requests.get(url, headers=headers, timeout=30)
    return resp


def print_typedef_summary(type_name, resp):
    """Print a summary of a typedef response."""
    print(f"\n{'='*70}")
    print(f"TYPE: {type_name}")
    print(f"{'='*70}")
    print(f"  HTTP Status: {resp.status_code}")

    if resp.status_code != 200:
        print(f"  Error: {resp.text[:500]}")
        return

    data = resp.json()

    # The response may have the typedef nested under entityDefs, classificationDefs, etc.
    # or it may be the typedef directly. Handle both.
    typedef = None
    for key in ("entityDefs", "classificationDefs", "structDefs", "enumDefs",
                "relationshipDefs", "businessMetadataDefs"):
        defs = data.get(key, [])
        if defs:
            typedef = defs[0]
            break

    if typedef is None:
        # Maybe it's the typedef directly
        if "name" in data:
            typedef = data
        else:
            print(f"  Could not parse typedef from response keys: {list(data.keys())}")
            print(f"  Raw (first 1000 chars): {json.dumps(data, indent=2)[:1000]}")
            return

    # Name and supertypes
    print(f"  name: {typedef.get('name')}")
    print(f"  typeVersion: {typedef.get('typeVersion')}")
    print(f"  serviceType: {typedef.get('serviceType')}")
    print(f"  superTypes: {typedef.get('superTypes', [])}")

    # Attribute defs (own attributes, not inherited)
    attr_defs = typedef.get("attributeDefs", [])
    print(f"\n  Attribute Defs ({len(attr_defs)}):")
    for attr in attr_defs:
        print(f"    - {attr.get('name')} (type={attr.get('typeName')}, "
              f"optional={attr.get('isOptional')}, unique={attr.get('isUnique')})")

    # Relationship attribute defs
    rel_attr_defs = typedef.get("relationshipAttributeDefs", [])
    print(f"\n  Relationship Attribute Defs ({len(rel_attr_defs)}):")
    for attr in rel_attr_defs:
        print(f"    - {attr.get('name')} (type={attr.get('typeName')}, "
              f"relationshipTypeName={attr.get('relationshipTypeName')}, "
              f"isLegacyAttribute={attr.get('isLegacyAttribute')})")

    # Sub/super types if available
    sub_types = typedef.get("subTypes", [])
    if sub_types:
        print(f"\n  subTypes: {sub_types}")


def create_catalog_entity(api_base, headers):
    """Try to create a simple Catalog entity and print the result."""
    print(f"\n{'='*70}")
    print("CREATE CATALOG ENTITY (test)")
    print(f"{'='*70}")

    timestamp = int(time.time())
    entity_payload = {
        "entity": {
            "typeName": "Catalog",
            "attributes": {
                "qualifiedName": f"check_types_test_catalog_{timestamp}",
                "name": f"check_types_test_catalog_{timestamp}",
            },
        }
    }

    print(f"  Payload: {json.dumps(entity_payload, indent=2)}")

    url = f"{api_base}/entity"
    resp = requests.post(url, headers=headers, json=entity_payload, timeout=30)

    print(f"  HTTP Status: {resp.status_code}")
    try:
        body = resp.json()
        print(f"  Response Body:\n{json.dumps(body, indent=2)}")
    except Exception:
        print(f"  Response Text: {resp.text[:1000]}")

    # If entity was created, print the GUID so we can clean up later
    if resp.status_code in (200, 201):
        try:
            guid_map = resp.json().get("mutatedEntities", {}).get("CREATE", [])
            if guid_map:
                guid = guid_map[0].get("guid")
                print(f"\n  ** Created entity GUID: {guid} **")
                print(f"  ** To delete: DELETE {api_base}/entity/guid/{guid} **")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Check type hierarchy on staging/preprod")
    parser.add_argument("--tenant", required=True,
                        help="Tenant name (e.g., 'preprod', 'staging')")
    parser.add_argument("--creds-file",
                        default=os.environ.get("TOKEN_FILE", os.path.expanduser("~/creds.yaml")),
                        help="Path to creds.yaml (default: $TOKEN_FILE or ~/creds.yaml)")
    parser.add_argument("--extra-types", nargs="*", default=[],
                        help="Additional type names to inspect")
    args = parser.parse_args()

    # Build API base (staging uses /api/meta)
    api_base = f"https://{args.tenant}.atlan.com/api/meta"

    # Auth
    print(f"Tenant: {args.tenant}")
    print(f"API base: {api_base}")
    print(f"Creds file: {args.creds_file}")
    print("Fetching access token...")

    client_secret = load_creds(args.creds_file, args.tenant)
    token = get_access_token(args.tenant, client_secret)
    print("Token acquired.\n")

    headers = make_headers(token)

    # Fetch typedefs
    type_names = ["Asset", "Catalog", "DataSet", "Process"] + args.extra_types
    for type_name in type_names:
        resp = get_typedef(api_base, headers, type_name)
        print_typedef_summary(type_name, resp)

    # Try creating a Catalog entity
    create_catalog_entity(api_base, headers)

    print(f"\n{'='*70}")
    print("Done.")


if __name__ == "__main__":
    main()

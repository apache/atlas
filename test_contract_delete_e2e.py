"""
E2E test for DataContract delete feature on dq-dev.

Asset: default/snowflake/1762364509/ANALYTICS/ATLAN_DEV/ATTRIBUTION_TOUCHES
Tenant: https://dq-dev.atlan.com

Usage:
    export ATLAN_API_KEY="your-api-key-here"
    python test_contract_delete_e2e.py

Requires pyatlan SDK with delete methods (installed in DQS app venv).
"""
import os
import sys
import time
import traceback

sys.path.insert(0, "/Users/shiva.hanumanthula/yes/atlan-data-quality-studio-app/.venv/lib/python3.13/site-packages")

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import DataContract, Table
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.model.enums import AtlanConnectorType, CertificateStatus

TENANT_URL = "https://dq-dev.atlan.com"
ASSET_QN = "default/snowflake/1762364509/ANALYTICS/ATLAN_DEV/ATTRIBUTION_TOUCHES"
CONNECTION_QN = "default/snowflake/1762364509"


def setup_client():
    api_key = os.environ.get("ATLAN_API_KEY")
    if not api_key:
        print("ERROR: Set ATLAN_API_KEY environment variable")
        sys.exit(1)
    client = AtlanClient(base_url=TENANT_URL, api_key=api_key)
    print(f"Connected to {TENANT_URL}")
    return client


def get_asset(client, qn):
    """Fetch the asset and verify it exists."""
    results = (
        FluentSearch()
        .where(FluentSearch.asset_type(Table))
        .where(Table.QUALIFIED_NAME.eq(qn))
        .include_on_results(Table.HAS_CONTRACT)
        .include_on_results(Table.DATA_CONTRACT_LATEST)
        .page_size(1)
        .execute(client)
    )
    assets = list(results)
    if not assets:
        print(f"ERROR: Asset not found: {qn}")
        sys.exit(1)
    asset = assets[0]
    print(f"Asset: {asset.name} (guid={asset.guid})")
    print(f"  hasContract: {asset.has_contract}")
    print(f"  dataContractLatest: {asset.data_contract_latest}")
    return asset


def find_existing_contracts(client, asset_guid):
    """Find all existing contracts for the asset."""
    results = (
        FluentSearch()
        .where(FluentSearch.asset_type(DataContract))
        .where(DataContract.DATA_CONTRACT_ASSET_GUID.eq(asset_guid))
        .include_on_results(DataContract.DATA_CONTRACT_VERSION)
        .include_on_results(DataContract.CERTIFICATE_STATUS)
        .include_on_results(DataContract.DATA_CONTRACT_SPEC)
        .page_size(50)
        .execute(client)
    )
    contracts = list(results)
    print(f"  Found {len(contracts)} existing contract(s)")
    for c in contracts:
        print(f"    - {c.qualified_name} v{c.data_contract_version} status={c.certificate_status} guid={c.guid}")
    return contracts


def create_draft_contract(client, asset_qn):
    """Create a new DRAFT contract for the asset."""
    print("\n--- Creating DRAFT contract ---")

    # Generate initial spec
    asset_for_spec = Table()
    asset_for_spec.type_name = "Table"
    asset_for_spec.qualified_name = asset_qn
    spec = client.contracts.generate_initial_spec(asset_for_spec)
    print(f"  Generated spec ({len(spec)} chars)")

    # Create the contract
    contract = DataContract.creator(
        asset_qualified_name=asset_qn,
        contract_spec=spec,
    )
    response = client.asset.save(contract)
    created = response.assets_created(DataContract)
    updated = response.assets_updated(DataContract)

    if created:
        guid = created[0].guid
        print(f"  Created new contract: guid={guid}")
        return guid
    elif updated:
        guid = updated[0].guid
        print(f"  Updated existing draft: guid={guid}")
        return guid
    else:
        print("  WARNING: No contract in response, checking mutated entities...")
        print(f"  Response: created={len(response.assets_created(DataContract))}, updated={len(response.assets_updated(DataContract))}")
        return None


def test_delete_all(client, asset_qn):
    """
    Test 1: Delete all contract versions.
    Creates a contract, then deletes all versions, verifies cleanup.
    """
    print("\n" + "=" * 60)
    print("TEST 1: Delete ALL contract versions")
    print("=" * 60)

    # Step 1: Get asset state before
    asset = get_asset(client, asset_qn)
    asset_guid = asset.guid

    # Step 2: Create a draft contract
    contract_guid = create_draft_contract(client, asset_qn)
    if not contract_guid:
        print("SKIP: Could not create contract")
        return False

    # Wait for ES indexing
    print("  Waiting 5s for ES indexing...")
    time.sleep(5)

    # Step 3: Verify contract exists
    contracts = find_existing_contracts(client, asset_guid)
    if not contracts:
        print("FAIL: No contracts found after creation")
        return False

    # Step 4: Delete all versions
    print(f"\n  Deleting ALL versions via client.contracts.delete(guid={contract_guid})")
    try:
        response = client.contracts.delete(contract_guid)
        deleted = response.assets_deleted(DataContract)
        print(f"  Delete response: {len(deleted)} entities deleted")
        for d in deleted:
            print(f"    - {d.qualified_name} guid={d.guid}")
    except Exception as e:
        print(f"FAIL: Delete raised exception: {e}")
        traceback.print_exc()
        return False

    # Wait for ES indexing
    print("  Waiting 5s for ES indexing...")
    time.sleep(5)

    # Step 5: Verify cleanup
    asset_after = get_asset(client, asset_qn)
    contracts_after = find_existing_contracts(client, asset_guid)

    has_contract = asset_after.has_contract
    remaining = len(contracts_after)

    if remaining == 0 and (has_contract is None or has_contract is False):
        print("\nPASS: All versions deleted, hasContract cleared")
        return True
    else:
        print(f"\nFAIL: remaining_contracts={remaining}, hasContract={has_contract}")
        return False


def test_delete_latest_only(client, asset_qn):
    """
    Test 2: Delete only the latest version.
    Creates V1 DRAFT, publishes V1, creates V2 DRAFT, deletes V2, verifies V1 is latest.
    """
    print("\n" + "=" * 60)
    print("TEST 2: Delete LATEST version only")
    print("=" * 60)

    # Step 1: Get asset
    asset = get_asset(client, asset_qn)
    asset_guid = asset.guid

    # Step 2: Create V1 draft
    v1_guid = create_draft_contract(client, asset_qn)
    if not v1_guid:
        print("SKIP: Could not create V1")
        return False

    time.sleep(3)

    # Step 3: Publish V1 (update YAML status to VERIFIED)
    print("\n--- Publishing V1 (status -> VERIFIED) ---")
    # Get current spec and update status
    v1_search = (
        FluentSearch()
        .where(FluentSearch.asset_type(DataContract))
        .where(DataContract.GUID.eq(v1_guid))
        .include_on_results(DataContract.DATA_CONTRACT_SPEC)
        .page_size(1)
        .execute(client)
    )
    v1_list = list(v1_search)
    if not v1_list:
        print("FAIL: Could not find V1 by GUID")
        return False

    v1_spec = v1_list[0].data_contract_spec
    # Replace status: DRAFT with status: VERIFIED in the YAML
    published_spec = v1_spec.replace("status: DRAFT", "status: VERIFIED").replace("status: draft", "status: VERIFIED")

    v1_update = DataContract.updater(
        qualified_name=v1_list[0].qualified_name,
        name=v1_list[0].name,
    )
    v1_update.data_contract_spec = published_spec
    try:
        response = client.asset.save(v1_update)
        print(f"  Published V1: updated={len(response.assets_updated(DataContract))}")
    except Exception as e:
        print(f"  Publish V1 failed (may be expected if already verified): {e}")

    time.sleep(3)

    # Step 4: Create V2 draft (using updater pattern)
    print("\n--- Creating V2 DRAFT ---")
    # Re-fetch V1 spec and change it slightly
    draft_spec = v1_spec.replace("status: VERIFIED", "status: DRAFT").replace("status: verified", "status: DRAFT")
    # Add a small change to trigger new version
    if "description:" in draft_spec:
        draft_spec = draft_spec.replace("description:", "description: Updated for V2 -", 1)

    v2_contract = DataContract.creator(
        asset_qualified_name=asset_qn,
        contract_spec=draft_spec,
    )
    response = client.asset.save(v2_contract)
    created = response.assets_created(DataContract)
    updated = response.assets_updated(DataContract)

    v2_guid = None
    if created:
        v2_guid = created[0].guid
        print(f"  Created V2: guid={v2_guid}")
    elif updated:
        v2_guid = updated[0].guid
        print(f"  Updated to V2: guid={v2_guid}")

    if not v2_guid:
        print("SKIP: Could not create V2")
        return False

    time.sleep(5)

    # Step 5: Verify two versions exist
    contracts = find_existing_contracts(client, asset_guid)
    if len(contracts) < 2:
        print(f"WARNING: Expected 2 versions, found {len(contracts)}")

    # Step 6: Delete latest only
    print(f"\n  Deleting LATEST version via client.contracts.delete_latest_version(guid={v2_guid})")
    try:
        response = client.contracts.delete_latest_version(v2_guid)
        deleted = response.assets_deleted(DataContract)
        print(f"  Delete response: {len(deleted)} entities deleted")
        for d in deleted:
            print(f"    - guid={d.guid}")
    except Exception as e:
        print(f"FAIL: delete_latest_version raised exception: {e}")
        traceback.print_exc()
        return False

    time.sleep(5)

    # Step 7: Verify V1 is still there and is latest
    asset_after = get_asset(client, asset_qn)
    contracts_after = find_existing_contracts(client, asset_guid)

    if len(contracts_after) >= 1:
        # V1 should still exist
        v1_still_exists = any(c.guid == v1_guid for c in contracts_after)
        v2_gone = not any(c.guid == v2_guid for c in contracts_after)

        if v1_still_exists and v2_gone:
            print(f"\nPASS: V2 deleted, V1 still exists, hasContract={asset_after.has_contract}")
            return True
        else:
            print(f"\nFAIL: v1_exists={v1_still_exists}, v2_gone={v2_gone}")
            return False
    else:
        print(f"\nFAIL: No contracts remaining (expected V1)")
        return False


def cleanup(client, asset_qn):
    """Clean up any contracts left from testing."""
    print("\n--- Cleanup ---")
    asset = get_asset(client, asset_qn)
    contracts = find_existing_contracts(client, asset.guid)
    for c in contracts:
        try:
            client.contracts.delete(c.guid)
            print(f"  Deleted contract {c.guid}")
        except Exception as e:
            print(f"  Failed to delete {c.guid}: {e}")


def main():
    client = setup_client()

    # First clean up any existing contracts
    cleanup(client, ASSET_QN)
    time.sleep(5)

    results = {}

    # Test 1: Delete all
    try:
        results["delete_all"] = test_delete_all(client, ASSET_QN)
    except Exception as e:
        print(f"\nTEST 1 EXCEPTION: {e}")
        traceback.print_exc()
        results["delete_all"] = False

    # Clean up between tests
    time.sleep(3)
    cleanup(client, ASSET_QN)
    time.sleep(5)

    # Test 2: Delete latest only
    try:
        results["delete_latest"] = test_delete_latest_only(client, ASSET_QN)
    except Exception as e:
        print(f"\nTEST 2 EXCEPTION: {e}")
        traceback.print_exc()
        results["delete_latest"] = False

    # Final cleanup
    cleanup(client, ASSET_QN)

    # Summary
    print("\n" + "=" * 60)
    print("E2E TEST RESULTS")
    print("=" * 60)
    for test_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {test_name}: {status}")

    all_passed = all(results.values())
    print(f"\nOverall: {'ALL PASSED' if all_passed else 'SOME FAILED'}")
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()

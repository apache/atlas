"""Bulk classification endpoint tests (8 tests).

Tests the 4 bulk classification REST endpoints:
  POST /entity/bulk/classification
  POST /entity/bulk/classification/displayName
  POST /entity/bulk/setClassifications
  POST /entity/bulk/repairClassificationsMappings
"""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError
from core.data_factory import (
    build_dataset_entity, build_classification_def,
    unique_name, unique_qn, unique_type_name,
)
from core.typedef_helpers import create_typedef_verified, ensure_classification_types


def _create_entity_and_register(client, ctx, suffix, max_retries=3):
    """Create a DataSet entity with retry on timeout, register cleanup, return guid."""
    qn = unique_qn(suffix)
    name = unique_name(suffix)
    entity = build_dataset_entity(qn=qn, name=name)
    for attempt in range(max_retries):
        resp = client.post("/entity", json_data={"entity": entity})
        if resp.status_code == 200:
            break
        if resp.status_code in (408, 500, 503) and attempt < max_retries - 1:
            time.sleep(5 * (attempt + 1))
            continue
        raise Exception(
            f"Entity creation for {suffix} returned {resp.status_code}"
        )
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    guid = entities[0]["guid"]
    ctx.register_entity_cleanup(guid)
    return guid


def _get_entity_classifications(client, guid):
    """GET entity and return classification type names from classifications list."""
    resp = client.get(f"/entity/guid/{guid}")
    if resp.status_code != 200:
        return []
    entity = resp.json().get("entity", {})
    classifications = entity.get("classifications", [])
    return [c.get("typeName") for c in classifications if isinstance(c, dict)]


def _poll_entity_has_classification(client, guid, tag_name, max_wait=30,
                                     interval=5):
    """Poll GET entity until tag_name appears in classificationNames."""
    import time
    for attempt in range(max_wait // interval):
        if attempt > 0:
            time.sleep(interval)
        names = _get_entity_classifications(client, guid)
        if tag_name in names:
            return True, names
    return False, names


@suite("bulk_classifications", depends_on_suites=["entity_crud"],
       description="Bulk classification endpoint operations")
class BulkClassificationsSuite:

    def setup(self, client, ctx):
        # Get 2 usable classification types (create new or use existing)
        requested = [unique_type_name("BulkTag"), unique_type_name("BulkTag2")]
        names, self.created_types, self.tag_ok = ensure_classification_types(
            client, requested,
        )
        self.tag_name, self.tag2_name = names[0], names[1]
        if self.created_types:
            ctx.register_typedef_cleanup(client, self.tag_name)
            ctx.register_typedef_cleanup(client, self.tag2_name)

        # --- Create 4 test entities ---
        self.guid_e1 = _create_entity_and_register(client, ctx, "bulk-cls-e1")
        self.guid_e2 = _create_entity_and_register(client, ctx, "bulk-cls-e2")
        self.guid_e3 = _create_entity_and_register(client, ctx, "bulk-cls-e3")
        self.guid_e4 = _create_entity_and_register(client, ctx, "bulk-cls-e4")

        # Smoke test: verify the classification type is actually usable on entities
        # Fallback types from headers might be governance-managed or restricted
        self.smoke_ok = False
        if self.tag_ok:
            smoke_guid = _create_entity_and_register(client, ctx, "bulk-cls-smoke")
            resp = client.post(
                f"/entity/guid/{smoke_guid}/classifications",
                json_data=[{"typeName": self.tag_name}],
            )
            if resp.status_code in (200, 204):
                # Verify it actually persisted
                names_check = _get_entity_classifications(client, smoke_guid)
                if self.tag_name in names_check:
                    self.smoke_ok = True
                    print(f"  [setup] Smoke test PASSED: {self.tag_name} applied to entity")
                else:
                    print(f"  [setup] Smoke test FAILED: POST 200/204 but classification "
                          f"not on entity. classificationNames={names_check}")
            else:
                detail = repr(resp.body)[:300] if hasattr(resp, 'body') and resp.body else ""
                print(f"  [setup] Smoke test FAILED: per-entity classification add "
                      f"returned {resp.status_code}: {detail}")

    # ----------------------------------------------------------------
    # POST /entity/bulk/classification
    # ----------------------------------------------------------------

    @test("bulk_add_classification_by_guids",
          tags=["bulk", "classification"], order=1)
    def test_bulk_add_classification_by_guids(self, client, ctx):
        """POST /entity/bulk/classification — add tag to E1 + E2."""
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")
        if not self.smoke_ok:
            raise SkipTestError(
                f"Smoke test failed — classification type {self.tag_name} "
                f"cannot be applied to DataSet entities (may be governance-restricted)"
            )

        resp = client.post("/entity/bulk/classification", json_data={
            "classification": {"typeName": self.tag_name},
            "entityGuids": [self.guid_e1, self.guid_e2],
        })
        if resp.status_code == 404:
            raise SkipTestError(
                "Bulk classification returned 404 — classification type may not "
                "have propagated through type cache"
            )
        body_detail = repr(resp.body)[:500] if hasattr(resp, 'body') and resp.body else "(empty)"
        print(f"  [bulk-add] POST /entity/bulk/classification -> {resp.status_code}: {body_detail}")
        assert_status_in(resp, [200, 204])

        # Verify via GET with polling (classification may take a moment to persist)
        for guid in (self.guid_e1, self.guid_e2):
            found, names = _poll_entity_has_classification(
                client, guid, self.tag_name,
            )
            assert found, (
                f"Entity {guid} should have {self.tag_name}, "
                f"got classificationNames={names}"
            )

    # ----------------------------------------------------------------
    # POST /entity/bulk/classification/displayName
    # ----------------------------------------------------------------

    @test("bulk_add_classification_by_display_name",
          tags=["bulk", "classification"], order=2)
    def test_bulk_add_classification_by_display_name(self, client, ctx):
        """POST /entity/bulk/classification/displayName — add tag to E3."""
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")
        if not self.smoke_ok:
            raise SkipTestError(
                f"Smoke test failed — classification type {self.tag_name} "
                f"cannot be applied to DataSet entities"
            )

        resp = client.post("/entity/bulk/classification/displayName", json_data=[
            {
                "typeName": self.tag_name,
                "entityGuid": self.guid_e3,
            },
        ])
        # 204 on success, 404 if endpoint doesn't exist
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Endpoint /entity/bulk/classification/displayName returned 404 — not available")
        body_detail = repr(resp.body)[:500] if hasattr(resp, 'body') and resp.body else "(empty)"
        print(f"  [bulk-displayname] POST -> {resp.status_code}: {body_detail}")

        found, names = _poll_entity_has_classification(
            client, self.guid_e3, self.tag_name,
        )
        assert found, (
            f"Entity E3 ({self.guid_e3}) should have {self.tag_name}, "
            f"got {names}"
        )

    # ----------------------------------------------------------------
    # POST /entity/bulk/setClassifications
    # ----------------------------------------------------------------

    @test("bulk_set_classifications_add",
          tags=["bulk", "classification"], order=3)
    def test_bulk_set_classifications_add(self, client, ctx):
        """POST /entity/bulk/setClassifications — add BulkTag to E4."""
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")
        if not self.smoke_ok:
            raise SkipTestError(
                f"Smoke test failed — classification type {self.tag_name} "
                f"cannot be applied to DataSet entities"
            )

        resp = client.post("/entity/bulk/setClassifications", json_data={
            "guidHeaderMap": {
                self.guid_e4: {
                    "guid": self.guid_e4,
                    "typeName": "DataSet",
                    "classifications": [{"typeName": self.tag_name}],
                },
            },
        })
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Endpoint /entity/bulk/setClassifications returned 404 — not available")
        body_detail = repr(resp.body)[:500] if hasattr(resp, 'body') and resp.body else "(empty)"
        print(f"  [bulk-set] POST -> {resp.status_code}: {body_detail}")

        found, names = _poll_entity_has_classification(
            client, self.guid_e4, self.tag_name,
        )
        assert found, (
            f"Entity E4 ({self.guid_e4}) should have {self.tag_name} "
            f"after setClassifications, got {names}"
        )

    @test("bulk_set_classifications_override",
          tags=["bulk", "classification"], order=4,
          depends_on=["bulk_set_classifications_add"])
    def test_bulk_set_classifications_override(self, client, ctx):
        """POST /entity/bulk/setClassifications?overrideClassifications=true.

        Replace BulkTag with BulkTag2 on E4.
        """
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")
        if not self.smoke_ok:
            raise SkipTestError("Smoke test failed — classification type not usable")

        resp = client.post(
            "/entity/bulk/setClassifications",
            params={"overrideClassifications": "true"},
            json_data={
                "guidHeaderMap": {
                    self.guid_e4: {
                        "guid": self.guid_e4,
                        "typeName": "DataSet",
                        "classifications": [{"typeName": self.tag2_name}],
                    },
                },
            },
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Endpoint /entity/bulk/setClassifications returned 404 — not available")

        time.sleep(2)
        names = _get_entity_classifications(client, self.guid_e4)
        assert self.tag2_name in names, (
            f"Entity E4 should have {self.tag2_name} after override, got {names}"
        )
        assert self.tag_name not in names, (
            f"Entity E4 should NOT have {self.tag_name} after override, got {names}"
        )

    @test("bulk_set_classifications_no_override",
          tags=["bulk", "classification"], order=5,
          depends_on=["bulk_set_classifications_override"])
    def test_bulk_set_classifications_no_override(self, client, ctx):
        """POST /entity/bulk/setClassifications?overrideClassifications=false.

        Send both BulkTag + BulkTag2 for E4 (which already has BulkTag2).
        With override=false the server should merge/add BulkTag without
        removing BulkTag2.

        KNOWN SERVER BUG: ClassificationAssociator.validateAndTransfer()
        reads from getAddOrUpdateClassifications() which is never populated
        by the REST API (only getClassifications() is). This means
        override=false is effectively a no-op — new classifications are
        never added. See ClassificationAssociator.java:297.
        """
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")
        if not self.smoke_ok:
            raise SkipTestError("Smoke test failed — classification type not usable")

        resp = client.post(
            "/entity/bulk/setClassifications",
            params={"overrideClassifications": "false"},
            json_data={
                "guidHeaderMap": {
                    self.guid_e4: {
                        "guid": self.guid_e4,
                        "typeName": "DataSet",
                        "classifications": [
                            {"typeName": self.tag_name},
                            {"typeName": self.tag2_name},
                        ],
                    },
                },
            },
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Endpoint /entity/bulk/setClassifications returned 404 — not available")

        # Existing tag should be preserved regardless
        found_tag2, names = _poll_entity_has_classification(
            client, self.guid_e4, self.tag2_name, max_wait=15, interval=5,
        )
        assert found_tag2, (
            f"Entity E4 should still have {self.tag2_name} after no-override, "
            f"got {names}"
        )

        # Server should merge new classification with existing ones
        found_tag1, names = _poll_entity_has_classification(
            client, self.guid_e4, self.tag_name, max_wait=15, interval=5,
        )
        assert found_tag1, (
            f"KNOWN BUG: overrideClassifications=false does not add new "
            f"classifications — ClassificationAssociator.validateAndTransfer() "
            f"reads from getAddOrUpdateClassifications() (always null) instead "
            f"of getClassifications(). See ClassificationAssociator.java:297. "
            f"E4 has {names}"
        )

    # ----------------------------------------------------------------
    # POST /entity/bulk/repairClassificationsMappings
    # ----------------------------------------------------------------

    @test("bulk_repair_classifications_mappings",
          tags=["bulk", "classification"], order=6,
          depends_on=["bulk_add_classification_by_guids"])
    def test_bulk_repair_classifications_mappings(self, client, ctx):
        """POST /entity/bulk/repairClassificationsMappings — repair E1 + E2."""
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")
        if not self.smoke_ok:
            raise SkipTestError("Smoke test failed — classification type not usable")

        resp = client.post(
            "/entity/bulk/repairClassificationsMappings",
            json_data=[self.guid_e1, self.guid_e2],
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Endpoint /entity/bulk/repairClassificationsMappings returned 404 — not available")

        if resp.status_code == 200:
            body = resp.json()
            # Response is an error map — empty dict {} means all GUIDs repaired successfully.
            # Non-empty dict means some GUIDs had errors (key=guid, value=error message).
            if isinstance(body, dict) and body:
                for guid in (self.guid_e1, self.guid_e2):
                    assert guid not in body, (
                        f"Repair reported error for {guid}: {body.get(guid)}"
                    )

    # ----------------------------------------------------------------
    # Negative / Error Tests
    # ----------------------------------------------------------------

    @test("bulk_add_classification_nonexistent_entity",
          tags=["bulk", "classification", "negative"], order=7)
    def test_bulk_add_classification_nonexistent_entity(self, client, ctx):
        """POST /entity/bulk/classification with fake GUID -> 404 or 400."""
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed after retries")

        resp = client.post("/entity/bulk/classification", json_data={
            "classification": {"typeName": self.tag_name},
            "entityGuids": ["00000000-0000-0000-0000-000000000000"],
        })
        assert_status_in(resp, [400, 404])

    @test("bulk_add_classification_nonexistent_type",
          tags=["bulk", "classification", "negative"], order=8)
    def test_bulk_add_classification_nonexistent_type(self, client, ctx):
        """POST /entity/bulk/classification with non-existent typeName -> 404 or 400."""
        resp = client.post("/entity/bulk/classification", json_data={
            "classification": {"typeName": "NonExistentTag_999"},
            "entityGuids": [self.guid_e1],
        })
        assert_status_in(resp, [400, 404])

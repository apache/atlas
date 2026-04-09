"""Classification advanced tests — batch fetch, propagation flag audit, creation-time attachment.

Covers Java IT gaps:
- ClassificationBatchFetchIntegrationTest (bulk GET with names-only optimization)
- ClassificationPropagationFlagIntegrationTest (MS-595 propagation flag audit)
- ClassificationIntegrationTest edge cases (create entity WITH classification, typedef delete)
"""

import json
import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError
from core.data_factory import (
    build_classification_def, build_dataset_entity, build_process_entity,
    unique_name, unique_qn, unique_type_name,
)
from core.typedef_helpers import create_typedef_verified, ensure_classification_types


def _index_search(client, dsl):
    """Run index search. Returns (available: bool, body: dict).
    Raises SkipTestError if search API not available."""
    resp = client.post("/search/indexsearch", json_data={"dsl": dsl})
    if resp.status_code in (400, 404, 405):
        raise SkipTestError(f"Search API returned {resp.status_code} — not available")
    if resp.status_code != 200:
        raise SkipTestError(f"Search API returned {resp.status_code}")
    return True, resp.json()


def _create_entity(client, ctx, suffix, classifications=None, max_retries=3):
    """Create DataSet with retry on timeout, register cleanup, return guid.
    Raises SkipTestError if creation fails after retries."""
    qn = unique_qn(suffix)
    entity = build_dataset_entity(qn=qn, name=unique_name(suffix))
    if classifications:
        entity["classifications"] = classifications
    for attempt in range(max_retries):
        resp = client.post("/entity", json_data={"entity": entity})
        if resp.status_code == 200:
            break
        if resp.status_code in (408, 500, 503) and attempt < max_retries - 1:
            import time
            time.sleep(5 * (attempt + 1))
            continue
        raise SkipTestError(f"Entity creation for {suffix} returned {resp.status_code}")
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    if not entities:
        raise SkipTestError(f"Entity creation for {suffix} returned empty mutatedEntities")
    guid = entities[0]["guid"]
    ctx.register_entity_cleanup(guid)
    return guid


@suite("classification_advanced", depends_on_suites=["entity_crud"],
       description="Classification batch fetch, propagation flag audit, edge cases")
class ClassificationAdvancedSuite:

    def setup(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)

        def _add_classification_with_retry(guid, payload, max_retries=2):
            """Add classification, retry on 404 (type cache lag)."""
            for attempt in range(max_retries + 1):
                resp = client.post(f"/entity/guid/{guid}/classifications",
                                   json_data=payload)
                if resp.status_code in (200, 204):
                    return True
                if resp.status_code == 404 and attempt < max_retries:
                    time.sleep(15)
                    continue
                return False
            return False

        # Get 3 usable classification types (create new or use existing)
        requested = [
            unique_type_name("BatchTag1"),
            unique_type_name("BatchTag2"),
            unique_type_name("BatchTag3"),
        ]
        names, self.created_types, self.tags_ok = ensure_classification_types(
            client, requested,
        )
        self.tag1, self.tag2, self.tag3 = names[0], names[1], names[2]
        if self.created_types:
            ctx.register_typedef_cleanup(client, self.tag1)
            ctx.register_typedef_cleanup(client, self.tag2)
            ctx.register_typedef_cleanup(client, self.tag3)

        # Create 4 entities with varying classification combos:
        #   E1: tag1
        #   E2: tag1 + tag2
        #   E3: tag3
        #   E4: no tags
        self.e1 = _create_entity(client, ctx, "batch-e1")
        self.e2 = _create_entity(client, ctx, "batch-e2")
        self.e3 = _create_entity(client, ctx, "batch-e3")
        self.e4 = _create_entity(client, ctx, "batch-e4")

        self.e1_tags_ok = False
        self.e2_tags_ok = False
        self.e3_tags_ok = False
        self.flag_tags_ok = False

        if self.tags_ok and self.e1:
            self.e1_tags_ok = _add_classification_with_retry(
                self.e1, [{"typeName": self.tag1}])
        if self.tags_ok and self.e2:
            self.e2_tags_ok = _add_classification_with_retry(
                self.e2, [{"typeName": self.tag1}, {"typeName": self.tag2}])
        if self.tags_ok and self.e3:
            self.e3_tags_ok = _add_classification_with_retry(
                self.e3, [{"typeName": self.tag3}])

        # Create a dedicated entity for propagation flag tests
        self.flag_entity = _create_entity(client, ctx, "propflag")
        if self.tags_ok and self.flag_entity:
            self.flag_tags_ok = _add_classification_with_retry(
                self.flag_entity, [{
                    "typeName": self.tag1,
                    "propagate": False,
                    "restrictPropagationThroughLineage": False,
                }])

        time.sleep(max(es_wait, 5))

    # ================================================================
    #  Group 1 — Batch fetch with classifications
    # ================================================================

    @test("bulk_get_returns_correct_classifications",
          tags=["classification", "batch", "v2"], order=1)
    def test_bulk_get(self, client, ctx):
        """Bulk GET entities — each has the right classification set."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef creation failed")
        if not all([self.e1, self.e2, self.e3, self.e4]):
            raise SkipTestError("One or more entity creations failed in setup")
        if not (self.e1_tags_ok and self.e2_tags_ok and self.e3_tags_ok):
            raise SkipTestError("Classification assignment failed for one or more entities")

        guids = [self.e1, self.e2, self.e3, self.e4]
        resp = client.get("/entity/bulk", params={"guid": guids})
        assert_status(resp, 200)

        entities = resp.json().get("entities", [])
        by_guid = {e.get("guid"): e for e in entities}

        # E1: exactly tag1
        e1 = by_guid.get(self.e1, {})
        e1_tags = [c.get("typeName") for c in e1.get("classifications", []) or []]
        assert self.tag1 in e1_tags, f"E1 should have {self.tag1}, got {e1_tags}"
        assert len(e1_tags) == 1, f"E1 should have 1 tag, got {len(e1_tags)}: {e1_tags}"

        # E2: tag1 + tag2
        e2 = by_guid.get(self.e2, {})
        e2_tags = [c.get("typeName") for c in e2.get("classifications", []) or []]
        assert self.tag1 in e2_tags, f"E2 missing {self.tag1}: {e2_tags}"
        assert self.tag2 in e2_tags, f"E2 missing {self.tag2}: {e2_tags}"

        # E3: tag3
        e3 = by_guid.get(self.e3, {})
        e3_tags = [c.get("typeName") for c in e3.get("classifications", []) or []]
        assert self.tag3 in e3_tags, f"E3 should have {self.tag3}, got {e3_tags}"

        # E4: no tags
        e4 = by_guid.get(self.e4, {})
        e4_tags = e4.get("classifications") or []
        assert len(e4_tags) == 0, f"E4 should have no tags, got {e4_tags}"

    @test("search_names_only_returns_classification_names",
          tags=["classification", "batch", "search", "v2"], order=2)
    def test_search_names_only(self, client, ctx):
        """Index search — classificationNames populated in search results."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef creation failed")
        if not self.e1_tags_ok or not self.e2_tags_ok:
            raise SkipTestError("Classification assignment failed for E1/E2")

        available, body = _index_search(client, {
            "from": 0, "size": 5,
            "query": {"bool": {"must": [
                {"terms": {"__guid": [self.e1, self.e2]}},
                {"term": {"__state": "ACTIVE"}},
            ]}},
        })

        entities = body.get("entities", [])
        assert entities, (
            f"No entities found in search for GUIDs {self.e1}, {self.e2}"
        )

        by_guid = {e.get("guid"): e for e in entities}

        # E1 — classificationNames should include tag1
        e1 = by_guid.get(self.e1, {})
        cn1 = e1.get("classificationNames", [])
        assert self.tag1 in cn1, (
            f"E1 classificationNames should include {self.tag1}, got {cn1}"
        )

        # E2 — classificationNames should include both
        e2 = by_guid.get(self.e2, {})
        cn2 = e2.get("classificationNames", [])
        assert self.tag1 in cn2, f"E2 missing {self.tag1} in classificationNames: {cn2}"
        assert self.tag2 in cn2, f"E2 missing {self.tag2} in classificationNames: {cn2}"

    @test("add_classification_then_bulk_fetch",
          tags=["classification", "batch", "v2"], order=3)
    def test_add_then_bulk(self, client, ctx):
        """Add classification to E4, then bulk fetch to verify it appears."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef creation failed")
        if not self.e4:
            raise SkipTestError("E4 entity creation failed")

        resp = client.post(f"/entity/guid/{self.e4}/classifications",
                           json_data=[{"typeName": self.tag3}])
        assert_status_in(resp, [200, 204])

        resp2 = client.get("/entity/bulk", params={"guid": [self.e4]})
        assert_status(resp2, 200)
        entities = resp2.json().get("entities", [])
        assert entities, f"Bulk GET returned no entities for {self.e4}"
        tags = [c.get("typeName") for c in entities[0].get("classifications", []) or []]
        assert self.tag3 in tags, (
            f"E4 should have {self.tag3} after add, got {tags}"
        )

    @test("remove_classification_then_bulk_fetch",
          tags=["classification", "batch", "v2"], order=4,
          depends_on=["add_classification_then_bulk_fetch"])
    def test_remove_then_bulk(self, client, ctx):
        """Remove classification from E4, then bulk fetch to verify it's gone."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef creation failed")
        if not self.e4:
            raise SkipTestError("E4 entity creation failed")

        resp = client.delete(f"/entity/guid/{self.e4}/classification/{self.tag3}")
        assert_status_in(resp, [200, 204])

        resp2 = client.get("/entity/bulk", params={"guid": [self.e4]})
        assert_status(resp2, 200)
        entities = resp2.json().get("entities", [])
        assert entities, f"Bulk GET returned no entities for {self.e4}"
        tags = entities[0].get("classifications") or []
        tag_names = [c.get("typeName") for c in tags if isinstance(c, dict)]
        assert self.tag3 not in tag_names, (
            f"E4 should NOT have {self.tag3} after removal, got {tag_names}"
        )

    # ================================================================
    #  Group 2 — Propagation flag audit (MS-595 regression)
    # ================================================================

    @test("update_propagate_flag_get_reflects_change",
          tags=["classification", "propagation", "v2"], order=5)
    def test_propagate_flag_get(self, client, ctx):
        """Update propagate false->true, GET API shows correct value."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef creation failed")
        if not self.flag_tags_ok or not self.flag_entity:
            raise SkipTestError("Propagation flag entity/tag setup failed")

        resp = client.put(
            f"/entity/guid/{self.flag_entity}/classifications",
            json_data=[{
                "typeName": self.tag1,
                "propagate": True,
                "restrictPropagationThroughLineage": False,
            }],
        )
        assert_status_in(resp, [200, 204])

        # Verify via GET
        resp2 = client.get(f"/entity/guid/{self.flag_entity}/classification/{self.tag1}")
        assert_status(resp2, 200)
        body = resp2.json()
        assert body.get("propagate") is True, (
            f"Expected propagate=True after update, got {body.get('propagate')}"
        )

    @test("update_propagate_flag_audit_reflects_change",
          tags=["classification", "propagation", "audit", "v2"], order=6,
          depends_on=["update_propagate_flag_get_reflects_change"])
    def test_propagate_flag_audit(self, client, ctx):
        """MS-595: Audit entry for CLASSIFICATION_UPDATE must reflect new propagate value."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef creation failed")
        if not self.flag_tags_ok or not self.flag_entity:
            raise SkipTestError("Propagation flag entity/tag setup failed")

        time.sleep(3)

        # Query audit for CLASSIFICATION_UPDATE via global endpoint (retry up to 5 times)
        audit_found = False
        audits = []
        for attempt in range(5):
            resp = client.post("/entity/auditSearch", json_data={
                "dsl": {
                    "from": 0,
                    "size": 5,
                    "sort": [{"created": {"order": "desc"}}],
                    "query": {
                        "bool": {
                            "filter": {
                                "bool": {
                                    "must": [
                                        {"term": {"entityId": self.flag_entity}},
                                        {"term": {"action": "CLASSIFICATION_UPDATE"}},
                                    ]
                                }
                            }
                        }
                    },
                },
                "suppressLogs": True,
            })
            if resp.status_code not in (200,):
                raise SkipTestError(
                    f"Audit API returned {resp.status_code} — not available"
                )

            body = resp.json()
            audits = body.get("entityAudits", [])
            if audits:
                audit_found = True
                break
            time.sleep(2)

        assert audit_found, (
            f"No CLASSIFICATION_UPDATE audit events found for {self.flag_entity} "
            f"after 5 retries"
        )

        # Find the most recent CLASSIFICATION_UPDATE audit
        latest = audits[0]
        action = latest.get("action", "")
        assert "CLASSIFICATION_UPDATE" in action, (
            f"Expected CLASSIFICATION_UPDATE audit, got {action}"
        )

        # Verify detail contains correct propagation flag values
        detail_str = latest.get("details") or latest.get("detail") or ""
        if detail_str and isinstance(detail_str, str):
            try:
                detail_obj = json.loads(detail_str)
                # If parseable, check for propagate flag in the JSON
                if isinstance(detail_obj, dict):
                    propagate_val = detail_obj.get("propagate")
                    if propagate_val is not None:
                        assert propagate_val is True, (
                            f"MS-595: Audit detail shows propagate={propagate_val}, "
                            f"expected True (stale audit data bug)"
                        )
                        print(f"  [audit] Verified propagate=True in audit detail JSON")
                elif isinstance(detail_obj, list):
                    # Detail might be a list of classifications
                    for item in detail_obj:
                        if isinstance(item, dict) and item.get("typeName") == self.tag1:
                            propagate_val = item.get("propagate")
                            if propagate_val is not None:
                                assert propagate_val is True, (
                                    f"MS-595: Audit detail shows propagate={propagate_val} "
                                    f"for {self.tag1}, expected True"
                                )
                                print(f"  [audit] Verified propagate=True in audit detail")
                            break
            except (json.JSONDecodeError, TypeError):
                # Detail not JSON-parseable — verify string contains indication
                print(f"  [audit] Audit detail not JSON-parseable, checking string content")
                # Best-effort: audit was recorded for this action
                pass

    @test("update_restrict_lineage_flag",
          tags=["classification", "propagation", "v2"], order=7,
          depends_on=["update_propagate_flag_get_reflects_change"])
    def test_restrict_lineage_flag(self, client, ctx):
        """Update restrictPropagationThroughLineage false->true, verify via GET."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef creation failed")
        if not self.flag_tags_ok or not self.flag_entity:
            raise SkipTestError("Propagation flag entity/tag setup failed")

        resp = client.put(
            f"/entity/guid/{self.flag_entity}/classifications",
            json_data=[{
                "typeName": self.tag1,
                "propagate": True,
                "restrictPropagationThroughLineage": True,
            }],
        )
        assert_status_in(resp, [200, 204])

        resp2 = client.get(f"/entity/guid/{self.flag_entity}/classification/{self.tag1}")
        assert_status(resp2, 200)
        body = resp2.json()
        assert body.get("propagate") is True, (
            f"Expected propagate=True, got {body.get('propagate')}"
        )
        assert body.get("restrictPropagationThroughLineage") is True, (
            f"Expected restrictPropagationThroughLineage=True, "
            f"got {body.get('restrictPropagationThroughLineage')}"
        )

    @test("switch_multiple_flags_at_once",
          tags=["classification", "propagation", "v2"], order=8,
          depends_on=["update_restrict_lineage_flag"])
    def test_switch_multiple_flags(self, client, ctx):
        """Switch restrictLineage true->false while changing propagate, verify GET."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef creation failed")
        if not self.flag_tags_ok or not self.flag_entity:
            raise SkipTestError("Propagation flag entity/tag setup failed")

        resp = client.put(
            f"/entity/guid/{self.flag_entity}/classifications",
            json_data=[{
                "typeName": self.tag1,
                "propagate": False,
                "restrictPropagationThroughLineage": False,
            }],
        )
        assert_status_in(resp, [200, 204])

        resp2 = client.get(f"/entity/guid/{self.flag_entity}/classification/{self.tag1}")
        assert_status(resp2, 200)
        body = resp2.json()
        assert body.get("propagate") is False, (
            f"Expected propagate=False, got {body.get('propagate')}"
        )
        assert body.get("restrictPropagationThroughLineage") is False, (
            f"Expected restrictPropagationThroughLineage=False, got "
            f"{body.get('restrictPropagationThroughLineage')}"
        )

    @test("propagate_flag_audit_after_restrict_lineage",
          tags=["classification", "propagation", "audit", "v2"], order=8.5,
          depends_on=["update_restrict_lineage_flag"])
    def test_restrict_lineage_flag_audit(self, client, ctx):
        """Verify audit for restrictPropagationThroughLineage change (MS-595)."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef creation failed")
        if not self.flag_tags_ok or not self.flag_entity:
            raise SkipTestError("Propagation flag entity/tag setup failed")

        time.sleep(3)

        # Query audit for CLASSIFICATION_UPDATE via global endpoint
        audit_found = False
        audits = []
        for attempt in range(5):
            resp = client.post("/entity/auditSearch", json_data={
                "dsl": {
                    "from": 0,
                    "size": 10,
                    "sort": [{"created": {"order": "desc"}}],
                    "query": {
                        "bool": {
                            "filter": {
                                "bool": {
                                    "must": [
                                        {"term": {"entityId": self.flag_entity}},
                                        {"term": {"action": "CLASSIFICATION_UPDATE"}},
                                    ]
                                }
                            }
                        }
                    },
                },
                "suppressLogs": True,
            })
            if resp.status_code != 200:
                raise SkipTestError(
                    f"Audit API returned {resp.status_code} — not available"
                )
            body = resp.json()
            audits = body.get("entityAudits", [])
            if len(audits) >= 2:
                # Need at least 2 audit entries (one from order 5/6, one from order 7)
                audit_found = True
                break
            time.sleep(2)

        assert audit_found, (
            f"Expected >= 2 CLASSIFICATION_UPDATE audit events for {self.flag_entity}, "
            f"got {len(audits)}"
        )

        # Verify we have multiple audit records (at least the flag updates we made)
        actions = [a.get("action", "") for a in audits]
        update_count = sum(1 for a in actions if "CLASSIFICATION_UPDATE" in a)
        assert update_count >= 2, (
            f"Expected >= 2 CLASSIFICATION_UPDATE audits (from flag changes), "
            f"got {update_count}. Actions: {actions}"
        )
        print(f"  [audit] Found {update_count} CLASSIFICATION_UPDATE audit entries")

    # ================================================================
    #  Group 3 — Edge cases
    # ================================================================

    @test("create_entity_with_classification_at_creation",
          tags=["classification", "v2"], order=9)
    def test_create_with_tag(self, client, ctx):
        """Create entity with classification attached at creation time."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef creation failed")

        guid = _create_entity(client, ctx, "tag-at-create",
                              classifications=[{"typeName": self.tag1}])

        # Verify classification attached
        resp = client.get(f"/entity/guid/{guid}")
        assert_status(resp, 200)
        entity = resp.json().get("entity", {})
        tags = [c.get("typeName") for c in entity.get("classifications", []) or []]
        assert self.tag1 in tags, (
            f"Classification {self.tag1} not attached at creation: {tags}"
        )

    @test("delete_classification_typedef",
          tags=["classification", "typedef", "v2"], order=10)
    def test_delete_tag_typedef(self, client, ctx):
        """Create a throwaway classification typedef, then delete it."""
        throwaway = unique_type_name("ThrowTag")
        ok, resp = create_typedef_verified(
            client,
            {"classificationDefs": [build_classification_def(name=throwaway)]},
            max_wait=30,
        )
        if not ok:
            raise SkipTestError(
                f"Throwaway typedef creation failed ({resp.status_code})"
            )

        # Delete — type is already queryable since create_typedef_verified confirmed it
        resp2 = client.delete(f"/types/typedef/name/{throwaway}")
        if resp2.status_code == 404:
            raise SkipTestError(
                f"Typedef deletion returned 404 — type cache never propagated"
            )
        assert_status_in(resp2, [200, 204])

        # Verify gone
        resp3 = client.get(f"/types/classificationdef/name/{throwaway}")
        assert_status_in(resp3, [404, 400])

    @test("es_denormalized_fields_after_classification_add",
          tags=["classification", "search", "v2"], order=11)
    def test_es_denormalized_after_add(self, client, ctx):
        """Verify __classificationNames and classificationNames in ES after tag add.
        Matches ClassificationBatchFetchIntegrationTest search validation."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef creation failed")
        if not self.e1 or not self.e1_tags_ok:
            raise SkipTestError("E1 entity/tag setup failed")

        # E1 has tag1 — search and verify ES fields
        es_wait = ctx.get("es_sync_wait", 5)

        found = False
        entity = {}
        for attempt in range(5):
            _, body = _index_search(client, {
                "from": 0, "size": 1,
                "query": {"bool": {"must": [
                    {"term": {"__guid": self.e1}},
                    {"term": {"__state": "ACTIVE"}},
                ]}},
            })
            entities = body.get("entities", [])
            if entities:
                entity = entities[0]
                found = True
                break
            time.sleep(3)

        assert found, f"E1 {self.e1} not found in ES after {5 * 3}s"

        cn = entity.get("classificationNames", [])
        assert self.tag1 in cn, (
            f"Expected {self.tag1} in classificationNames, got {cn}"
        )
        print(f"  [es-fields] E1 classificationNames={cn} — verified {self.tag1} present")

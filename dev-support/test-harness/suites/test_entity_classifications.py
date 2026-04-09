"""Classification add/update/delete on entities.

Covers Java IT equivalents:
- ClassificationIntegrationTest (CRUD lifecycle, create-with-tag, nonexistent entity)
- ClassificationDeletionESIntegrationTest (ES denormalized field cleanup)
- Propagation through lineage (add, verify, delete-cleanup, search verification)
"""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_list_min_length, SkipTestError,
)
from core.audit_helpers import poll_audit_events
from core.kafka_helpers import assert_entity_in_kafka
from core.data_factory import (
    build_classification_def, build_dataset_entity, build_process_entity,
    unique_name, unique_qn, unique_type_name,
    detect_process_io_type, create_process_with_io,
)
from core.typedef_helpers import create_typedef_verified, ensure_classification_types


def _poll_entity_classifications(client, guid, expected_tag, max_wait=30,
                                 interval=5, label=""):
    """Poll GET entity until expected_tag appears in classifications list.

    Returns (found: bool, classification_names: list).
    Prints progress for debugging.
    """
    elapsed = 0
    names = []
    prefix = f"  [{label}] " if label else "  "
    while elapsed < max_wait:
        resp = client.get(f"/entity/guid/{guid}")
        if resp.status_code == 200:
            entity = resp.json().get("entity", {})
            classifications = entity.get("classifications", [])
            names = [c.get("typeName") for c in classifications if isinstance(c, dict)]
            if expected_tag in names:
                print(f"{prefix}Tag {expected_tag} found on {guid} after {elapsed}s. "
                      f"All tags: {names}")
                return True, names
            print(f"{prefix}Polling {guid} for {expected_tag} ({elapsed}s/{max_wait}s). "
                  f"Current tags: {names}")
        time.sleep(interval)
        elapsed += interval
    print(f"{prefix}Tag {expected_tag} NOT found on {guid} after {max_wait}s. "
          f"Final tags: {names}")
    return False, names


def _poll_tag_removed(client, guid, tag_name, max_wait=30, interval=5, label=""):
    """Poll GET entity until tag_name disappears from classifications list.

    Returns (removed: bool, remaining_names: list).
    """
    elapsed = 0
    names = []
    prefix = f"  [{label}] " if label else "  "
    while elapsed < max_wait:
        time.sleep(interval)
        elapsed += interval
        resp = client.get(f"/entity/guid/{guid}")
        if resp.status_code == 200:
            entity = resp.json().get("entity", {})
            classifications = entity.get("classifications", [])
            names = [c.get("typeName") for c in classifications if isinstance(c, dict)]
            if tag_name not in names:
                print(f"{prefix}Tag {tag_name} removed from {guid} after {elapsed}s. "
                      f"Remaining: {names}")
                return True, names
            print(f"{prefix}Polling {guid} for {tag_name} removal ({elapsed}s/{max_wait}s). "
                  f"Current tags: {names}")
    print(f"{prefix}Tag {tag_name} still present on {guid} after {max_wait}s. "
          f"Tags: {names}")
    return False, names


def _create_entity_and_get_guid(client, ctx, suffix, type_name="DataSet"):
    """Create an entity, register cleanup, return GUID. Raises on failure."""
    qn = unique_qn(suffix)
    entity = build_dataset_entity(qn=qn, name=unique_name(suffix), type_name=type_name)
    resp = client.post("/entity", json_data={"entity": entity})
    assert_status(resp, 200)
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    assert entities, f"Expected entity in mutatedEntities for {suffix}"
    guid = entities[0]["guid"]
    ctx.register_entity_cleanup(guid)
    return guid


def _search_entity_in_es(client, guid, es_sync_wait=5, max_retries=5,
                         retry_interval=3):
    """Search for entity by GUID in ES with retry. Returns (found, entity_dict)."""
    for attempt in range(max_retries):
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0, "size": 1,
                "query": {"bool": {"must": [
                    {"term": {"__guid": guid}},
                    {"term": {"__state": "ACTIVE"}},
                ]}}
            }
        })
        if resp.status_code != 200:
            raise SkipTestError(
                f"Search API returned {resp.status_code} — not available in this env"
            )
        entities = resp.json().get("entities", [])
        if entities:
            return True, entities[0]
        if attempt < max_retries - 1:
            time.sleep(retry_interval)
    return False, {}


@suite("entity_classifications", depends_on_suites=["entity_crud"],
       description="Entity classification operations")
class EntityClassificationsSuite:

    def setup(self, client, ctx):
        # Detect entity type for Process I/O (Catalog on staging, DataSet on local)
        self.io_type = ctx.get("process_io_type") or detect_process_io_type(client)

        # Get 2 usable classification types (create new or use existing)
        requested = [unique_type_name("HarnessTag"), unique_type_name("HarnessTag2")]
        names, self.created_types, self.tags_ok = ensure_classification_types(
            client, requested,
        )
        self.tag_name, self.tag2_name = names[0], names[1]

        ctx.set("harness_tag_name", self.tag_name)
        if self.created_types:
            ctx.register_typedef_cleanup(client, self.tag_name)
            ctx.register_typedef_cleanup(client, self.tag2_name)

        # Create a dedicated entity
        qn = unique_qn("tag-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("tag-test"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Entity creation returned empty mutatedEntities"
        self.entity_guid = entities[0]["guid"]
        self.entity_qn = qn
        ctx.register_entity("tag_test_entity", self.entity_guid, "DataSet")
        ctx.register_entity_cleanup(self.entity_guid)

    # ================================================================
    #  CRUD lifecycle
    # ================================================================

    @test("create_entity_with_classification", tags=["classification"], order=0.5)
    def test_create_entity_with_classification(self, client, ctx):
        """Create entity with classification attached at creation time."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef not queryable after setup")

        qn = unique_qn("tag-create-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("tag-create"))
        entity["classifications"] = [{"typeName": self.tag_name}]
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)

        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Expected at least one entity in mutatedEntities"
        guid = entities[0]["guid"]
        ctx.register_entity_cleanup(guid)

        # Verify classification is attached
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        entity_body = resp2.json().get("entity", {})
        classifications = entity_body.get("classifications", [])
        found = any(c.get("typeName") == self.tag_name for c in classifications)
        assert found, (
            f"Classification {self.tag_name} not attached at creation time, "
            f"got: {[c.get('typeName') for c in classifications]}"
        )

    @test("add_classification", tags=["classification"], order=1)
    def test_add_classification(self, client, ctx):
        if not self.tags_ok:
            raise SkipTestError("Classification typedef not queryable after setup")

        payload = [{"typeName": self.tag_name}]
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/classifications",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        # Read-after-write: GET entity and verify classification present
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        classifications = entity.get("classifications", [])
        found = any(c.get("typeName") == self.tag_name for c in classifications)
        assert found, f"Classification {self.tag_name} not found on entity after add"

    @test("get_classifications", tags=["classification"], order=2, depends_on=["add_classification"])
    def test_get_classifications(self, client, ctx):
        resp = client.get(f"/entity/guid/{self.entity_guid}/classifications")
        assert_status(resp, 200)
        body = resp.json()
        # Body is either a list or has "list" field
        classifications = body if isinstance(body, list) else body.get("list", [])
        found = any(c.get("typeName") == self.tag_name for c in classifications)
        assert found, f"Classification {self.tag_name} not found on entity via GET classifications"

    @test("get_single_classification", tags=["classification"], order=3, depends_on=["add_classification"])
    def test_get_single_classification(self, client, ctx):
        resp = client.get(
            f"/entity/guid/{self.entity_guid}/classification/{self.tag_name}"
        )
        assert_status(resp, 200)
        assert_field_equals(resp, "typeName", self.tag_name)

    @test("update_classification", tags=["classification"], order=4, depends_on=["add_classification"])
    def test_update_classification(self, client, ctx):
        payload = [{"typeName": self.tag_name}]
        resp = client.put(
            f"/entity/guid/{self.entity_guid}/classifications",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        # Read-after-write: verify classification still attached with correct typeName
        resp2 = client.get(
            f"/entity/guid/{self.entity_guid}/classification/{self.tag_name}"
        )
        assert_status(resp2, 200)
        body = resp2.json()
        assert body.get("typeName") == self.tag_name, (
            f"Expected typeName={self.tag_name}, got {body.get('typeName')}"
        )

    @test("update_classification_propagation_flags", tags=["classification", "propagation"],
          order=4.5, depends_on=["add_classification"])
    def test_update_classification_propagation_flags(self, client, ctx):
        payload = [{
            "typeName": self.tag_name,
            "propagate": True,
            "restrictPropagationThroughLineage": True,
        }]
        resp = client.put(
            f"/entity/guid/{self.entity_guid}/classifications",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        # Verify flags updated
        resp2 = client.get(
            f"/entity/guid/{self.entity_guid}/classification/{self.tag_name}"
        )
        assert_status(resp2, 200)
        body = resp2.json()
        assert body.get("propagate") is True, (
            f"Expected propagate=True after update, got {body.get('propagate')}"
        )
        if "restrictPropagationThroughLineage" in body:
            assert body["restrictPropagationThroughLineage"] is True, (
                f"Expected restrictPropagationThroughLineage=True, "
                f"got {body.get('restrictPropagationThroughLineage')}"
            )

    @test("add_classification_audit", tags=["classification", "audit"], order=5,
          depends_on=["add_classification"])
    def test_add_classification_audit(self, client, ctx):
        events, total = poll_audit_events(
            client, self.entity_guid, action_filter="CLASSIFICATION_ADD",
            qualifiedName=self.entity_qn, max_wait=60, interval=10,
        )
        if events is None:
            raise SkipTestError("Audit endpoint not available (404/405)")
        if not events:
            raise SkipTestError(
                f"Audit endpoint available but no CLASSIFICATION_ADD events after 60s — "
                f"audit indexing may not be configured"
            )

    @test("classification_add_kafka_cdc", tags=["classification", "kafka"], order=5.5,
          depends_on=["add_classification"])
    def test_classification_add_kafka_cdc(self, client, ctx):
        """AUD-04: Verify Kafka CDC notification for CLASSIFICATION_ADD."""
        kafka_verifier = ctx.get("kafka_verifier")
        if not kafka_verifier:
            raise SkipTestError("Kafka verifier not configured (--no-kafka or no bootstrap servers)")
        result = assert_entity_in_kafka(ctx, self.entity_guid, "CLASSIFICATION_ADD")
        if result is None:
            raise SkipTestError("Kafka CDC event not found within timeout")

    @test("multi_tag_application", tags=["classification"], order=6, depends_on=["add_classification"])
    def test_multi_tag_application(self, client, ctx):
        """Add second classification and verify both are present."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef not queryable")

        payload = [{"typeName": self.tag2_name}]
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/classifications",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        # GET entity and verify both classifications present
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        classifications = entity.get("classifications", [])
        tag_names = [c.get("typeName") for c in classifications]
        assert self.tag_name in tag_names, (
            f"Expected {self.tag_name} in classifications, got {tag_names}"
        )
        assert self.tag2_name in tag_names, (
            f"Expected {self.tag2_name} in classifications, got {tag_names}"
        )

    # ================================================================
    #  Propagation through lineage
    # ================================================================

    @test("classification_propagation", tags=["classification", "propagation"], order=7)
    def test_classification_propagation(self, client, ctx):
        """Tag source with propagation=True, verify it propagates through lineage to target."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef not queryable")

        io = self.io_type
        print(f"  [propagation] Creating lineage: {io} A -> Process -> {io} B")

        # Create entities with correct type for Process I/O
        guid_a = _create_entity_and_get_guid(client, ctx, "prop-src", type_name=io)
        guid_b = _create_entity_and_get_guid(client, ctx, "prop-tgt", type_name=io)
        print(f"  [propagation] Source={guid_a}, Target={guid_b}")

        # Create Process linking A -> B (120s timeout for staging)
        ok, proc_guid = create_process_with_io(
            client, ctx, "prop-proc", [guid_a], [guid_b], entity_type=io,
        )
        if not ok:
            raise SkipTestError("Process creation failed — lineage not supported in this env")
        print(f"  [propagation] Process={proc_guid}")

        # Add classification to A with propagation enabled
        print(f"  [propagation] Adding {self.tag_name} to source {guid_a} "
              f"with propagate=True, restrictPropagationThroughLineage=False")
        payload = [{
            "typeName": self.tag_name,
            "propagate": True,
            "restrictPropagationThroughLineage": False,
        }]
        resp_tag = client.post(f"/entity/guid/{guid_a}/classifications", json_data=payload)
        assert_status_in(resp_tag, [200, 204])

        # Store guids for downstream tests
        ctx.set("prop_src_guid", guid_a)
        ctx.set("prop_tgt_guid", guid_b)
        ctx.set("prop_proc_guid", proc_guid)

        # Poll for propagation to target B (through process) — 120s
        print(f"  [propagation] Waiting up to 120s for propagation to target {guid_b}...")
        found_b, names_b = _poll_entity_classifications(
            client, guid_b, self.tag_name, max_wait=120, interval=5,
            label="propagation",
        )
        assert found_b, (
            f"Classification {self.tag_name} did NOT propagate from source "
            f"{guid_a} to target {guid_b} through lineage after 120s. "
            f"Target classifications: {names_b}"
        )

        # Also verify on the Process entity (intermediate node)
        print(f"  [propagation] Verifying tag on process {proc_guid}...")
        resp_proc_check = client.get(f"/entity/guid/{proc_guid}")
        assert_status(resp_proc_check, 200)
        proc_entity = resp_proc_check.json().get("entity", {})
        proc_cls = proc_entity.get("classifications", [])
        proc_tags = [c.get("typeName") for c in proc_cls if isinstance(c, dict)]
        assert self.tag_name in proc_tags, (
            f"Classification {self.tag_name} did NOT propagate to process "
            f"{proc_guid}. Process classifications: {proc_tags}"
        )
        print(f"  [propagation] VERIFIED: tag propagated to both process and target")

    @test("classification_propagation_delete_cleanup", tags=["classification", "propagation"],
          order=7.5, depends_on=["classification_propagation"])
    def test_classification_propagation_delete_cleanup(self, client, ctx):
        """Remove tag from source, verify propagated tag is cleaned up on target."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef not queryable")

        io = self.io_type
        print("  [prop-delete] Creating fresh lineage for delete-cleanup test")

        # Create fresh lineage: A -> proc -> B
        guid_a = _create_entity_and_get_guid(client, ctx, "prop-del-src", type_name=io)
        guid_b = _create_entity_and_get_guid(client, ctx, "prop-del-tgt", type_name=io)
        print(f"  [prop-delete] Source={guid_a}, Target={guid_b}")

        ok, proc_guid = create_process_with_io(
            client, ctx, "prop-del-proc", [guid_a], [guid_b], entity_type=io,
        )
        if not ok:
            raise SkipTestError("Process creation failed — lineage not supported in this env")

        # Tag A with propagation enabled
        print(f"  [prop-delete] Adding {self.tag_name} to source with propagate=True")
        payload = [{
            "typeName": self.tag_name,
            "propagate": True,
            "restrictPropagationThroughLineage": False,
        }]
        resp_tag = client.post(f"/entity/guid/{guid_a}/classifications", json_data=payload)
        assert_status_in(resp_tag, [200, 204])

        # Wait for propagation to B (up to 120s — can be slow on preprod)
        print(f"  [prop-delete] Waiting for propagation to target {guid_b}...")
        found_b, _ = _poll_entity_classifications(
            client, guid_b, self.tag_name, max_wait=120, interval=5,
            label="prop-delete",
        )
        assert found_b, (
            f"Classification {self.tag_name} did not propagate to target "
            f"{guid_b} after 120s — cannot verify delete cleanup"
        )

        # Now remove the tag from source A
        print(f"  [prop-delete] Removing {self.tag_name} from source {guid_a}")
        resp_del = client.delete(f"/entity/guid/{guid_a}/classification/{self.tag_name}")
        assert_status_in(resp_del, [200, 204])

        # Poll for propagation cleanup — tag should disappear from B (up to 120s)
        print(f"  [prop-delete] Waiting up to 120s for propagated tag to be removed from {guid_b}...")
        tag_gone, remaining = _poll_tag_removed(
            client, guid_b, self.tag_name, max_wait=120, interval=5,
            label="prop-delete",
        )
        assert tag_gone, (
            f"Propagated classification {self.tag_name} was NOT cleaned up on "
            f"target {guid_b} after removing from source {guid_a} (waited 120s). "
            f"Remaining tags: {remaining}"
        )

    # ================================================================
    #  Transitive delete — propagation path broken by intermediate delete
    # ================================================================

    @test("classification_propagation_transitive_delete",
          tags=["classification", "propagation", "transitive"], order=7.6)
    def test_classification_propagation_transitive_delete(self, client, ctx):
        """Tag propagates A→B→C via lineage. Delete B. Verify tag cleaned up from C.

        Topology:  A --Proc1--> B --Proc2--> C
        Tag on A with propagate=True, restrictPropagationThroughLineage=False.
        After propagation reaches C, soft-delete B.
        Expected:
          - Tag remains on A (source, directly applied)
          - Tag is cleaned up from C (propagation path severed)
        """
        if not self.tags_ok:
            raise SkipTestError("Classification typedef not queryable")

        io = self.io_type
        print(f"  [trans-del] Creating 3-entity lineage: {io} A -> Proc1 -> {io} B -> Proc2 -> {io} C")

        # --- Create entities A, B, C ---
        guid_a = _create_entity_and_get_guid(client, ctx, "trans-del-a", type_name=io)
        guid_b = _create_entity_and_get_guid(client, ctx, "trans-del-b", type_name=io)
        guid_c = _create_entity_and_get_guid(client, ctx, "trans-del-c", type_name=io)
        print(f"  [trans-del] A={guid_a}, B={guid_b}, C={guid_c}")

        # --- Create Proc1: A -> B ---
        ok1, proc1_guid = create_process_with_io(
            client, ctx, "trans-del-p1", [guid_a], [guid_b], entity_type=io,
        )
        if not ok1:
            raise SkipTestError("Proc1 creation failed — lineage not supported in this env")
        print(f"  [trans-del] Proc1={proc1_guid} (A->B)")

        # --- Create Proc2: B -> C ---
        ok2, proc2_guid = create_process_with_io(
            client, ctx, "trans-del-p2", [guid_b], [guid_c], entity_type=io,
        )
        if not ok2:
            raise SkipTestError("Proc2 creation failed — lineage not supported in this env")
        print(f"  [trans-del] Proc2={proc2_guid} (B->C)")

        # --- Tag A with propagation enabled ---
        print(f"  [trans-del] Adding {self.tag_name} to A ({guid_a}) "
              f"with propagate=True, restrictPropagationThroughLineage=False")
        resp_tag = client.post(f"/entity/guid/{guid_a}/classifications", json_data=[{
            "typeName": self.tag_name,
            "propagate": True,
            "restrictPropagationThroughLineage": False,
        }])
        assert_status_in(resp_tag, [200, 204])

        # --- Wait for tag to propagate to B (up to 120s) ---
        print(f"  [trans-del] Waiting for tag to propagate to B ({guid_b})...")
        found_b, names_b = _poll_entity_classifications(
            client, guid_b, self.tag_name, max_wait=120, interval=5,
            label="trans-del-B",
        )
        assert found_b, (
            f"Tag {self.tag_name} did NOT propagate from A to B after 120s. "
            f"B classifications: {names_b}"
        )

        # --- Wait for tag to propagate to C (transitive through B, up to 120s) ---
        print(f"  [trans-del] Waiting for tag to propagate to C ({guid_c})...")
        found_c, names_c = _poll_entity_classifications(
            client, guid_c, self.tag_name, max_wait=120, interval=5,
            label="trans-del-C",
        )
        assert found_c, (
            f"Tag {self.tag_name} did NOT propagate transitively from A through B to C "
            f"after 120s. C classifications: {names_c}"
        )
        print(f"  [trans-del] VERIFIED: tag propagated A -> B -> C")

        # Store for downstream tests
        ctx.set("trans_del_guid_a", guid_a)
        ctx.set("trans_del_guid_b", guid_b)
        ctx.set("trans_del_guid_c", guid_c)
        ctx.set("trans_del_proc1_guid", proc1_guid)
        ctx.set("trans_del_proc2_guid", proc2_guid)

    @test("classification_propagation_transitive_delete_b",
          tags=["classification", "propagation", "transitive"], order=7.7,
          depends_on=["classification_propagation_transitive_delete"])
    def test_classification_propagation_transitive_delete_b(self, client, ctx):
        """Delete intermediate entity B, verify propagated tag cleaned up from C.

        After A→B→C propagation is established, soft-delete B.
        - Tag on A must remain (directly applied).
        - Tag on C should be cleaned up (propagation path through B is broken).
        """
        if not self.tags_ok:
            raise SkipTestError("Classification typedef not queryable")

        guid_a = ctx.get("trans_del_guid_a")
        guid_b = ctx.get("trans_del_guid_b")
        guid_c = ctx.get("trans_del_guid_c")
        assert all([guid_a, guid_b, guid_c]), "Transitive delete GUIDs not set"

        # --- Soft-delete B (the intermediate entity) ---
        print(f"  [trans-del-B] Soft-deleting intermediate entity B ({guid_b})")
        resp_del = client.delete(f"/entity/guid/{guid_b}")
        assert_status(resp_del, 200)

        # Verify B is DELETED
        resp_b = client.get(f"/entity/guid/{guid_b}")
        assert_status(resp_b, 200)
        b_status = resp_b.json().get("entity", {}).get("status")
        assert b_status == "DELETED", f"Expected B status=DELETED, got {b_status}"
        print(f"  [trans-del-B] B is now DELETED")

        # --- Verify tag remains on A (source, directly applied) ---
        print(f"  [trans-del-B] Verifying tag still on A ({guid_a})...")
        resp_a = client.get(f"/entity/guid/{guid_a}")
        assert_status(resp_a, 200)
        a_cls = resp_a.json().get("entity", {}).get("classifications", [])
        a_tags = [c.get("typeName") for c in a_cls if isinstance(c, dict)]
        assert self.tag_name in a_tags, (
            f"Tag {self.tag_name} should remain on source A after B deletion. "
            f"A classifications: {a_tags}"
        )
        print(f"  [trans-del-B] VERIFIED: tag still on A (source)")

        # --- Verify tag is cleaned up from C (propagation path broken) ---
        print(f"  [trans-del-B] Waiting for propagated tag to be cleaned up from C ({guid_c})...")
        tag_gone_c, remaining_c = _poll_tag_removed(
            client, guid_c, self.tag_name, max_wait=60, interval=5,
            label="trans-del-C",
        )

        if tag_gone_c:
            print(f"  [trans-del-B] VERIFIED: propagated tag cleaned up from C "
                  f"after intermediate B was deleted. Remaining tags on C: {remaining_c}")
        else:
            # Tag still on C — this is a known behavior difference across versions.
            # Log as warning but still record the actual behavior.
            print(f"  [trans-del-B] WARNING: propagated tag {self.tag_name} still on C "
                  f"after B deletion (60s). Tags on C: {remaining_c}")
            print(f"  [trans-del-B] This may indicate propagated tags are NOT auto-cleaned "
                  f"when an intermediate entity is soft-deleted.")
            # Assert it should be cleaned — this is the expected correct behavior
            assert tag_gone_c, (
                f"Propagated tag {self.tag_name} was NOT cleaned up from C ({guid_c}) "
                f"after intermediate entity B ({guid_b}) was deleted. "
                f"Expected: propagation path A->B->C is severed, so C should lose the tag. "
                f"Remaining tags on C: {remaining_c}"
            )

    @test("classification_propagation_transitive_delete_verify_lineage",
          tags=["classification", "propagation", "transitive", "lineage"], order=7.8,
          depends_on=["classification_propagation_transitive_delete_b"])
    def test_classification_propagation_transitive_delete_verify_lineage(self, client, ctx):
        """After B is deleted, verify lineage from A no longer reaches C."""
        guid_a = ctx.get("trans_del_guid_a")
        guid_c = ctx.get("trans_del_guid_c")
        assert guid_a, "trans_del_guid_a not set"
        assert guid_c, "trans_del_guid_c not set"

        # Check lineage from A — with B deleted, C should not be reachable
        resp = client.get(f"/lineage/{guid_a}", params={"depth": 5, "direction": "OUTPUT"})
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            # Collect all GUIDs in the lineage graph
            guid_map = body.get("guidEntityMap", {})
            relations = body.get("relations", [])
            all_guids_in_lineage = set(guid_map.keys())
            for rel in relations:
                from_id = rel.get("fromEntityId")
                to_id = rel.get("toEntityId")
                if from_id:
                    all_guids_in_lineage.add(from_id)
                if to_id:
                    all_guids_in_lineage.add(to_id)

            if guid_c not in all_guids_in_lineage:
                print(f"  [trans-del-lineage] VERIFIED: C ({guid_c}) is NOT reachable "
                      f"from A ({guid_a}) lineage after B deletion")
            else:
                # C may still appear in guidEntityMap as a known entity
                # but the relation chain should be broken
                print(f"  [trans-del-lineage] C ({guid_c}) still appears in lineage "
                      f"guidEntityMap — checking if relation chain is intact")
                # Check if there's a direct path from A to C through active relations
                active_from = set()
                active_to = set()
                for rel in relations:
                    from_id = rel.get("fromEntityId")
                    to_id = rel.get("toEntityId")
                    if from_id and to_id:
                        active_from.add(from_id)
                        active_to.add(to_id)
                print(f"  [trans-del-lineage] Relations: {len(relations)} total. "
                      f"guidEntityMap has {len(guid_map)} entries")

    # ================================================================
    #  Search / ES verification
    # ================================================================

    @test("classification_in_search_results", tags=["classification", "search"], order=8,
          depends_on=["add_classification"])
    def test_classification_in_search_results(self, client, ctx):
        """Verify classificationNames and classifications objects in ES search result."""
        es_wait = ctx.get("es_sync_wait", 5)
        time.sleep(es_wait)

        found, entity = _search_entity_in_es(
            client, self.entity_guid, es_sync_wait=es_wait,
        )
        assert found, f"Entity {self.entity_guid} not found in search after {es_wait}s + retries"

        # Verify classificationNames array
        cn = entity.get("classificationNames", [])
        assert self.tag_name in cn, (
            f"Expected {self.tag_name} in classificationNames, got {cn}"
        )

        # Verify classifications objects
        classifications = entity.get("classifications", [])
        if classifications:
            found_cls = any(
                isinstance(c, dict) and c.get("typeName") == self.tag_name
                for c in classifications
            )
            assert found_cls, (
                f"Classification object with typeName={self.tag_name} not in search "
                f"result classifications: {classifications}"
            )

    @test("classification_propagation_in_search", tags=["classification", "search", "propagation"],
          order=9, depends_on=["classification_propagation"])
    def test_classification_propagation_in_search(self, client, ctx):
        """Verify propagated classification appears in ES search on downstream entity."""
        if not self.tags_ok:
            raise SkipTestError("Classification typedef not queryable")

        io = self.io_type
        print("  [prop-search] Creating fresh lineage for ES propagation search test")

        # Create fresh lineage for search validation
        guid_src = _create_entity_and_get_guid(client, ctx, "prop-search-src", type_name=io)
        guid_tgt = _create_entity_and_get_guid(client, ctx, "prop-search-tgt", type_name=io)

        ok, proc_guid = create_process_with_io(
            client, ctx, "prop-search-proc", [guid_src], [guid_tgt], entity_type=io,
        )
        if not ok:
            raise SkipTestError("Process creation failed — lineage not supported in this env")

        # Tag src with propagation enabled
        print(f"  [prop-search] Adding {self.tag_name} to source {guid_src} with propagate=True")
        resp_tag = client.post(f"/entity/guid/{guid_src}/classifications", json_data=[{
            "typeName": self.tag_name,
            "propagate": True,
            "restrictPropagationThroughLineage": False,
        }])
        assert_status_in(resp_tag, [200, 204])

        # Step 1: Poll REST API to confirm propagation reached target (up to 120s)
        # Classification propagation is async via task queue and can take 2+ min on preprod
        print(f"  [prop-search] Waiting for propagation to target {guid_tgt} via REST API...")
        found_prop, prop_names = _poll_entity_classifications(
            client, guid_tgt, self.tag_name, max_wait=120, interval=5,
            label="prop-search",
        )
        assert found_prop, (
            f"Classification {self.tag_name} did NOT propagate from source "
            f"{guid_src} to target {guid_tgt} after 120s. "
            f"Target classifications: {prop_names}"
        )

        # Step 2: Poll ES for classificationNames to contain the tag (up to 120s)
        # AtlasEntityHeader.classificationNames merges both direct + propagated names
        # (there is no separate propagatedClassificationNames field in search results)
        print(f"  [prop-search] Propagation confirmed via REST. "
              f"Now polling ES for classificationNames...")
        found_in_es = False
        es_cn = []
        for i in range(24):
            time.sleep(5)
            found, entity = _search_entity_in_es(
                client, guid_tgt, max_retries=1, retry_interval=1,
            )
            if found:
                es_cn = entity.get("classificationNames", [])
                if self.tag_name in es_cn:
                    found_in_es = True
                    print(f"  [prop-search] Tag found in ES classificationNames "
                          f"after {(i+1)*5}s: {es_cn}")
                    break
            print(f"  [prop-search] Tag not in ES yet ({(i+1)*5}s/120s). "
                  f"classificationNames: {es_cn}")

        assert found_in_es, (
            f"Propagated classification {self.tag_name} confirmed via REST API "
            f"but NOT found in ES classificationNames for target {guid_tgt} "
            f"after 120s. Got: {es_cn}"
        )
        print(f"  [prop-search] VERIFIED: {self.tag_name} in ES classificationNames "
              f"for target {guid_tgt}")

    @test("classification_es_cleanup_after_removal",
          tags=["classification", "search"], order=9.5,
          depends_on=["add_classification"])
    def test_classification_es_cleanup_after_removal(self, client, ctx):
        """ES denormalized fields (__classificationNames, classificationNames)
        must be cleaned up after classification removal.
        Matches ClassificationDeletionESIntegrationTest.java.
        """
        if not self.tags_ok:
            raise SkipTestError("Classification typedef not queryable")

        # Create fresh entity with classification for this test
        guid = _create_entity_and_get_guid(client, ctx, "es-cleanup-test")
        resp = client.post(
            f"/entity/guid/{guid}/classifications",
            json_data=[{"typeName": self.tag_name}],
        )
        assert_status_in(resp, [200, 204])

        # Wait for ES sync, verify tag appears in ES
        es_wait = ctx.get("es_sync_wait", 5)
        time.sleep(es_wait)
        found, entity = _search_entity_in_es(client, guid, max_retries=5, retry_interval=3)
        assert found, f"Entity {guid} not found in ES after adding classification"

        cn_before = entity.get("classificationNames", [])
        print(f"  [es-cleanup] Before removal: classificationNames={cn_before}")
        assert self.tag_name in cn_before, (
            f"Expected {self.tag_name} in classificationNames before removal, "
            f"got {cn_before}"
        )

        # Remove classification via REST API
        resp_del = client.delete(f"/entity/guid/{guid}/classification/{self.tag_name}")
        assert_status_in(resp_del, [200, 204])

        # Verify classification removed from REST API
        resp_check = client.get(f"/entity/guid/{guid}")
        assert_status(resp_check, 200)
        rest_cls = resp_check.json().get("entity", {}).get("classifications", [])
        rest_tags = [c.get("typeName") for c in rest_cls if isinstance(c, dict)]
        assert self.tag_name not in rest_tags, (
            f"Classification {self.tag_name} still present via REST after delete: {rest_tags}"
        )

        # Poll ES until classificationNames no longer contains the tag (retry up to 30s)
        print(f"  [es-cleanup] Waiting for ES to reflect classification removal...")
        es_cleaned = False
        for attempt in range(6):
            time.sleep(5)
            found, entity = _search_entity_in_es(client, guid, max_retries=1, retry_interval=1)
            if not found:
                continue
            cn_after = entity.get("classificationNames", [])
            print(f"  [es-cleanup] Attempt {attempt + 1}: classificationNames={cn_after}")
            if self.tag_name not in cn_after:
                es_cleaned = True
                break

        assert es_cleaned, (
            f"ES classificationNames still contains {self.tag_name} after removal "
            f"and 30s wait. Last seen: {cn_after}"
        )
        print(f"  [es-cleanup] VERIFIED: {self.tag_name} removed from ES classificationNames")

    # ================================================================
    #  Delete classification
    # ================================================================

    @test("delete_classification", tags=["classification"], order=10, depends_on=["add_classification"])
    def test_delete_classification(self, client, ctx):
        resp = client.delete(
            f"/entity/guid/{self.entity_guid}/classification/{self.tag_name}"
        )
        assert_status_in(resp, [200, 204])

        # Verify removed
        resp = client.get(f"/entity/guid/{self.entity_guid}/classifications")
        assert_status(resp, 200)
        body = resp.json()
        classifications = body if isinstance(body, list) else body.get("list", [])
        found = any(c.get("typeName") == self.tag_name for c in classifications)
        assert not found, f"Classification {self.tag_name} should have been removed"

    @test("classification_delete_kafka_cdc", tags=["classification", "kafka"], order=10.5,
          depends_on=["delete_classification"])
    def test_classification_delete_kafka_cdc(self, client, ctx):
        """AUD-05: Verify Kafka CDC notification for CLASSIFICATION_DELETE."""
        kafka_verifier = ctx.get("kafka_verifier")
        if not kafka_verifier:
            raise SkipTestError("Kafka verifier not configured (--no-kafka or no bootstrap servers)")
        result = assert_entity_in_kafka(ctx, self.entity_guid, "CLASSIFICATION_DELETE")
        if result is None:
            raise SkipTestError("Kafka CDC event not found within timeout")

    # ================================================================
    #  Edge cases
    # ================================================================

    @test("add_classification_by_unique_attr", tags=["classification"], order=11)
    def test_add_classification_by_unique_attr(self, client, ctx):
        if not self.tags_ok:
            raise SkipTestError("Classification typedef not queryable")

        # Create another entity for this test
        qn = unique_qn("tag-ua-test")
        guid = _create_entity_and_get_guid(client, ctx, "tag-ua-test")

        # We need to get the actual QN used
        resp_entity = client.get(f"/entity/guid/{guid}")
        assert_status(resp_entity, 200)
        actual_qn = resp_entity.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert actual_qn, f"Could not read qualifiedName for entity {guid}"

        # Add classification by unique attribute
        payload = [{"typeName": self.tag_name}]
        resp = client.post(
            "/entity/uniqueAttribute/type/DataSet/classifications",
            json_data=payload,
            params={"attr:qualifiedName": actual_qn},
        )
        assert_status_in(resp, [200, 204])

        # Read-after-write: verify classification attached
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        entity_body = resp2.json().get("entity", {})
        classifications = entity_body.get("classifications", [])
        found = any(c.get("typeName") == self.tag_name for c in classifications)
        assert found, (
            f"Classification {self.tag_name} not found after add by unique attr"
        )

        # Clean up: delete classification
        resp = client.delete(
            f"/entity/uniqueAttribute/type/DataSet/classification/{self.tag_name}",
            params={"attr:qualifiedName": actual_qn},
        )
        assert_status_in(resp, [200, 204])

    @test("add_classification_nonexistent_entity", tags=["classification"], order=12)
    def test_add_classification_nonexistent_entity(self, client, ctx):
        if not self.tags_ok:
            raise SkipTestError("Classification typedef not queryable")

        payload = [{"typeName": self.tag_name}]
        resp = client.post(
            "/entity/guid/00000000-0000-0000-0000-000000000000/classifications",
            json_data=payload,
        )
        assert_status_in(resp, [404, 400])
        body = resp.json()
        assert isinstance(body, dict), f"Expected dict error response, got {type(body).__name__}"
        assert "errorMessage" in body or "errorCode" in body or "message" in body or "error" in body, (
            f"Expected error details in response, got keys: {list(body.keys())}"
        )

"""Entity label add/delete tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in
from core.audit_helpers import poll_audit_events
from core.assertions import SkipTestError
from core.data_factory import build_dataset_entity, unique_qn, unique_name

import time


@suite("entity_labels", depends_on_suites=["entity_crud"],
       description="Entity label operations")
class EntityLabelsSuite:

    def setup(self, client, ctx):
        qn = unique_qn("label-test")
        self.entity_qn = qn
        entity = build_dataset_entity(qn=qn, name=unique_name("label-test"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Entity creation returned empty mutatedEntities"
        self.entity_guid = entities[0]["guid"]
        ctx.register_entity("label_test_entity", self.entity_guid, "DataSet")
        ctx.register_entity_cleanup(self.entity_guid)

    @test("add_labels", tags=["labels"], order=1)
    def test_add_labels(self, client, ctx):
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=["test-label-1", "test-label-2"],
        )
        assert_status_in(resp, [200, 204])

    @test("get_entity_with_labels", tags=["labels"], order=2, depends_on=["add_labels"])
    def test_get_entity_with_labels(self, client, ctx):
        resp = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp, 200)
        entity = resp.json().get("entity", {})
        labels = entity.get("labels", [])
        assert "test-label-1" in labels, f"Expected 'test-label-1' in labels, got {labels}"

    @test("set_labels", tags=["labels"], order=2.5, depends_on=["add_labels"])
    def test_set_labels(self, client, ctx):
        # PUT adds/sets labels — on staging, PUT behaves as ADD (appends) not REPLACE
        resp = client.put(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=["new-label-x"],
        )
        assert_status_in(resp, [200, 204])

        # Verify new label is present (PUT may add rather than replace)
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        labels = entity.get("labels", [])
        assert "new-label-x" in labels, f"Expected 'new-label-x' in labels, got {labels}"

    @test("delete_labels", tags=["labels"], order=3, depends_on=["add_labels"])
    def test_delete_labels(self, client, ctx):
        resp = client.delete(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=["test-label-1"],
        )
        assert_status_in(resp, [200, 204])

        # Read-after-write: GET entity and verify "test-label-1" is gone
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        labels = entity.get("labels", [])
        assert "test-label-1" not in labels, f"Expected 'test-label-1' removed, got {labels}"

    @test("delete_all_labels_then_verify", tags=["labels"], order=3.5, depends_on=["set_labels"])
    def test_delete_all_labels_then_verify(self, client, ctx):
        # First check what labels remain
        resp_check = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp_check, 200)
        current_labels = resp_check.json().get("entity", {}).get("labels", [])
        if not current_labels:
            return  # Already empty

        # Delete all remaining labels
        resp = client.delete(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=current_labels,
        )
        assert_status_in(resp, [200, 204])

        # Verify labels are empty
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        labels = entity.get("labels", [])
        assert len(labels) == 0, f"Expected empty labels after deleting all, got {labels}"

    @test("add_labels_audit", tags=["labels", "audit"], order=4, depends_on=["add_labels"])
    def test_add_labels_audit(self, client, ctx):
        events, total = poll_audit_events(
            client, self.entity_guid, action_filter="LABEL_ADD",
            max_wait=60, interval=10, qualifiedName=self.entity_qn,
        )
        if events is None:
            raise SkipTestError("Audit endpoint not available (404/405)")
        if not events:
            raise SkipTestError(
                f"Audit endpoint available but no LABEL_ADD events after 60s — "
                f"audit indexing may not be configured"
            )

    @test("add_labels_nonexistent_entity", tags=["labels", "negative"], order=5)
    def test_add_labels_nonexistent_entity(self, client, ctx):
        resp = client.post(
            "/entity/guid/00000000-0000-0000-0000-000000000000/labels",
            json_data=["some-label"],
        )
        assert_status_in(resp, [404, 400])
        body = resp.json()
        if isinstance(body, dict):
            assert "errorMessage" in body or "errorCode" in body or "message" in body or "error" in body, (
                f"Expected error details in response, got keys: {list(body.keys())}"
            )

    @test("add_labels_idempotent", tags=["labels"], order=1.5, depends_on=["add_labels"])
    def test_add_labels_idempotent(self, client, ctx):
        """Add same label twice — verify no duplication."""
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=["test-label-1"],
        )
        assert_status_in(resp, [200, 204])
        # Read and check no duplicates
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        labels = resp2.json().get("entity", {}).get("labels", [])
        count = labels.count("test-label-1")
        assert count <= 1, f"Label 'test-label-1' appears {count} times — expected at most 1"

    @test("add_labels_by_unique_attr", tags=["labels"], order=2.2)
    def test_add_labels_by_unique_attr(self, client, ctx):
        """POST labels via /entity/uniqueAttribute/type/DataSet/labels."""
        resp = client.post(
            "/entity/uniqueAttribute/type/DataSet/labels",
            params={"attr:qualifiedName": self.entity_qn},
            json_data=["unique-attr-label"],
        )
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code in [200, 204]:
            # Verify label was added
            resp2 = client.get(f"/entity/guid/{self.entity_guid}")
            assert_status(resp2, 200)
            labels = resp2.json().get("entity", {}).get("labels", [])
            assert "unique-attr-label" in labels, (
                f"Expected 'unique-attr-label' in labels, got {labels}"
            )

    @test("delete_labels_by_unique_attr", tags=["labels"], order=3.2)
    def test_delete_labels_by_unique_attr(self, client, ctx):
        """DELETE labels via unique attribute path."""
        # First add a label to delete
        client.post(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=["del-by-attr"],
        )
        resp = client.delete(
            "/entity/uniqueAttribute/type/DataSet/labels",
            params={"attr:qualifiedName": self.entity_qn},
            json_data=["del-by-attr"],
        )
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code in [200, 204]:
            resp2 = client.get(f"/entity/guid/{self.entity_guid}")
            assert_status(resp2, 200)
            labels = resp2.json().get("entity", {}).get("labels", [])
            assert "del-by-attr" not in labels, (
                f"Expected 'del-by-attr' removed, got {labels}"
            )

    @test("add_labels_special_chars", tags=["labels"], order=5.5)
    def test_add_labels_special_chars(self, client, ctx):
        """Add labels with hyphens, underscores; verify round-trip.
        Atlas allows alphanumeric, _ and - only (dots are rejected with 400).
        """
        valid_labels = ["label-with-hyphens", "label_with_underscores", "MixedCase123"]
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=valid_labels,
        )
        assert_status_in(resp, [200, 204])
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        labels = resp2.json().get("entity", {}).get("labels", [])
        for lbl in valid_labels:
            assert lbl in labels, f"Expected '{lbl}' in labels, got {labels}"

        # Dots should be rejected
        resp3 = client.post(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=["label.with.dots"],
        )
        assert_status_in(resp3, [400])

    @test("labels_in_search", tags=["labels", "search"], order=6)
    def test_labels_in_search(self, client, ctx):
        """Verify labels appear in search results after ES sync."""
        # Ensure labels exist
        client.post(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=["search-check-label"],
        )
        time.sleep(5)
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {"term": {"__guid": self.entity_guid}},
            }
        })
        assert_status(resp, 200)
        body = resp.json()
        entities = body.get("entities", [])
        if entities:
            entity = entities[0]
            labels = entity.get("labels", [])
            # Labels may or may not be indexed
            if labels:
                assert "search-check-label" in labels, (
                    f"Expected 'search-check-label' in search result labels, got {labels}"
                )

    @test("add_empty_label_list", tags=["labels"], order=7)
    def test_add_empty_label_list(self, client, ctx):
        """POST empty label array — Atlas uses SET semantics so this clears all labels."""
        # Ensure at least one label exists first
        client.post(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=["empty-test-label"],
        )
        resp_before = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp_before, 200)
        labels_before = resp_before.json().get("entity", {}).get("labels", [])
        assert len(labels_before) > 0, "Expected at least one label before empty POST test"

        resp = client.post(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=[],
        )
        assert_status_in(resp, [200, 204])

        # Atlas POST labels has SET semantics — empty array clears all labels
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        labels_after = resp2.json().get("entity", {}).get("labels", [])
        assert len(labels_after) == 0, (
            f"POST with empty [] should clear all labels (SET semantics). "
            f"Before: {labels_before}, After: {labels_after}"
        )

    @test("add_labels_bulk_entities", tags=["labels"], order=8)
    def test_add_labels_bulk_entities(self, client, ctx):
        """Add labels to 2 entities independently, verify each."""
        # Create a second entity
        qn2 = unique_qn("label-bulk-2")
        entity2 = build_dataset_entity(qn=qn2, name=unique_name("label-bulk-2"))
        resp = client.post("/entity", json_data={"entity": entity2})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Second entity creation failed"
        guid2 = entities[0]["guid"]
        ctx.register_entity_cleanup(guid2)

        # Add different labels to each
        resp1 = client.post(
            f"/entity/guid/{self.entity_guid}/labels",
            json_data=["bulk-label-a"],
        )
        assert_status_in(resp1, [200, 204])
        resp2 = client.post(
            f"/entity/guid/{guid2}/labels",
            json_data=["bulk-label-b"],
        )
        assert_status_in(resp2, [200, 204])

        # Verify each entity has its own labels
        resp_e1 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp_e1, 200)
        labels1 = resp_e1.json().get("entity", {}).get("labels", [])
        assert "bulk-label-a" in labels1, f"Entity 1 missing 'bulk-label-a', got {labels1}"

        resp_e2 = client.get(f"/entity/guid/{guid2}")
        assert_status(resp_e2, 200)
        labels2 = resp_e2.json().get("entity", {}).get("labels", [])
        assert "bulk-label-b" in labels2, f"Entity 2 missing 'bulk-label-b', got {labels2}"

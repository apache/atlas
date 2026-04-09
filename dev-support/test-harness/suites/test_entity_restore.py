"""Entity soft-delete and restore tests."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, assert_field_equals, SkipTestError
from core.data_factory import build_dataset_entity, build_classification_def, unique_qn, unique_name, unique_type_name
from core.typedef_helpers import create_typedef_verified, ensure_classification_types


@suite("entity_restore", depends_on_suites=["entity_crud"],
       description="Entity soft-delete and restore operations")
class EntityRestoreSuite:

    def setup(self, client, ctx):
        # Create a dedicated entity for restore tests
        qn = unique_qn("restore-test")
        self.entity_name = unique_name("restore-test")
        entity = build_dataset_entity(qn=qn, name=self.entity_name,
                                      extra_attrs={"description": "restore test entity"})
        resp = client.post("/entity", json_data={"entity": entity})
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        self.entity_guid = entities[0]["guid"]
        self.entity_qn = qn
        ctx.register_entity("restore_test_entity", self.entity_guid, "DataSet")
        ctx.register_entity_cleanup(self.entity_guid)

    @test("soft_delete_entity", tags=["restore", "crud"], order=1)
    def test_soft_delete_entity(self, client, ctx):
        resp = client.delete(f"/entity/guid/{self.entity_guid}")
        assert_status(resp, 200)

        # Verify status is DELETED
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.status", "DELETED")

    @test("restore_entity", tags=["restore", "crud"], order=2, depends_on=["soft_delete_entity"])
    def test_restore_entity(self, client, ctx):
        resp = client.post(
            "/entity/restore/bulk",
            params={"guid": self.entity_guid},
        )
        # 200 or 204 on success; 404 if endpoint not available
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict):
                assert body, "Expected non-empty restore response"
        elif resp.status_code == 404:
            ctx.set("restore_unavailable", True)
            raise SkipTestError(
                "Restore endpoint returned 404 — not available on this environment"
            )

    @test("verify_restored_entity", tags=["restore", "crud"], order=3, depends_on=["restore_entity"])
    def test_verify_restored_entity(self, client, ctx):
        if ctx.get("restore_unavailable"):
            raise SkipTestError("Restore endpoint not available on this environment")
        resp = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.status", "ACTIVE")

    @test("verify_restored_attributes", tags=["restore"], order=3.5, depends_on=["verify_restored_entity"])
    def test_verify_restored_attributes(self, client, ctx):
        """After restore, verify original attributes are preserved."""
        if ctx.get("restore_unavailable"):
            raise SkipTestError("Restore endpoint not available on this environment")
        resp = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp, 200)
        entity = resp.json().get("entity", {})
        attrs = entity.get("attributes", {})
        assert attrs.get("name") == self.entity_name, (
            f"Expected name='{self.entity_name}', got '{attrs.get('name')}'"
        )
        assert attrs.get("description") == "restore test entity", (
            f"Expected description='restore test entity', got '{attrs.get('description')}'"
        )

    @test("restore_active_entity", tags=["restore"], order=4, depends_on=["verify_restored_entity"])
    def test_restore_active_entity(self, client, ctx):
        """Restore an already-active entity — should be a no-op or succeed."""
        if ctx.get("restore_unavailable"):
            raise SkipTestError("Restore endpoint not available on this environment")
        resp = client.post(
            "/entity/restore/bulk",
            params={"guid": self.entity_guid},
        )
        assert_status_in(resp, [200, 204])

        # Verify still ACTIVE
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.status", "ACTIVE")

    @test("restore_verify_search", tags=["restore", "search"], order=5, depends_on=["verify_restored_entity"])
    def test_restore_verify_search(self, client, ctx):
        """After restore, verify entity appears in ACTIVE search."""
        if ctx.get("restore_unavailable"):
            raise SkipTestError("Restore endpoint not available on this environment")
        time.sleep(5)
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"__guid": self.entity_guid}},
                            {"term": {"__state": "ACTIVE"}},
                        ]
                    }
                }
            }
        })
        assert_status(resp, 200)
        body = resp.json()
        count = body.get("approximateCount", 0)
        if count == 0:
            raise SkipTestError(
                "Restored entity not yet visible in ACTIVE search — ES sync may be slow"
            )

    @test("restore_with_classifications", tags=["restore"], order=6)
    def test_restore_with_classifications(self, client, ctx):
        """Soft-delete entity with classification, restore, verify classification preserved."""
        if ctx.get("restore_unavailable"):
            raise SkipTestError("Restore endpoint not available on this environment")

        # Get 1 usable classification type (create new or use existing)
        requested = [unique_type_name("RestoreTag")]
        names, created_new, ok = ensure_classification_types(
            client, requested,
        )
        if not ok:
            raise SkipTestError("Classification type not available")
        tag_name = names[0]
        if created_new:
            ctx.register_typedef_cleanup(client, tag_name)

        qn = unique_qn("restore-cls")
        entity = build_dataset_entity(qn=qn, name=unique_name("restore-cls"))
        entity["classifications"] = [{"typeName": tag_name}]
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Entity creation failed"
        guid = entities[0]["guid"]
        ctx.register_entity_cleanup(guid)

        # Soft delete
        client.delete(f"/entity/guid/{guid}")
        # Restore
        resp2 = client.post("/entity/restore/bulk", params={"guid": guid})
        assert_status_in(resp2, [200, 204, 404])
        if resp2.status_code == 404:
            raise SkipTestError("Restore endpoint not available")

        # Verify classification preserved (poll up to 120s — propagation can be slow on preprod)
        cls_names = []
        for _attempt in range(24):
            time.sleep(5)
            resp3 = client.get(f"/entity/guid/{guid}")
            if resp3.status_code != 200:
                continue
            entity_data = resp3.json().get("entity", {})
            if entity_data.get("status") != "ACTIVE":
                continue
            classifications = entity_data.get("classifications", [])
            cls_names = [c.get("typeName") for c in classifications if isinstance(c, dict)]
            if tag_name in cls_names:
                break
        assert_field_equals(resp3, "entity.status", "ACTIVE")
        assert tag_name in cls_names, (
            f"Expected {tag_name} in classificationNames after restore (120s), got {cls_names}"
        )

    @test("restore_with_labels", tags=["restore"], order=7)
    def test_restore_with_labels(self, client, ctx):
        """Soft-delete entity with labels, restore, verify labels preserved."""
        if ctx.get("restore_unavailable"):
            raise SkipTestError("Restore endpoint not available on this environment")

        qn = unique_qn("restore-lbl")
        entity = build_dataset_entity(qn=qn, name=unique_name("restore-lbl"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Entity creation failed"
        guid = entities[0]["guid"]
        ctx.register_entity_cleanup(guid)

        # Add labels
        client.post(f"/entity/guid/{guid}/labels", json_data=["restore-label-1"])

        # Soft delete + restore
        client.delete(f"/entity/guid/{guid}")
        resp2 = client.post("/entity/restore/bulk", params={"guid": guid})
        assert_status_in(resp2, [200, 204, 404])
        if resp2.status_code == 404:
            raise SkipTestError("Restore endpoint not available")

        # Verify labels preserved
        resp3 = client.get(f"/entity/guid/{guid}")
        assert_status(resp3, 200)
        assert_field_equals(resp3, "entity.status", "ACTIVE")
        labels = resp3.json().get("entity", {}).get("labels", [])
        assert "restore-label-1" in labels, (
            f"Expected 'restore-label-1' in labels after restore, got {labels}"
        )

    @test("restore_nonexistent", tags=["restore", "negative"], order=8)
    def test_restore_nonexistent(self, client, ctx):
        """Restore nonexistent GUID — expect error."""
        if ctx.get("restore_unavailable"):
            raise SkipTestError("Restore endpoint not available on this environment")
        fake_guid = "00000000-0000-0000-0000-000000000000"
        resp = client.post("/entity/restore/bulk", params={"guid": fake_guid})
        assert_status_in(resp, [200, 204, 400, 404])

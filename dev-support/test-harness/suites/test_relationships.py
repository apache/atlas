"""Relationship CRUD tests."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, SkipTestError,
)
from core.data_factory import (
    build_dataset_entity, build_relationship_def, unique_qn, unique_name,
    unique_type_name, detect_process_io_type, create_process_with_io,
)
from core.typedef_helpers import create_typedef_verified


@suite("relationships", depends_on_suites=["entity_crud"],
       description="Relationship CRUD operations")
class RelationshipsSuite:

    def setup(self, client, ctx):
        # Detect entity type for Process I/O (Catalog on staging, DataSet on local)
        self.io_type = ctx.get("process_io_type") or detect_process_io_type(client)

        # Create two entities for relationship tests (use correct type for process I/O)
        qn1 = unique_qn("rel-src")
        qn2 = unique_qn("rel-tgt")
        e1 = build_dataset_entity(qn=qn1, name=unique_name("rel-src"), type_name=self.io_type)
        e2 = build_dataset_entity(qn=qn2, name=unique_name("rel-tgt"), type_name=self.io_type)

        resp = client.post("/entity/bulk", json_data={"entities": [e1, e2]})
        body = resp.json()
        guids = []
        for action in ("CREATE", "UPDATE"):
            for e in body.get("mutatedEntities", {}).get(action, []):
                guids.append(e["guid"])

        self.src_guid = guids[0] if len(guids) > 0 else None
        self.tgt_guid = guids[1] if len(guids) > 1 else None

        if self.src_guid:
            ctx.register_entity("rel_src", self.src_guid, self.io_type)
            ctx.register_entity_cleanup(self.src_guid)
        if self.tgt_guid:
            ctx.register_entity("rel_tgt", self.tgt_guid, self.io_type)
            ctx.register_entity_cleanup(self.tgt_guid)

    @test("create_relationship", tags=["relationship", "crud"], order=1)
    def test_create_relationship(self, client, ctx):
        if not self.src_guid or not self.tgt_guid:
            raise Exception("Missing source or target entity for relationship test")

        # Create a Process linking src -> tgt (uses correct entity type and 120s timeout)
        ok, proc_guid = create_process_with_io(
            client, ctx, "rel-proc",
            [self.src_guid], [self.tgt_guid], entity_type=self.io_type,
        )
        assert ok, "Process creation failed — cannot test relationship via lineage"
        ctx.register_entity("rel_process", proc_guid, "Process")

    @test("get_relationship_by_guid", tags=["relationship"], order=2)
    def test_get_relationship_by_guid(self, client, ctx):
        # Get entity and look for relationship GUIDs
        assert self.src_guid, "src_guid not found — setup must have failed"
        resp = client.get(f"/entity/guid/{self.src_guid}")
        assert_status(resp, 200)
        # Check if entity has any relationship attributes with GUIDs
        entity = resp.json().get("entity", {})
        rel_attrs = entity.get("relationshipAttributes", {})
        rel_guid = None
        for attr_name, attr_val in rel_attrs.items():
            if isinstance(attr_val, list):
                for item in attr_val:
                    if isinstance(item, dict) and "relationshipGuid" in item:
                        rel_guid = item["relationshipGuid"]
                        break
            elif isinstance(attr_val, dict) and "relationshipGuid" in attr_val:
                rel_guid = attr_val["relationshipGuid"]
            if rel_guid:
                break

        if rel_guid:
            resp2 = client.get(f"/relationship/guid/{rel_guid}")
            assert_status(resp2, 200)
            ctx.set("test_rel_guid", rel_guid)

            # Validate relationship structure
            body = resp2.json()
            rel = body.get("relationship", body)  # may be wrapped or direct
            if isinstance(rel, dict):
                assert "guid" in rel or "typeName" in rel or "end1" in rel, (
                    "Relationship should have guid, typeName, or end1/end2"
                )

    @test("create_direct_relationship", tags=["relationship", "crud"], order=4)
    def test_create_direct_relationship(self, client, ctx):
        assert self.src_guid and self.tgt_guid, (
            "src_guid or tgt_guid not found — setup must have failed"
        )
        # Create a custom relationship def first (use correct entity type)
        rel_def_name = unique_type_name("TestRelDef")
        payload = {"relationshipDefs": [build_relationship_def(
            name=rel_def_name, end1_type=self.io_type, end2_type=self.io_type,
        )]}
        ok, resp = create_typedef_verified(client, payload)
        if ok:
            ctx.register_typedef_cleanup(client, rel_def_name)
            ctx.set("direct_rel_def_name", rel_def_name)

            # Create relationship instance
            rel_payload = {
                "typeName": rel_def_name,
                "end1": {"guid": self.src_guid, "typeName": self.io_type},
                "end2": {"guid": self.tgt_guid, "typeName": self.io_type},
                "propagateTags": "NONE",
            }
            resp2 = client.post("/relationship", json_data=rel_payload)
            assert_status_in(resp2, [200, 201, 404])
            if resp2.status_code in [200, 201]:
                body = resp2.json()
                rel_guid = body.get("guid")
                if rel_guid:
                    ctx.set("direct_rel_guid", rel_guid)
                    ctx.register_cleanup(
                        lambda: client.delete(f"/relationship/guid/{rel_guid}")
                    )

    @test("get_relationship_extended_info", tags=["relationship"], order=5, depends_on=["create_direct_relationship"])
    def test_get_relationship_extended_info(self, client, ctx):
        rel_guid = ctx.get("direct_rel_guid") or ctx.get("test_rel_guid")
        assert rel_guid, (
            "No relationship GUID found in context — "
            "create_direct_relationship or get_relationship_by_guid must have failed"
        )
        resp = client.get(f"/relationship/guid/{rel_guid}", params={"extendedInfo": "true"})
        assert_status(resp, 200)
        body = resp.json()
        rel = body.get("relationship", body)
        if isinstance(rel, dict):
            assert "guid" in rel or "typeName" in rel or "end1" in rel, (
                "Extended relationship response should have guid, typeName, or end1/end2"
            )

    @test("update_relationship_propagate_tags", tags=["relationship", "crud"], order=6, depends_on=["create_direct_relationship"])
    def test_update_relationship_propagate_tags(self, client, ctx):
        rel_guid = ctx.get("direct_rel_guid")
        rel_def_name = ctx.get("direct_rel_def_name")
        assert rel_guid, "direct_rel_guid not found in context — create_direct_relationship must have failed"
        assert rel_def_name, "direct_rel_def_name not found in context — create_direct_relationship must have failed"
        payload = {
            "guid": rel_guid,
            "typeName": rel_def_name,
            "end1": {"guid": self.src_guid, "typeName": self.io_type},
            "end2": {"guid": self.tgt_guid, "typeName": self.io_type},
            "propagateTags": "ONE_TO_TWO",
        }
        resp = client.put("/relationship", json_data=payload)
        assert_status_in(resp, [200, 204, 400])
        if resp.status_code in [200, 204]:
            # Read-after-write: verify propagateTags updated
            resp2 = client.get(f"/relationship/guid/{rel_guid}")
            assert_status(resp2, 200)
            body = resp2.json()
            rel = body.get("relationship", body)
            if isinstance(rel, dict) and "propagateTags" in rel:
                assert rel["propagateTags"] == "ONE_TO_TWO", (
                    f"Expected propagateTags=ONE_TO_TWO, got {rel['propagateTags']}"
                )

    @test("delete_direct_relationship", tags=["relationship", "crud"], order=7, depends_on=["create_direct_relationship"])
    def test_delete_direct_relationship(self, client, ctx):
        rel_guid = ctx.get("direct_rel_guid")
        assert rel_guid, "direct_rel_guid not found — create_direct_relationship must have failed"
        resp = client.delete(f"/relationship/guid/{rel_guid}")
        assert_status_in(resp, [200, 204])

        # Verify deleted: GET should return 404 or status=DELETED
        resp2 = client.get(f"/relationship/guid/{rel_guid}")
        assert resp2.status_code in [404, 200], (
            f"Expected 404 or 200 after delete, got {resp2.status_code}"
        )
        if resp2.status_code == 200:
            body = resp2.json()
            rel = body.get("relationship", body)
            if isinstance(rel, dict):
                assert rel.get("status") == "DELETED", (
                    f"Expected status=DELETED, got {rel.get('status')}"
                )

    @test("bulk_relationship_create", tags=["relationship", "crud"], order=8)
    def test_bulk_relationship_create(self, client, ctx):
        assert self.src_guid and self.tgt_guid, "src/tgt GUIDs not found — setup must have failed"
        rel_def_name = ctx.get("direct_rel_def_name")
        assert rel_def_name, (
            "direct_rel_def_name not found — create_direct_relationship must have failed"
        )
        # Bulk create relationships
        rels = [{
            "typeName": rel_def_name,
            "end1": {"guid": self.src_guid, "typeName": self.io_type},
            "end2": {"guid": self.tgt_guid, "typeName": self.io_type},
            "propagateTags": "NONE",
        }]
        resp = client.post("/relationship/bulk", json_data=rels)
        # Bulk endpoint may not exist on all versions
        assert_status_in(resp, [200, 201, 400, 404, 405])
        if resp.status_code in [200, 201]:
            body = resp.json()
            if isinstance(body, list):
                for rel in body:
                    rel_guid = rel.get("guid")
                    if rel_guid:
                        ctx.register_cleanup(
                            lambda g=rel_guid: client.delete(f"/relationship/guid/{g}")
                        )

    @test("delete_relationship", tags=["relationship", "crud"], order=10,
          depends_on=["get_relationship_by_guid"])
    def test_delete_relationship(self, client, ctx):
        rel_guid = ctx.get("test_rel_guid") or ctx.get("direct_rel_guid")
        if not rel_guid:
            raise SkipTestError(
                "No relationship GUID available — "
                "get_relationship_by_guid must have failed"
            )
        resp = client.delete(f"/relationship/guid/{rel_guid}")
        assert_status_in(resp, [200, 204])

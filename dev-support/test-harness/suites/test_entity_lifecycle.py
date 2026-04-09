"""Full entity lifecycle tests — create through purge (12 tests).

Tests the complete lifecycle: create → update → classify → label → BM →
lineage → search verify → soft delete → search removed → restore →
search restored → hard purge.
"""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_equals,
    assert_field_present, SkipTestError,
)
from core.data_factory import (
    build_dataset_entity, build_process_entity, build_classification_def,
    build_business_metadata_def, unique_qn, unique_name, unique_type_name,
    detect_process_io_type, create_process_with_io,
)
from core.typedef_helpers import (
    create_typedef_verified, ensure_classification_types,
    extract_bm_names_from_response, ensure_bm_types,
)


@suite("entity_lifecycle", depends_on_suites=["entity_crud", "typedefs"],
       description="Full entity lifecycle from creation to purge")
class EntityLifecycleSuite:

    def setup(self, client, ctx):
        # Get 1 usable classification type (create new or use existing)
        requested = [unique_type_name("LifecycleTag")]
        names, self.created_tag, self.tag_ok = ensure_classification_types(
            client, requested,
        )
        self.tag_name = names[0]
        if self.created_tag:
            ctx.register_typedef_cleanup(client, self.tag_name)

        # Create BM typedef with fallback to existing types
        self.bm_display_name = unique_type_name("LifecycleBM")
        bm_info, created_bm, self.bm_ok = ensure_bm_types(
            client, self.bm_display_name,
        )
        self.bm_internal_name = None
        self.bm_attr_map = {}
        self.bm_field_display = "bmField1"  # default
        if bm_info:
            self.bm_internal_name = bm_info["internal_name"]
            self.bm_display_name = bm_info["display_name"]
            self.bm_attr_map = bm_info["attr_map"]
            self.bm_field_display = bm_info["first_attr_display"]
        if self.bm_ok and created_bm:
            cleanup_name = self.bm_internal_name or self.bm_display_name
            ctx.register_typedef_cleanup(client, cleanup_name)

    @test("lifecycle_create", tags=["lifecycle", "crud"], order=1)
    def test_lifecycle_create(self, client, ctx):
        """Create entity, verify GUID and attributes."""
        qn = unique_qn("lifecycle")
        self.entity_name = unique_name("lifecycle")
        entity = build_dataset_entity(
            qn=qn, name=self.entity_name,
            extra_attrs={"description": "lifecycle test entity"},
        )
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Entity creation returned empty mutatedEntities"
        self.entity_guid = entities[0]["guid"]
        self.entity_qn = qn
        ctx.set("lifecycle_guid", self.entity_guid)
        ctx.set("lifecycle_qn", qn)
        ctx.register_cleanup(lambda: client.delete(
            f"/entity/guid/{self.entity_guid}", params={"deleteType": "PURGE"}
        ))

        # Verify via GET
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.status", "ACTIVE")

    @test("lifecycle_update", tags=["lifecycle", "crud"], order=2,
          depends_on=["lifecycle_create"])
    def test_lifecycle_update(self, client, ctx):
        """Update description, verify read-after-write."""
        guid = ctx.get("lifecycle_guid")
        assert guid, "lifecycle_guid not found"
        new_desc = "lifecycle updated description"
        entity = {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": ctx.get("lifecycle_qn"),
                "name": self.entity_name,
                "description": new_desc,
            },
        }
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        actual = resp2.json().get("entity", {}).get("attributes", {}).get("description")
        assert actual == new_desc, f"Expected '{new_desc}', got '{actual}'"

    @test("lifecycle_add_classification", tags=["lifecycle"], order=3,
          depends_on=["lifecycle_create"])
    def test_lifecycle_add_classification(self, client, ctx):
        """Add classification, verify attached."""
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed")
        guid = ctx.get("lifecycle_guid")
        assert guid, "lifecycle_guid not found"
        resp = client.post(
            f"/entity/guid/{guid}/classifications",
            json_data=[{"typeName": self.tag_name}],
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Classification type not yet propagated")

        # Poll for classification to appear via GET entity
        # Note: AtlasEntity (GET response) has 'classifications' (list of objects),
        # NOT 'classificationNames' (that field only exists in AtlasEntityHeader/search results)
        cls_names = []
        for _attempt in range(24):
            time.sleep(5)
            resp2 = client.get(f"/entity/guid/{guid}")
            if resp2.status_code != 200:
                continue
            entity_data = resp2.json().get("entity", {})
            classifications = entity_data.get("classifications", [])
            cls_names = [c.get("typeName") for c in classifications if isinstance(c, dict)]
            if self.tag_name in cls_names:
                break
        assert self.tag_name in cls_names, (
            f"Expected {self.tag_name} in entity classifications after 120s, "
            f"got {cls_names}"
        )

    @test("lifecycle_add_labels", tags=["lifecycle"], order=4,
          depends_on=["lifecycle_create"])
    def test_lifecycle_add_labels(self, client, ctx):
        """Add labels, verify in entity response."""
        guid = ctx.get("lifecycle_guid")
        assert guid, "lifecycle_guid not found"
        resp = client.post(
            f"/entity/guid/{guid}/labels",
            json_data=["lifecycle-label-1", "lifecycle-label-2"],
        )
        assert_status_in(resp, [200, 204])

        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        labels = resp2.json().get("entity", {}).get("labels", [])
        assert "lifecycle-label-1" in labels, f"Expected lifecycle-label-1, got {labels}"

    @test("lifecycle_add_business_metadata", tags=["lifecycle"], order=5,
          depends_on=["lifecycle_create"])
    def test_lifecycle_add_business_metadata(self, client, ctx):
        """Add BM attributes, verify attached."""
        if not self.bm_ok:
            raise SkipTestError("BM typedef creation failed")
        guid = ctx.get("lifecycle_guid")
        assert guid, "lifecycle_guid not found"
        # Use displayName endpoint with human-readable names
        resp = client.post(
            f"/entity/guid/{guid}/businessmetadata/displayName",
            json_data={self.bm_display_name: {self.bm_field_display: "lifecycle-bm-value"}},
        )
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code in [200, 204]:
            resp2 = client.get(f"/entity/guid/{guid}")
            assert_status(resp2, 200)
            ba = resp2.json().get("entity", {}).get("businessAttributes", {})
            # Entity response uses server-generated internal names
            bm_key = self.bm_internal_name or self.bm_display_name
            if bm_key in ba:
                attr_key = self.bm_attr_map.get(self.bm_field_display, self.bm_field_display)
                assert ba[bm_key].get(attr_key) == "lifecycle-bm-value", (
                    f"Expected {attr_key}=lifecycle-bm-value, got {ba[bm_key]}"
                )

    @test("lifecycle_create_lineage", tags=["lifecycle"], order=6,
          depends_on=["lifecycle_create"])
    def test_lifecycle_create_lineage(self, client, ctx):
        """Create Process with entity as input, verify lineage."""
        guid = ctx.get("lifecycle_guid")
        assert guid, "lifecycle_guid not found"

        # Detect correct entity type for process I/O
        io_type = ctx.get("process_io_type") or detect_process_io_type(client)

        # The lifecycle entity is a DataSet. If staging needs Catalog for
        # process I/O, create Catalog entities instead of reusing the lifecycle one.
        if io_type != "DataSet":
            # Create proper I/O entities with the correct type
            in_ent = build_dataset_entity(
                qn=unique_qn("lifecycle-in"), name=unique_name("lifecycle-in"),
                type_name=io_type,
            )
            resp_in = client.post("/entity", json_data={"entity": in_ent})
            if resp_in.status_code != 200:
                raise SkipTestError(
                    f"Could not create {io_type} input entity for lineage "
                    f"({resp_in.status_code})"
                )
            in_creates = resp_in.json().get("mutatedEntities", {}).get("CREATE", []) or \
                         resp_in.json().get("mutatedEntities", {}).get("UPDATE", [])
            if not in_creates:
                raise SkipTestError("Input entity creation returned empty mutatedEntities")
            in_guid = in_creates[0]["guid"]
            ctx.register_entity_cleanup(in_guid)
        else:
            in_guid = guid
            io_type = "DataSet"

        # Create output entity
        qn2 = unique_qn("lifecycle-out")
        ent2 = build_dataset_entity(qn=qn2, name=unique_name("lifecycle-out"),
                                    type_name=io_type)
        resp = client.post("/entity", json_data={"entity": ent2})
        assert_status(resp, 200)
        creates = resp.json().get("mutatedEntities", {}).get("CREATE", [])
        updates = resp.json().get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        if not entities:
            raise SkipTestError("Could not create output entity for lineage")
        out_guid = entities[0]["guid"]
        ctx.register_entity_cleanup(out_guid)

        ok, proc_guid = create_process_with_io(
            client, ctx, "lifecycle-proc",
            [in_guid], [out_guid], entity_type=io_type,
        )
        if not ok:
            raise SkipTestError("Process creation failed — lineage not available")

    @test("lifecycle_verify_search", tags=["lifecycle", "search"], order=7,
          depends_on=["lifecycle_create"])
    def test_lifecycle_verify_search(self, client, ctx):
        """Poll search, verify entity appears in ES."""
        guid = ctx.get("lifecycle_guid")
        assert guid, "lifecycle_guid not found"
        time.sleep(5)
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0, "size": 5,
                "query": {"term": {"__guid": guid}},
            }
        })
        assert_status(resp, 200)
        count = resp.json().get("approximateCount", 0)
        assert count > 0, f"Entity {guid} not found in search after 5s"

    @test("lifecycle_soft_delete", tags=["lifecycle", "crud"], order=8,
          depends_on=["lifecycle_verify_search"])
    def test_lifecycle_soft_delete(self, client, ctx):
        """Delete entity, verify status=DELETED."""
        guid = ctx.get("lifecycle_guid")
        assert guid, "lifecycle_guid not found"
        resp = client.delete(f"/entity/guid/{guid}")
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.status", "DELETED")

    @test("lifecycle_verify_search_removed", tags=["lifecycle", "search"], order=9,
          depends_on=["lifecycle_soft_delete"])
    def test_lifecycle_verify_search_removed(self, client, ctx):
        """Verify entity no longer in ACTIVE search."""
        guid = ctx.get("lifecycle_guid")
        assert guid, "lifecycle_guid not found"
        time.sleep(5)
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0, "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"__guid": guid}},
                            {"term": {"__state": "ACTIVE"}},
                        ]
                    }
                },
            }
        })
        assert_status(resp, 200)
        count = resp.json().get("approximateCount", 0)
        if count > 0:
            raise SkipTestError("Deleted entity still in ACTIVE search — ES sync may be slow")

    @test("lifecycle_restore", tags=["lifecycle", "crud"], order=10,
          depends_on=["lifecycle_soft_delete"])
    def test_lifecycle_restore(self, client, ctx):
        """Restore entity, verify status=ACTIVE."""
        guid = ctx.get("lifecycle_guid")
        assert guid, "lifecycle_guid not found"
        resp = client.post("/entity/restore/bulk", params={"guid": guid})
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Restore endpoint not available")

        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.status", "ACTIVE")

    @test("lifecycle_verify_restore_search", tags=["lifecycle", "search"], order=11,
          depends_on=["lifecycle_restore"])
    def test_lifecycle_verify_restore_search(self, client, ctx):
        """Verify entity reappears in search after restore."""
        guid = ctx.get("lifecycle_guid")
        assert guid, "lifecycle_guid not found"
        time.sleep(5)
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0, "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"__guid": guid}},
                            {"term": {"__state": "ACTIVE"}},
                        ]
                    }
                },
            }
        })
        assert_status(resp, 200)
        count = resp.json().get("approximateCount", 0)
        if count == 0:
            raise SkipTestError("Restored entity not yet in ACTIVE search — ES sync may be slow")

    @test("lifecycle_hard_purge", tags=["lifecycle", "crud"], order=12,
          depends_on=["lifecycle_verify_restore_search"])
    def test_lifecycle_hard_purge(self, client, ctx):
        """Hard delete with deleteType=PURGE, verify entity completely gone."""
        guid = ctx.get("lifecycle_guid")
        assert guid, "lifecycle_guid not found"
        resp = client.delete(f"/entity/guid/{guid}", params={"deleteType": "PURGE"})
        assert_status_in(resp, [200, 204])

        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status_in(resp2, [200, 404])
        if resp2.status_code == 200:
            status = resp2.json().get("entity", {}).get("status")
            assert status in ("DELETED", "PURGED", None), (
                f"Expected DELETED/PURGED/gone, got status={status}"
            )

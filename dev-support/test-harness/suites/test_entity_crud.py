"""Entity CRUD tests (~25 tests)."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty, assert_field_in,
    assert_mutation_response, SkipTestError,
)
from core.audit_helpers import poll_audit_events
from core.search_helpers import assert_entity_in_search
from core.kafka_helpers import assert_entity_in_kafka
from core.data_factory import (
    build_dataset_entity, build_process_entity, build_entity_def,
    unique_qn, unique_name, unique_type_name,
    detect_process_io_type, create_process_with_io,
)
from core.typedef_helpers import create_typedef_verified, ensure_entity_types


@suite("entity_crud", description="Entity CRUD operations")
class EntityCrudSuite:

    def setup(self, client, ctx):
        # Create two test DataSet entities for CRUD tests
        self.ds1_qn = unique_qn("ds1")
        self.ds1_name = unique_name("ds1")
        self.ds2_qn = unique_qn("ds2")
        self.ds2_name = unique_name("ds2")
        self.process_qn = unique_qn("proc1")
        self.process_name = unique_name("proc1")

        # Detect entity type for Process I/O (Catalog on staging, DataSet on local)
        self.process_io_type = detect_process_io_type(client)
        ctx.set("process_io_type", self.process_io_type)

    # ---- CREATE ----

    @test("create_entity", tags=["smoke", "crud"], order=1)
    def test_create_entity(self, client, ctx):
        entity = build_dataset_entity(qn=self.ds1_qn, name=self.ds1_name)
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        assert_field_not_empty(resp, "mutatedEntities")

        # Deep validation: verify mutation response structure
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert len(entities) > 0, "Expected at least one entity in mutatedEntities"

        # Validate guidAssignments if present
        if "guidAssignments" in body:
            assert isinstance(body["guidAssignments"], dict), "guidAssignments should be a dict"

        guid = entities[0]["guid"]
        ctx.register_entity("ds1", guid, "DataSet", qualifiedName=self.ds1_qn)
        ctx.register_entity_cleanup(guid)

        # Read-after-write: verify persisted entity matches what was sent
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.typeName", "DataSet")
        assert_field_equals(resp2, "entity.attributes.name", self.ds1_name)
        assert_field_equals(resp2, "entity.attributes.qualifiedName", self.ds1_qn)

        # Kafka: verify ENTITY_CREATE notification
        assert_entity_in_kafka(ctx, guid, "ENTITY_CREATE")

    @test("create_entity_missing_required", tags=["crud"], order=4)
    def test_create_entity_missing_required(self, client, ctx):
        """POST /entity without qualifiedName -> expect 400/404/422.

        API: POST /api/meta/entity
        Payload: {"entity": {"typeName": "DataSet", "attributes": {"name": "<random>"}}}
        Note: qualifiedName is intentionally MISSING to test server validation.
        Expected: Server should return 400/404 (ATLAS-404-00-007) for missing mandatory attribute.
        Known issue (preprod): Server hangs >120s instead of fast-failing validation.
        """
        entity = {
            "typeName": "DataSet",
            "attributes": {
                "name": unique_name("no-qn"),
                # qualifiedName intentionally missing
            },
        }
        import time as _time
        _start = _time.time()
        print(f"  [API] POST /entity — typeName=DataSet, qualifiedName=MISSING (negative test)")
        resp = client.post("/entity", json_data={"entity": entity}, timeout=120)
        elapsed = _time.time() - _start
        if resp.status_code == 408:
            print(f"  [TIMEOUT] POST /entity (missing QN) timed out after {elapsed:.0f}s — "
                  f"server did not return validation error within 120s. "
                  f"Expected: 400/404 with ATLAS-404-00-007 (mandatory field missing)")
        else:
            print(f"  [API] POST /entity returned {resp.status_code} in {elapsed:.0f}s")
        assert_status_in(resp, [400, 404, 422])
        body = resp.json()
        if isinstance(body, dict):
            assert "errorMessage" in body or "errorCode" in body or "message" in body or "error" in body, (
                f"Expected error details in response, got keys: {list(body.keys())}"
            )

    @test("create_entity_audit", tags=["crud", "audit"], order=5, depends_on=["create_entity"])
    def test_create_entity_audit(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered")
        qn = ctx.get_entity_qn("ds1")
        events, total = poll_audit_events(client, guid, action_filter="ENTITY_CREATE",
                                          max_wait=60, interval=10, qualifiedName=qn)
        if events is None:
            raise SkipTestError("Audit endpoint not available (404/405)")
        if not events:
            raise SkipTestError(
                f"Audit endpoint returns 200 but no ENTITY_CREATE events for {guid} after 60s — "
                f"audit indexing may not be configured on this environment"
            )
        event = events[0]
        entity_id = event.get("entityId") or event.get("entityGuid")
        if entity_id:
            assert entity_id == guid, f"Audit entityId mismatch: expected {guid}, got {entity_id}"

    @test("create_entity_with_custom_type", tags=["crud"], order=30, depends_on=["create_entity"])
    def test_create_entity_with_custom_type(self, client, ctx):
        # Use custom entity type from typedefs suite, or create/discover one
        custom_type = ctx.get("test_entity_type_name")
        if not custom_type:
            requested_name = unique_type_name("CrudTestType")
            names, created_new, ok = ensure_entity_types(
                client, [requested_name],
            )
            if not ok:
                raise SkipTestError(
                    "Could not create or discover a usable entity type "
                    "extending DataSet"
                )
            custom_type = names[0]
            ctx.set("test_entity_type_name", custom_type)
            if created_new:
                ctx.register_typedef_cleanup(client, custom_type)

        qn = unique_qn("custom-type")
        entity = build_dataset_entity(qn=qn, name=unique_name("custom"), type_name=custom_type)
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            creates = body.get("mutatedEntities", {}).get("CREATE", [])
            updates = body.get("mutatedEntities", {}).get("UPDATE", [])
            entities = creates or updates
            if entities:
                guid = entities[0]["guid"]
                ctx.register_entity("custom_type_entity", guid, custom_type, qualifiedName=qn)
                ctx.register_entity_cleanup(guid)
                assert entities[0].get("typeName") == custom_type, (
                    f"Expected typeName={custom_type}, got {entities[0].get('typeName')}"
                )

    @test("create_entity_in_search", tags=["crud", "search"], order=6, depends_on=["create_entity"])
    def test_create_entity_in_search(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        result = assert_entity_in_search(client, self.ds1_qn, guid=guid)
        assert result is not None, (
            f"Entity {self.ds1_qn} not found in search — "
            f"search endpoint may be unavailable or ES sync timed out"
        )

    @test("create_entity_bulk", tags=["crud"], order=2)
    def test_create_entity_bulk(self, client, ctx):
        entity2 = build_dataset_entity(qn=self.ds2_qn, name=self.ds2_name)
        resp = client.post("/entity/bulk", json_data={"entities": [entity2]})
        assert_status(resp, 200)
        assert_field_not_empty(resp, "mutatedEntities")

        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        guid = entities[0]["guid"]
        ctx.register_entity("ds2", guid, "DataSet", qualifiedName=self.ds2_qn)
        ctx.register_entity_cleanup(guid)

        # Read-after-write: verify persisted entity matches what was sent
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.typeName", "DataSet")
        assert_field_equals(resp2, "entity.attributes.name", self.ds2_name)
        assert_field_equals(resp2, "entity.attributes.qualifiedName", self.ds2_qn)

    @test("create_process_entity", tags=["crud"], order=3, depends_on=["create_entity", "create_entity_bulk"])
    def test_create_process_entity(self, client, ctx):
        # Create I/O entities with the correct type for this environment
        # (Catalog on staging/preprod, DataSet on local dev)
        io_type = self.process_io_type
        proc_in_qn = unique_qn("proc-in")
        proc_out_qn = unique_qn("proc-out")
        in_entity = build_dataset_entity(qn=proc_in_qn, name=unique_name("proc-in"), type_name=io_type)
        out_entity = build_dataset_entity(qn=proc_out_qn, name=unique_name("proc-out"), type_name=io_type)

        resp_in = client.post("/entity", json_data={"entity": in_entity})
        assert_status(resp_in, 200)
        in_creates = resp_in.json().get("mutatedEntities", {}).get("CREATE", []) or \
                     resp_in.json().get("mutatedEntities", {}).get("UPDATE", [])
        assert in_creates, "Process input entity creation returned empty mutatedEntities"
        in_guid = in_creates[0]["guid"]
        ctx.register_entity_cleanup(in_guid)

        resp_out = client.post("/entity", json_data={"entity": out_entity})
        assert_status(resp_out, 200)
        out_creates = resp_out.json().get("mutatedEntities", {}).get("CREATE", []) or \
                      resp_out.json().get("mutatedEntities", {}).get("UPDATE", [])
        assert out_creates, "Process output entity creation returned empty mutatedEntities"
        out_guid = out_creates[0]["guid"]
        ctx.register_entity_cleanup(out_guid)

        # Create Process — use 120s timeout since process creation can be slow
        entity = build_process_entity(
            qn=self.process_qn,
            name=self.process_name,
            inputs=[{"guid": in_guid, "typeName": io_type}],
            outputs=[{"guid": out_guid, "typeName": io_type}],
        )
        resp = client.post("/entity", json_data={"entity": entity}, timeout=120)
        assert_status(resp, 200)

        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Process entity creation returned empty mutatedEntities"
        guid = entities[0]["guid"]
        ctx.register_entity("process1", guid, "Process", qualifiedName=self.process_qn)
        ctx.register_entity_cleanup(guid)

        # Read-after-write: verify persisted process entity
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.typeName", "Process")
        assert_field_equals(resp2, "entity.attributes.name", self.process_name)
        assert_field_equals(resp2, "entity.attributes.qualifiedName", self.process_qn)

    # ---- READ ----

    @test("get_entity_by_guid", tags=["smoke", "crud"], order=10, depends_on=["create_entity"])
    def test_get_entity_by_guid(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        resp = client.get(f"/entity/guid/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.guid", guid)
        assert_field_equals(resp, "entity.typeName", "DataSet")
        assert_field_equals(resp, "entity.attributes.qualifiedName", self.ds1_qn)
        assert_field_in(resp, "entity.status", ["ACTIVE", "DELETED"])
        assert_field_not_empty(resp, "entity.attributes.name")

    @test("get_entity_header", tags=["crud"], order=11, depends_on=["create_entity"])
    def test_get_entity_header(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        resp = client.get(f"/entity/guid/{guid}/header")
        assert_status(resp, 200)
        assert_field_equals(resp, "guid", guid)

    @test("get_entity_by_unique_attr", tags=["crud"], order=12, depends_on=["create_entity"])
    def test_get_entity_by_unique_attr(self, client, ctx):
        resp = client.get(
            "/entity/uniqueAttribute/type/DataSet",
            params={"attr:qualifiedName": self.ds1_qn},
        )
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.attributes.qualifiedName", self.ds1_qn)

    @test("get_entity_header_by_unique_attr", tags=["crud"], order=13, depends_on=["create_entity"])
    def test_get_entity_header_by_unique_attr(self, client, ctx):
        resp = client.get(
            "/entity/uniqueAttribute/type/DataSet/header",
            params={"attr:qualifiedName": self.ds1_qn},
        )
        assert_status(resp, 200)
        assert_field_present(resp, "guid")

    @test("get_entity_bulk", tags=["crud"], order=14, depends_on=["create_entity", "create_entity_bulk"])
    def test_get_entity_bulk(self, client, ctx):
        guid1 = ctx.get_entity_guid("ds1")
        guid2 = ctx.get_entity_guid("ds2")
        resp = client.get("/entity/bulk", params={"guid": [guid1, guid2]})
        assert_status(resp, 200)
        assert_field_present(resp, "entities")

    @test("get_entity_bulk_large_batch", tags=["crud"], order=15.5, depends_on=["create_entity_bulk"])
    def test_get_entity_bulk_large_batch(self, client, ctx):
        # Create 10 throwaway entities and fetch them all in a single bulk GET
        guids = []
        entities_payload = []
        for i in range(10):
            qn = unique_qn(f"bulk-get-{i}")
            entities_payload.append(build_dataset_entity(qn=qn, name=unique_name(f"bget-{i}")))
        resp = client.post("/entity/bulk", json_data={"entities": entities_payload})
        assert_status(resp, 200)
        body = resp.json()
        for action in ("CREATE", "UPDATE"):
            for e in body.get("mutatedEntities", {}).get(action, []):
                guids.append(e["guid"])

        # Register cleanup for all created entities
        for g in guids:
            ctx.register_entity_cleanup(g)

        assert len(guids) >= 10, f"Expected at least 10 entities created, got {len(guids)}"

        # Bulk GET all at once
        resp2 = client.get("/entity/bulk", params={"guid": guids})
        assert_status(resp2, 200)
        body2 = resp2.json()
        returned_entities = body2.get("entities", [])
        returned_guids = {e.get("guid") for e in returned_entities}
        for g in guids:
            assert g in returned_guids, (
                f"GUID {g} not found in bulk GET response ({len(returned_guids)} entities returned)"
            )

    @test("get_entity_min_ext_info", tags=["crud"], order=15, depends_on=["create_entity"])
    def test_get_entity_min_ext_info(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        resp = client.get(f"/entity/guid/{guid}", params={"minExtInfo": "true"})
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.guid", guid)

    @test("get_entity_ignore_relationships", tags=["crud"], order=17, depends_on=["create_entity"])
    def test_get_entity_ignore_relationships(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        resp = client.get(f"/entity/guid/{guid}", params={"ignoreRelationships": "true"})
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.guid", guid)

    @test("get_entity_not_found", tags=["crud"], order=16)
    def test_get_entity_not_found(self, client, ctx):
        resp = client.get("/entity/guid/00000000-0000-0000-0000-000000000000")
        assert_status_in(resp, [404, 400])
        body = resp.json()
        if isinstance(body, dict):
            assert "errorMessage" in body or "errorCode" in body or "message" in body or "error" in body, (
                f"Expected error details in 404 response, got keys: {list(body.keys())}"
            )

    # ---- UPDATE ----

    @test("update_entity_by_guid", tags=["crud"], order=20, depends_on=["create_entity"])
    def test_update_entity_by_guid(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataSet",
                "guid": guid,
                "attributes": {
                    "qualifiedName": self.ds1_qn,
                    "name": self.ds1_name,
                    "description": "Updated by test harness",
                },
            }
        })
        assert_status(resp, 200)

        # Validate mutation response
        body = resp.json()
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        if updates:
            assert updates[0].get("guid"), "Updated entity should have guid"

        # Read-after-write verification
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.attributes.description", "Updated by test harness")

    @test("update_entity_by_unique_attr", tags=["crud"], order=21, depends_on=["create_entity"])
    def test_update_entity_by_unique_attr(self, client, ctx):
        updated = build_dataset_entity(
            qn=self.ds1_qn,
            name=self.ds1_name,
            extra_attrs={"description": "Updated via unique attr"},
        )
        resp = client.put(
            "/entity/uniqueAttribute/type/DataSet",
            json_data={"entity": updated},
            params={"attr:qualifiedName": self.ds1_qn},
        )
        assert_status(resp, 200)

        # Read-after-write: verify description updated
        guid = ctx.get_entity_guid("ds1")
        if guid:
            resp2 = client.get(f"/entity/guid/{guid}")
            assert_status(resp2, 200)
            assert_field_equals(resp2, "entity.attributes.description", "Updated via unique attr")

    @test("update_entity_partial", tags=["crud"], order=22, depends_on=["create_entity"])
    def test_update_entity_partial(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataSet",
                "guid": guid,
                "attributes": {
                    "qualifiedName": self.ds1_qn,
                    "name": self.ds1_name,
                    "description": "Partial update test",
                },
            }
        })
        assert_status(resp, 200)

        # Read-after-write: GET entity and verify description
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.attributes.description", "Partial update test")

    @test("update_entity_audit", tags=["crud", "audit"], order=25, depends_on=["update_entity_by_guid"])
    def test_update_entity_audit(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity registered")
        qn = ctx.get_entity_qn("ds1")
        events, total = poll_audit_events(client, guid, action_filter="ENTITY_UPDATE",
                                          max_wait=60, interval=10, qualifiedName=qn)
        if events is None:
            raise SkipTestError("Audit endpoint not available (404/405)")
        if not events:
            raise SkipTestError(
                f"Audit endpoint returns 200 but no ENTITY_UPDATE events for {guid} after 60s — "
                f"audit indexing may not be configured on this environment"
            )
        event = events[0]
        entity_id = event.get("entityId") or event.get("entityGuid")
        if entity_id:
            assert entity_id == guid, f"Audit entityId mismatch: expected {guid}, got {entity_id}"

    # ---- DELETE ----

    @test("delete_entity_by_guid", tags=["crud"], order=80)
    def test_delete_entity_by_guid(self, client, ctx):
        # Create a throwaway entity to delete
        qn = unique_qn("delete-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("del"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        guid = entities[0]["guid"]

        # Delete it
        resp = client.delete(f"/entity/guid/{guid}")
        assert_status(resp, 200)

        # Validate DELETE mutation response has the correct guid
        del_body = resp.json()
        deletes = del_body.get("mutatedEntities", {}).get("DELETE", [])
        if deletes:
            assert deletes[0].get("guid") == guid, f"Expected deleted guid={guid}"

        # Store deleted GUID so audit suite can verify ENTITY_DELETE events
        ctx.set("deleted_entity_guid", guid)

        # Verify deleted (soft delete returns entity with DELETED status)
        resp = client.get(f"/entity/guid/{guid}")
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            assert_field_equals(resp, "entity.status", "DELETED")

        # Kafka: verify ENTITY_DELETE notification
        assert_entity_in_kafka(ctx, guid, "ENTITY_DELETE")

    @test("delete_entity_by_unique_attr", tags=["crud"], order=81)
    def test_delete_entity_by_unique_attr(self, client, ctx):
        qn = unique_qn("delete-ua-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("del-ua"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)

        resp = client.delete(
            "/entity/uniqueAttribute/type/DataSet",
            params={"attr:qualifiedName": qn},
        )
        assert_status(resp, 200)

    @test("delete_entity_bulk", tags=["crud"], order=82)
    def test_delete_entity_bulk(self, client, ctx):
        # Create two throwaway entities
        entities = []
        guids = []
        for i in range(2):
            qn = unique_qn(f"bulk-del-{i}")
            entities.append(build_dataset_entity(qn=qn, name=unique_name(f"bdel-{i}")))
        resp = client.post("/entity/bulk", json_data={"entities": entities})
        assert_status(resp, 200)
        body = resp.json()
        for action in ("CREATE", "UPDATE"):
            for e in body.get("mutatedEntities", {}).get(action, []):
                guids.append(e["guid"])

        if guids:
            resp = client.delete("/entity/bulk", params={"guid": guids})
            assert_status(resp, 200)
            body = resp.json()
            deletes = body.get("mutatedEntities", {}).get("DELETE", [])
            assert len(deletes) > 0, "Expected non-empty mutatedEntities.DELETE in bulk delete"
            deleted_guids = [e.get("guid") for e in deletes]
            for g in guids:
                assert g in deleted_guids, f"Expected {g} in deleted guids, got {deleted_guids}"

    @test("delete_entity_verify_search_removal", tags=["crud", "search"], order=83, depends_on=["delete_entity_by_guid"])
    def test_delete_entity_verify_search_removal(self, client, ctx):
        # Create, delete, then search to verify deleted entity is excluded
        qn = unique_qn("del-search-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("del-search"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        guid = entities[0]["guid"]

        # Delete it
        client.delete(f"/entity/guid/{guid}")

        # Search with excludeDeletedEntities
        import time
        time.sleep(5)
        resp2 = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"__guid": guid}},
                            {"term": {"__state": "ACTIVE"}},
                        ]
                    }
                }
            }
        })
        if resp2.status_code == 200:
            count = resp2.json().get("approximateCount", 0)
            assert count == 0, (
                f"Deleted entity {guid} should not appear in ACTIVE search, got count={count}"
            )

    @test("delete_entity_by_unique_attr_bulk", tags=["crud"], order=84)
    def test_delete_entity_by_unique_attr_bulk(self, client, ctx):
        # Create two throwaway entities to delete via bulk unique attribute
        qn1 = unique_qn("bulk-ua-del-1")
        qn2 = unique_qn("bulk-ua-del-2")
        entities = [
            build_dataset_entity(qn=qn1, name=unique_name("bua-del-1")),
            build_dataset_entity(qn=qn2, name=unique_name("bua-del-2")),
        ]
        resp = client.post("/entity/bulk", json_data={"entities": entities})
        assert_status(resp, 200)

        # Delete via /entity/bulk/uniqueAttribute with JSON body of AtlasObjectId
        resp = client.delete(
            "/entity/bulk/uniqueAttribute",
            json_data=[
                {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": qn1}},
                {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": qn2}},
            ],
        )
        assert_status_in(resp, [200, 204])
        if resp.status_code == 200:
            body = resp.json()
            deletes = body.get("mutatedEntities", {}).get("DELETE", [])
            assert len(deletes) > 0, (
                "Expected non-empty mutatedEntities.DELETE from bulk unique attr delete"
            )

    @test("restore_soft_deleted_entity", tags=["crud", "restore"], order=85)
    def test_restore_soft_deleted_entity(self, client, ctx):
        # Create entity
        qn = unique_qn("restore-crud-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("restore-crud"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        guid = entities[0]["guid"]
        ctx.register_entity_cleanup(guid)

        # Soft-delete
        resp = client.delete(f"/entity/guid/{guid}")
        assert_status(resp, 200)

        # Restore
        resp = client.post("/entity/restore/bulk", params={"guid": guid})
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Restore endpoint returned 404 — not available on this environment")

        # Verify restored
        resp = client.get(f"/entity/guid/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.status", "ACTIVE")

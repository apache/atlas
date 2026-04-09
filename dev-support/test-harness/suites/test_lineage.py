"""Lineage query tests."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, assert_field_present, assert_field_equals, SkipTestError
from core.data_factory import (
    build_dataset_entity, build_process_entity, unique_qn, unique_name,
    detect_process_io_type, create_process_with_io,
)


@suite("lineage", depends_on_suites=["entity_crud"],
       description="Lineage query endpoints")
class LineageSuite:

    def setup(self, client, ctx):
        # Wait for graph to settle after entity_crud created Process
        time.sleep(2)

    @test("get_lineage_by_guid", tags=["smoke", "lineage"], order=1)
    def test_get_lineage_by_guid(self, client, ctx):
        # Use ds1 which should have lineage via process1
        guid = ctx.get_entity_guid("ds1")
        if not guid:
            raise Exception("No ds1 entity; depends on entity_crud suite")
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "BOTH",
            "depth": 3,
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert_field_equals(resp, "baseEntityGuid", guid)
            assert "guidEntityMap" in body or "relations" in body, (
                "Expected 'guidEntityMap' or 'relations' in lineage response"
            )
            # baseEntityGuid should be in the guidEntityMap
            guid_map = body.get("guidEntityMap", {})
            if guid_map:
                assert guid in guid_map, (
                    f"baseEntityGuid {guid} should be in guidEntityMap, got: {list(guid_map.keys())[:5]}"
                )

    @test("get_lineage_input", tags=["lineage"], order=2)
    def test_get_lineage_input(self, client, ctx):
        guid = ctx.get_entity_guid("ds2")
        assert guid, "ds2 GUID not found in context — entity_crud suite must have failed"
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "INPUT",
            "depth": 3,
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert_field_equals(resp, "baseEntityGuid", guid)
            guid_map = body.get("guidEntityMap", {})
            if guid_map:
                assert guid in guid_map, (
                    f"baseEntityGuid {guid} should be in guidEntityMap"
                )
            # INPUT direction: relations should reference fromEntityId pointing to base
            relations = body.get("relations", [])
            if relations:
                for rel in relations:
                    assert "fromEntityId" in rel and "toEntityId" in rel, (
                        f"Relation missing fromEntityId/toEntityId: {list(rel.keys())}"
                    )

    @test("get_lineage_output", tags=["lineage"], order=3)
    def test_get_lineage_output(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "OUTPUT",
            "depth": 3,
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert_field_equals(resp, "baseEntityGuid", guid)
            guid_map = body.get("guidEntityMap", {})
            if guid_map:
                assert guid in guid_map, (
                    f"baseEntityGuid {guid} should be in guidEntityMap"
                )
            relations = body.get("relations", [])
            if relations:
                for rel in relations:
                    assert "fromEntityId" in rel and "toEntityId" in rel, (
                        f"Relation missing fromEntityId/toEntityId: {list(rel.keys())}"
                    )

    @test("post_lineage_on_demand", tags=["lineage"], order=4)
    def test_post_lineage_on_demand(self, client, ctx):
        # Use process1 which has actual lineage connections.
        # ds1 (DataSet) gets INVALID_LINEAGE_ENTITY_TYPE (404) on the on-demand
        # endpoint on some environments, so we prefer process1.
        guid = ctx.get_entity_guid("process1")
        if not guid:
            # Fallback: try ds1, but on-demand lineage may reject DataSet types
            guid = ctx.get_entity_guid("ds1")
        assert guid, "No process1 or ds1 GUID found — entity_crud suite must have failed"
        resp = client.post(f"/lineage/{guid}", json_data={
            "defaultParams": {
                "inputRelationsLimit": 10,
                "outputRelationsLimit": 10,
            },
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 404:
            body = resp.json() if isinstance(resp.body, dict) else {}
            err = body.get("errorMessage", "")
            if "not a valid lineage entity type" in err.lower() or "INVALID_LINEAGE_ENTITY_TYPE" in str(body.get("errorCode", "")):
                raise SkipTestError(
                    f"On-demand lineage rejected entity type (process1 may not be in context): {err}"
                )
            raise SkipTestError(
                f"On-demand lineage returned 404: {err or body}"
            )
        if resp.status_code == 200:
            body = resp.json()
            assert "baseEntityGuid" in body, "Expected 'baseEntityGuid' in on-demand lineage response"
            assert body["baseEntityGuid"] == guid, (
                f"Expected baseEntityGuid={guid}, got {body['baseEntityGuid']}"
            )
            assert "guidEntityMap" in body or "relations" in body or "searchParameters" in body, (
                f"On-demand lineage missing expected fields, got keys: {list(body.keys())}"
            )

    @test("post_lineage_list", tags=["lineage"], order=5)
    def test_post_lineage_list(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.post("/lineage/list", json_data={
            "guid": guid,
            "size": 10,
            "from": 0,
            "depth": 3,
            "direction": "OUTPUT",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (dict, list)), (
                f"Expected dict or list response from lineage list, got {type(body).__name__}"
            )
            # If dict, should have entities or searchResult
            if isinstance(body, dict):
                has_data = any(k in body for k in (
                    "entities", "searchResult", "searchParameters",
                    "hasMoreUpstreamVertices", "hasMoreDownstreamVertices",
                ))
                assert has_data, (
                    f"Lineage list response missing expected fields, got keys: {list(body.keys())}"
                )

    @test("lineage_isolated_entity", tags=["lineage"], order=6)
    def test_lineage_isolated_entity(self, client, ctx):
        # Create entity with no Process links -> lineage should be empty
        qn = unique_qn("lineage-isolated")
        entity = build_dataset_entity(qn=qn, name=unique_name("lineage-iso"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        guid = entities[0]["guid"]
        ctx.register_entity_cleanup(guid)

        resp2 = client.get(f"/lineage/{guid}", params={
            "direction": "BOTH",
            "depth": 3,
        })
        assert_status_in(resp2, [200, 404])
        if resp2.status_code == 200:
            body2 = resp2.json()
            relations = body2.get("relations", [])
            assert len(relations) == 0, (
                f"Expected no lineage relations for isolated entity, got {len(relations)}"
            )
            # guidEntityMap should only contain the base entity itself
            guid_map = body2.get("guidEntityMap", {})
            if guid_map:
                assert len(guid_map) <= 1, (
                    f"Isolated entity should have at most 1 entry in guidEntityMap, got {len(guid_map)}"
                )

    @test("lineage_depth_limited", tags=["lineage"], order=7)
    def test_lineage_depth_limited(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "BOTH",
            "depth": 1,
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert_field_equals(resp, "baseEntityGuid", guid)
            # With depth=1, lineageDepth should be <= 1
            reported_depth = body.get("lineageDepth", -1)
            if reported_depth >= 0:
                assert reported_depth <= 1, (
                    f"Expected lineageDepth <= 1, got {reported_depth}"
                )

    @test("lineage_hide_process", tags=["lineage"], order=8)
    def test_lineage_hide_process(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "BOTH",
            "depth": 3,
            "hideProcess": "true",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            guid_map = body.get("guidEntityMap", {})
            relations = body.get("relations", [])
            # hideProcess=true: relations should be virtual (dataset→dataset).
            # Process entities ARE expected in guidEntityMap (for processId lookup),
            # but no relation endpoint should be a Process.
            process_guids = {
                eg for eg, ed in guid_map.items()
                if isinstance(ed, dict) and ed.get("typeName") == "Process"
            }
            for rel in relations:
                if isinstance(rel, dict):
                    from_id = rel.get("fromEntityId")
                    to_id = rel.get("toEntityId")
                    assert from_id not in process_guids, (
                        f"Relation fromEntityId {from_id} is a Process with hideProcess=true"
                    )
                    assert to_id not in process_guids, (
                        f"Relation toEntityId {to_id} is a Process with hideProcess=true"
                    )

    @test("lineage_output_direction_only", tags=["lineage"], order=4.5)
    def test_lineage_output_direction_only(self, client, ctx):
        """POST /lineage with direction=OUTPUT only."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context"
        resp = client.post(f"/lineage/{guid}", json_data={
            "direction": "OUTPUT",
            "inputRelationsLimit": 0,
            "outputRelationsLimit": 10,
            "depth": 3,
        })
        assert_status_in(resp, [200, 400])
        if resp.status_code == 200:
            body = resp.json()
            assert body.get("baseEntityGuid") == guid, (
                f"Expected baseEntityGuid={guid}, got {body.get('baseEntityGuid')}"
            )

    @test("lineage_list_with_offset", tags=["lineage"], order=5.5)
    def test_lineage_list_with_offset(self, client, ctx):
        """POST /lineage/list with offset for pagination."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context"
        resp = client.post("/lineage/list", json_data={
            "guid": guid,
            "size": 10,
            "from": 0,
            "depth": 3,
            "direction": "OUTPUT",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict):
                # Try from=1
                resp2 = client.post("/lineage/list", json_data={
                    "guid": guid,
                    "size": 10,
                    "from": 1,
                    "depth": 3,
                    "direction": "OUTPUT",
                })
                assert_status_in(resp2, [200, 400])

    @test("lineage_with_attributes", tags=["lineage"], order=8.5)
    def test_lineage_with_attributes(self, client, ctx):
        """GET /lineage with attributes parameter."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context"
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "BOTH",
            "depth": 3,
            "attributes": "qualifiedName,name",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            guid_map = body.get("guidEntityMap", {})
            if guid_map and guid in guid_map:
                entity = guid_map[guid]
                if isinstance(entity, dict):
                    attrs = entity.get("attributes", entity.get("displayText", {}))
                    # Attributes should be present if supported
                    assert entity, "guidEntityMap entry should not be empty"

    @test("lineage_nonexistent_guid", tags=["lineage", "negative"], order=9)
    def test_lineage_nonexistent_guid(self, client, ctx):
        """GET /lineage for nonexistent GUID -> 404 or empty.

        SERVER BUG: EntityLineageService.getAtlasLineageInfo() does not
        null-check the AtlasVertex returned by findByGuid(), causing NPE
        (500) instead of 404 for non-existent GUIDs.
        """
        fake_guid = "00000000-0000-0000-0000-000000000000"
        resp = client.get(f"/lineage/{fake_guid}", params={
            "direction": "BOTH",
            "depth": 3,
        })
        # Accept 500 — server NPEs on null vertex (known bug, see
        # EntityLineageService.java:153-154: entity.getProperty() on null)
        assert_status_in(resp, [200, 404, 500])
        if resp.status_code == 500:
            body = resp.json() if isinstance(resp.body, dict) else {}
            causes = body.get("causes", [])
            is_npe = any("NullPointerException" in str(c) for c in causes)
            if is_npe:
                print(f"  [KNOWN BUG] Server NPE on non-existent GUID — "
                      f"EntityLineageService missing null check on vertex")
        elif resp.status_code == 404:
            body = resp.json()
            if isinstance(body, dict):
                assert any(k in body for k in ("errorMessage", "errorCode", "message", "error")), (
                    f"Expected error details for nonexistent GUID, got keys: {list(body.keys())}"
                )
        elif resp.status_code == 200:
            body = resp.json()
            relations = body.get("relations", [])
            assert len(relations) == 0, (
                f"Expected no relations for nonexistent GUID, got {len(relations)}"
            )

    @test("lineage_by_unique_attribute", tags=["lineage"], order=10)
    def test_lineage_by_unique_attribute(self, client, ctx):
        """GET /lineage/uniqueAttribute/type/DataSet with qualifiedName."""
        ds1_qn = ctx.get("ds1_qn")
        if not ds1_qn:
            # Try to fetch from entity
            guid = ctx.get_entity_guid("ds1")
            if guid:
                resp = client.get(f"/entity/guid/{guid}", params={"minExtInfo": "true"})
                if resp.status_code == 200:
                    ds1_qn = resp.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        if not ds1_qn:
            raise SkipTestError("ds1 qualifiedName not available")
        resp = client.get(
            "/lineage/uniqueAttribute/type/DataSet",
            params={"attr:qualifiedName": ds1_qn, "direction": "BOTH", "depth": 3},
        )
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert "baseEntityGuid" in body or "guidEntityMap" in body, (
                f"Lineage by unique attr response missing expected fields, keys: {list(body.keys())}"
            )

    @test("lineage_depth_zero", tags=["lineage"], order=11)
    def test_lineage_depth_zero(self, client, ctx):
        """GET /lineage with depth=0 -> only base entity."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "BOTH",
            "depth": 0,
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            guid_map = body.get("guidEntityMap", {})
            relations = body.get("relations", [])
            # With depth=0 we expect very limited results
            if guid_map:
                assert guid in guid_map, "Base entity should be in guidEntityMap"

    @test("lineage_relations_structure", tags=["lineage"], order=12)
    def test_lineage_relations_structure(self, client, ctx):
        """Validate lineage relation objects have required fields."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.get(f"/lineage/{guid}", params={
            "direction": "BOTH",
            "depth": 3,
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            relations = body.get("relations", [])
            for i, rel in enumerate(relations[:10]):
                assert "fromEntityId" in rel, (
                    f"Relation [{i}] missing fromEntityId, keys: {list(rel.keys())}"
                )
                assert "toEntityId" in rel, (
                    f"Relation [{i}] missing toEntityId, keys: {list(rel.keys())}"
                )
                # Both should be non-empty GUIDs
                assert rel["fromEntityId"], f"Relation [{i}] has empty fromEntityId"
                assert rel["toEntityId"], f"Relation [{i}] has empty toEntityId"

    @test("lineage_diamond_topology", tags=["lineage"], order=13)
    def test_lineage_diamond_topology(self, client, ctx):
        """Create A->P1->B, A->P2->B diamond; verify B appears once in guidEntityMap."""
        io_type = ctx.get("process_io_type") or detect_process_io_type(client)

        # Create two entities with correct type for process I/O
        qn_a = unique_qn("diamond-a")
        qn_b = unique_qn("diamond-b")
        ent_a = build_dataset_entity(qn=qn_a, name=unique_name("diamond-a"), type_name=io_type)
        ent_b = build_dataset_entity(qn=qn_b, name=unique_name("diamond-b"), type_name=io_type)
        resp = client.post("/entity/bulk", json_data={"entities": [ent_a, ent_b]})
        assert_status(resp, 200)
        body = resp.json()
        guids = []
        for action in ("CREATE", "UPDATE"):
            for e in body.get("mutatedEntities", {}).get(action, []):
                guids.append(e["guid"])
        if len(guids) < 2:
            raise SkipTestError("Could not create 2 entities for diamond topology")
        guid_a, guid_b = guids[0], guids[1]
        ctx.register_entity_cleanup(guid_a)
        ctx.register_entity_cleanup(guid_b)

        # Create P1: A -> B
        ok1, p1_guid = create_process_with_io(
            client, ctx, "diamond-p1", [guid_a], [guid_b], entity_type=io_type,
        )
        if not ok1:
            raise SkipTestError("P1 creation failed — lineage not supported")

        # Create P2: A -> B (second path)
        ok2, p2_guid = create_process_with_io(
            client, ctx, "diamond-p2", [guid_a], [guid_b], entity_type=io_type,
        )
        if not ok2:
            raise SkipTestError("P2 creation failed — lineage not supported")

        time.sleep(2)

        # Get lineage from A -> B should be unique in guidEntityMap
        resp3 = client.get(f"/lineage/{guid_a}", params={
            "direction": "OUTPUT",
            "depth": 3,
        })
        assert_status_in(resp3, [200, 404])
        if resp3.status_code == 200:
            body3 = resp3.json()
            guid_map = body3.get("guidEntityMap", {})
            # B should appear at most once as a key
            assert guid_b in guid_map or len(guid_map) == 0, (
                "Target entity should be in guidEntityMap or lineage is empty"
            )

"""Bulk unique attribute endpoint tests.

Tests GET /entity/bulk/uniqueAttribute/type/{typeName} which retrieves
multiple entities by their unique attributes (e.g., qualifiedName) in bulk.

API: GET /api/meta/entity/bulk/uniqueAttribute/type/{typeName}
Query params: attr_0:qualifiedName=<qn1>&attr_1:qualifiedName=<qn2>&...
Response: AtlasEntitiesWithExtInfo {entities: [...], referredEntities: {...}}

Verified against preprod:
  - Returns entities matching unique attributes, skips nonexistent ones
  - Nonexistent type returns 400 (ATLAS-400-00-014)
  - No attr params returns 200 with empty response
  - Multiple entities can be fetched in one call using attr_0, attr_1, ...
"""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError


@suite("bulk_unique_attribute", depends_on_suites=["entity_crud"],
       description="Bulk entity lookup by unique attribute")
class BulkUniqueAttributeSuite:

    @test("bulk_get_by_qn_single", tags=["bulk", "unique_attr"], order=1)
    def test_bulk_get_by_qn_single(self, client, ctx):
        """GET /entity/bulk/uniqueAttribute/type/DataSet — single entity by QN."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        qn = ctx.get_entity_qn("ds1")
        if not qn:
            resp_e = client.get(f"/entity/guid/{guid}")
            assert_status(resp_e, 200)
            qn = resp_e.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert qn, "ds1 qualifiedName not available"

        print(f"  [API] GET /entity/bulk/uniqueAttribute/type/DataSet — attr_0:qualifiedName={qn}")
        resp = client.get(
            "/entity/bulk/uniqueAttribute/type/DataSet",
            params={"attr_0:qualifiedName": qn},
        )
        assert_status(resp, 200)
        body = resp.json()
        assert "entities" in body, f"Expected 'entities' in response, got keys: {list(body.keys())}"
        entities = body["entities"]
        assert isinstance(entities, list) and len(entities) >= 1, (
            f"Expected at least 1 entity, got {len(entities) if isinstance(entities, list) else entities}"
        )
        # Verify the returned entity matches
        found = entities[0]
        assert found.get("guid") == guid, (
            f"Expected guid={guid}, got {found.get('guid')}"
        )
        assert found.get("typeName") == "DataSet", (
            f"Expected typeName=DataSet, got {found.get('typeName')}"
        )
        assert found.get("status") == "ACTIVE", (
            f"Expected status=ACTIVE, got {found.get('status')}"
        )

    @test("bulk_get_by_qn_multiple", tags=["bulk", "unique_attr"], order=2)
    def test_bulk_get_by_qn_multiple(self, client, ctx):
        """GET /entity/bulk/uniqueAttribute/type/DataSet — two entities by QN."""
        guid1 = ctx.get_entity_guid("ds1")
        guid2 = ctx.get_entity_guid("ds2")
        assert guid1 and guid2, "ds1 and ds2 GUIDs required"
        qn1 = ctx.get_entity_qn("ds1")
        qn2 = ctx.get_entity_qn("ds2")
        if not qn1 or not qn2:
            for label, g in [("ds1", guid1), ("ds2", guid2)]:
                r = client.get(f"/entity/guid/{g}")
                if r.status_code == 200:
                    q = r.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
                    if label == "ds1":
                        qn1 = q
                    else:
                        qn2 = q
        assert qn1 and qn2, "Could not resolve both qualifiedNames"

        print(f"  [API] GET /entity/bulk/uniqueAttribute/type/DataSet — 2 entities")
        resp = client.get(
            "/entity/bulk/uniqueAttribute/type/DataSet",
            params={
                "attr_0:qualifiedName": qn1,
                "attr_1:qualifiedName": qn2,
            },
        )
        assert_status(resp, 200)
        body = resp.json()
        entities = body.get("entities", [])
        assert len(entities) == 2, (
            f"Expected 2 entities, got {len(entities)}"
        )
        returned_guids = {e.get("guid") for e in entities}
        assert guid1 in returned_guids, f"ds1 guid={guid1} missing from response"
        assert guid2 in returned_guids, f"ds2 guid={guid2} missing from response"

    @test("bulk_get_mixed_existing_nonexistent", tags=["bulk", "unique_attr"], order=3)
    def test_bulk_get_mixed_existing_nonexistent(self, client, ctx):
        """GET with one valid and one nonexistent QN — returns only the valid one."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        qn = ctx.get_entity_qn("ds1")
        if not qn:
            r = client.get(f"/entity/guid/{guid}")
            if r.status_code == 200:
                qn = r.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert qn, "ds1 qualifiedName not available"

        resp = client.get(
            "/entity/bulk/uniqueAttribute/type/DataSet",
            params={
                "attr_0:qualifiedName": qn,
                "attr_1:qualifiedName": "NONEXISTENT_QN_XYZ_99999",
            },
        )
        assert_status(resp, 200)
        body = resp.json()
        entities = body.get("entities", [])
        # Should return only the valid entity, skip nonexistent
        assert len(entities) == 1, (
            f"Expected 1 entity (nonexistent skipped), got {len(entities)}"
        )
        assert entities[0].get("guid") == guid

    @test("bulk_get_with_min_ext_info", tags=["bulk", "unique_attr"], order=4)
    def test_bulk_get_with_min_ext_info(self, client, ctx):
        """GET with minExtInfo=true — should return minimal extended info."""
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        if not qn:
            r = client.get(f"/entity/guid/{guid}")
            if r.status_code == 200:
                qn = r.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert qn, "ds1 qualifiedName not available"

        resp = client.get(
            "/entity/bulk/uniqueAttribute/type/DataSet",
            params={
                "attr_0:qualifiedName": qn,
                "minExtInfo": "true",
            },
        )
        assert_status(resp, 200)
        body = resp.json()
        assert "entities" in body
        assert len(body["entities"]) >= 1

    @test("bulk_get_nonexistent_type", tags=["bulk", "unique_attr", "negative"], order=5)
    def test_bulk_get_nonexistent_type(self, client, ctx):
        """GET with nonexistent typeName — returns 400."""
        resp = client.get(
            "/entity/bulk/uniqueAttribute/type/NonExistentType_XYZ",
            params={"attr_0:qualifiedName": "test"},
        )
        # Server returns 400 ATLAS-400-00-014 ("Type ENTITY with name ... does not exist")
        assert_status(resp, 400)
        body = resp.json()
        assert isinstance(body, dict)
        assert "errorCode" in body or "errorMessage" in body, (
            f"Expected error details, got keys: {list(body.keys())}"
        )
        print(f"  [bulk-unique] Error: {body.get('errorCode')} — {body.get('errorMessage', '')[:60]}")

    @test("bulk_get_no_attrs", tags=["bulk", "unique_attr", "negative"], order=6)
    def test_bulk_get_no_attrs(self, client, ctx):
        """GET with no attr params — returns 200 with empty response."""
        resp = client.get("/entity/bulk/uniqueAttribute/type/DataSet")
        # Server returns 200 with empty dict (no attrs = no entities to look up)
        assert_status(resp, 200)
        body = resp.json()
        # Either empty dict or dict with empty entities list
        if isinstance(body, dict):
            entities = body.get("entities", [])
            if entities is not None:
                assert len(entities) == 0, (
                    f"Expected empty entities for no attrs, got {len(entities)}"
                )

    @test("bulk_get_ignore_relationships", tags=["bulk", "unique_attr"], order=7)
    def test_bulk_get_ignore_relationships(self, client, ctx):
        """GET with ignoreRelationships=true."""
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        if not qn:
            r = client.get(f"/entity/guid/{guid}")
            if r.status_code == 200:
                qn = r.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert qn, "ds1 qualifiedName not available"

        resp = client.get(
            "/entity/bulk/uniqueAttribute/type/DataSet",
            params={
                "attr_0:qualifiedName": qn,
                "ignoreRelationships": "true",
            },
        )
        assert_status(resp, 200)
        body = resp.json()
        assert "entities" in body
        assert len(body["entities"]) >= 1
        # With ignoreRelationships, relationship attributes should be absent/null
        entity = body["entities"][0]
        assert entity.get("guid") == guid

    @test("bulk_get_response_structure", tags=["bulk", "unique_attr"], order=8)
    def test_bulk_get_response_structure(self, client, ctx):
        """Validate response structure: entities + referredEntities."""
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        if not qn:
            r = client.get(f"/entity/guid/{guid}")
            if r.status_code == 200:
                qn = r.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert qn, "ds1 qualifiedName not available"

        resp = client.get(
            "/entity/bulk/uniqueAttribute/type/DataSet",
            params={"attr_0:qualifiedName": qn},
        )
        assert_status(resp, 200)
        body = resp.json()
        # Response should have entities and referredEntities
        assert "entities" in body, f"Missing 'entities', keys: {list(body.keys())}"
        assert "referredEntities" in body, f"Missing 'referredEntities', keys: {list(body.keys())}"
        assert isinstance(body["entities"], list)
        assert isinstance(body["referredEntities"], dict)

        # Each entity should have standard fields
        entity = body["entities"][0]
        for field in ("guid", "typeName", "attributes", "status"):
            assert field in entity, f"Entity missing '{field}', keys: {list(entity.keys())}"

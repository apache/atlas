"""Index repair endpoint tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError


@suite("repair", depends_on_suites=["entity_crud"],
       description="Index repair endpoints")
class RepairSuite:

    @test("repair_single_index", tags=["repair"], order=1)
    def test_repair_single_index(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        # Need qualifiedName for the entity
        resp_entity = client.get(f"/entity/guid/{guid}")
        assert_status(resp_entity, 200)
        qn = resp_entity.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert qn, f"qualifiedName not found on entity {guid}"

        resp = client.post("/repair/single-index", params={
            "qualifiedName": qn,
        })
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict):
                # Repair response may contain status or message
                assert body, "Expected non-empty response from repair/single-index"

    @test("repair_composite_index", tags=["repair"], order=2)
    def test_repair_composite_index(self, client, ctx):
        resp = client.post("/repair/composite-index", json_data={
            "propertyName": "qualifiedName",
        })
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict):
                assert body, "Expected non-empty response from repair/composite-index"

    @test("repair_batch", tags=["repair"], order=3)
    def test_repair_batch(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.post("/repair/batch", json_data={
            "guids": [guid],
        }, params={"indexType": "SINGLE"})
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict):
                assert body, "Expected non-empty response from repair/batch"

    @test("repair_single_by_guid", tags=["repair"], order=4)
    def test_repair_single_by_guid(self, client, ctx):
        """POST /repair/single-index with guid param."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/repair/single-index", params={"guid": guid})
        assert_status_in(resp, [200, 204, 400, 404])

    @test("repair_batch_multiple", tags=["repair"], order=5)
    def test_repair_batch_multiple(self, client, ctx):
        """POST /repair/batch with multiple GUIDs."""
        guid1 = ctx.get_entity_guid("ds1")
        guid2 = ctx.get_entity_guid("ds2")
        if not guid1 or not guid2:
            raise SkipTestError("ds1/ds2 GUIDs not available")
        resp = client.post("/repair/batch", json_data={
            "guids": [guid1, guid2],
        }, params={"indexType": "SINGLE"})
        assert_status_in(resp, [200, 204, 400, 404])

    @test("repair_all_classifications", tags=["repair"], order=6)
    def test_repair_all_classifications(self, client, ctx):
        """POST /entity/repairAllClassifications endpoint."""
        resp = client.post("/entity/repairAllClassifications", timeout=120)
        if resp.status_code == 500:
            body = resp.json() if resp.body else {}
            msg = body.get("errorMessage", "")
            if "tags_by_id" in msg:
                raise SkipTestError(
                    "repairAllClassifications requires Cassandra tags keyspace — "
                    f"not available: {msg[:120]}"
                )
        assert_status_in(resp, [200, 204, 400, 404])

    @test("repair_has_lineage", tags=["repair"], order=7)
    def test_repair_has_lineage(self, client, ctx):
        """POST /entity/repairhaslineage endpoint."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context"
        resp = client.post("/entity/repairhaslineage", json_data={
            "request": [{"assetGuid": guid}],
        }, timeout=120)
        assert_status_in(resp, [200, 204, 400, 404])

    @test("repair_nonexistent", tags=["repair", "negative"], order=8)
    def test_repair_nonexistent(self, client, ctx):
        """Repair with nonexistent qualifiedName — should not crash."""
        resp = client.post("/repair/single-index", params={
            "qualifiedName": "nonexistent/qn/that/does/not/exist/12345",
        })
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code in [400, 404]:
            body = resp.json()
            if isinstance(body, dict):
                assert any(k in body for k in ("errorMessage", "errorCode", "message", "error")), (
                    f"Expected error details for nonexistent QN, got keys: {list(body.keys())}"
                )

    @test("repair_composite_invalid", tags=["repair", "negative"], order=9)
    def test_repair_composite_invalid(self, client, ctx):
        """Repair composite index with invalid property name."""
        resp = client.post("/repair/composite-index", json_data={
            "propertyName": "nonExistentProperty_XYZ",
        })
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code in [400, 404]:
            body = resp.json()
            if isinstance(body, dict):
                assert any(k in body for k in ("errorMessage", "errorCode", "message", "error")), (
                    f"Expected error details for invalid property, got keys: {list(body.keys())}"
                )

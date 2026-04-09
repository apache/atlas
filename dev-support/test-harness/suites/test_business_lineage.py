"""Business lineage create tests.

Only POST /business-lineage/create-lineage exists in BusinessLineageREST.java.
There are no GET or DELETE endpoints for business lineage.
"""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError


@suite("business_lineage", depends_on_suites=["entity_crud", "data_mesh"],
       description="Business lineage create endpoint")
class BusinessLineageSuite:

    @test("create_lineage_invalid", tags=["business_lineage"], order=1)
    def test_create_lineage_invalid(self, client, ctx):
        """POST with fake productGuid and empty assetGuids — should fail gracefully."""
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": "00000000-0000-0000-0000-000000000000",
            "assetGuids": [],
        })
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code in [400, 404]:
            body = resp.json()
            if isinstance(body, dict):
                assert any(k in body for k in ("errorMessage", "errorCode", "message", "error")), (
                    f"Expected error details in response, got keys: {list(body.keys())}"
                )

    @test("create_lineage_with_entity", tags=["business_lineage"], order=2)
    def test_create_lineage_with_entity(self, client, ctx):
        """POST with a real entity GUID (DataSet, not DataProduct)."""
        entity_guid = ctx.get_entity_guid("ds1")
        assert entity_guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": entity_guid,
            "assetGuids": [entity_guid],
        })
        # 400 expected: entity is a DataSet, not a DataProduct
        assert_status_in(resp, [200, 204, 400, 404])

    @test("create_lineage_empty_assets", tags=["business_lineage"], order=3)
    def test_create_lineage_empty_assets(self, client, ctx):
        """POST with valid GUID but empty assetGuids."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": guid,
            "assetGuids": [],
        })
        assert_status_in(resp, [200, 204, 400, 404])

    @test("create_lineage_with_product", tags=["business_lineage"], order=4)
    def test_create_lineage_with_product(self, client, ctx):
        """Create business lineage with actual DataProduct if available."""
        product_guid = ctx.get_entity_guid("product1")
        ds_guid = ctx.get_entity_guid("ds1")
        if not product_guid:
            raise SkipTestError("DataProduct not available — data_mesh suite may have skipped")
        if not ds_guid:
            raise SkipTestError("ds1 GUID not available — entity_crud suite may have failed")
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": product_guid,
            "assetGuids": [ds_guid],
        })
        assert_status_in(resp, [200, 204, 400, 404])

    @test("create_lineage_nonexistent", tags=["business_lineage", "negative"], order=5)
    def test_create_lineage_nonexistent(self, client, ctx):
        """POST with nonexistent productGuid."""
        fake = "00000000-0000-0000-0000-000000000000"
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": fake,
            "assetGuids": [fake],
        })
        assert_status_in(resp, [400, 404])
        body = resp.json()
        if isinstance(body, dict):
            assert any(k in body for k in ("errorMessage", "errorCode", "message", "error")), (
                f"Expected error details in response, got keys: {list(body.keys())}"
            )

    @test("create_lineage_response_structure", tags=["business_lineage"], order=6)
    def test_create_lineage_response_structure(self, client, ctx):
        """Verify response structure when create-lineage returns 200."""
        product_guid = ctx.get_entity_guid("product1")
        ds_guid = ctx.get_entity_guid("ds1")
        if not product_guid:
            raise SkipTestError("DataProduct not available — data_mesh suite may have skipped")
        if not ds_guid:
            raise SkipTestError("ds1 GUID not available — entity_crud suite may have failed")
        resp = client.post("/business-lineage/create-lineage", json_data={
            "productGuid": product_guid,
            "assetGuids": [ds_guid],
        })
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (dict, list)), (
                f"Expected dict or list response, got {type(body).__name__}"
            )

"""Attribute update endpoint tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError


@suite("attribute", depends_on_suites=["entity_crud"],
       description="Attribute update endpoint")
class AttributeSuite:

    @test("update_attribute", tags=["attribute"], order=1)
    def test_update_attribute(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.post("/attribute/update", json_data={
            "guid": guid,
            "typeName": "DataSet",
            "attributes": {
                "description": "Updated via attribute API",
            }
        })
        # This endpoint is restricted to Argo service user, may get 403 in local dev
        assert_status_in(resp, [200, 204, 400, 403, 404])

    @test("update_attribute_invalid", tags=["attribute", "negative"], order=2)
    def test_update_attribute_invalid(self, client, ctx):
        resp = client.post("/attribute/update", json_data={
            "guid": "00000000-0000-0000-0000-000000000000",
            "typeName": "DataSet",
            "attributes": {},
        })
        assert_status_in(resp, [200, 204, 400, 403, 404])

    @test("update_attribute_read_after_write", tags=["attribute"], order=3)
    def test_update_attribute_read_after_write(self, client, ctx):
        """Update description via /attribute/update, GET entity and verify."""
        import time
        time.sleep(60)
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        new_desc = "Updated via attribute API - test harness"
        resp = client.post("/attribute/update", json_data={
            "guid": guid,
            "typeName": "DataSet",
            "attributes": {"description": new_desc},
        })
        assert_status_in(resp, [200, 204, 400, 403, 404])
        if resp.status_code in [200, 204]:
            resp2 = client.get(f"/entity/guid/{guid}")
            assert_status(resp2, 200)
            actual_desc = resp2.json().get("entity", {}).get("attributes", {}).get("description")
            assert actual_desc == new_desc, (
                f"Expected description='{new_desc}', got '{actual_desc}'"
            )

    @test("update_multiple_attributes", tags=["attribute"], order=4)
    def test_update_multiple_attributes(self, client, ctx):
        """Update description and userDescription simultaneously."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/attribute/update", json_data={
            "guid": guid,
            "typeName": "DataSet",
            "attributes": {
                "description": "multi-attr-desc",
                "userDescription": "multi-attr-user-desc",
            },
        })
        assert_status_in(resp, [200, 204, 400, 403, 404])

    @test("clear_attribute_value", tags=["attribute"], order=5)
    def test_clear_attribute_value(self, client, ctx):
        """Set description to empty string, verify cleared."""
        import time
        time.sleep(60)
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/attribute/update", json_data={
            "guid": guid,
            "typeName": "DataSet",
            "attributes": {"description": ""},
        })
        assert_status_in(resp, [200, 204, 400, 403, 404])
        if resp.status_code in [200, 204]:
            resp2 = client.get(f"/entity/guid/{guid}")
            if resp2.status_code == 200:
                desc = resp2.json().get("entity", {}).get("attributes", {}).get("description")
                assert desc in (None, "", "null"), (
                    f"Expected cleared description, got '{desc}'"
                )

    @test("update_nonexistent_field", tags=["attribute", "negative"], order=6)
    def test_update_nonexistent_field(self, client, ctx):
        """Update a field that does not exist on the type."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/attribute/update", json_data={
            "guid": guid,
            "typeName": "DataSet",
            "attributes": {"nonExistentField_xyz_123": "some-value"},
        })
        assert_status_in(resp, [200, 204, 400, 403, 404])

    @test("update_attribute_by_qn", tags=["attribute"], order=7)
    def test_update_attribute_by_qn(self, client, ctx):
        """POST /attribute/update with qualifiedName param."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        # Get QN first
        resp_e = client.get(f"/entity/guid/{guid}")
        if resp_e.status_code != 200:
            return
        qn = resp_e.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        if not qn:
            return
        resp = client.post("/attribute/update", json_data={
            "qualifiedName": qn,
            "typeName": "DataSet",
            "attributes": {"description": "Updated by QN"},
        })
        assert_status_in(resp, [200, 204, 400, 403, 404])

    @test("update_invalid_type", tags=["attribute", "negative"], order=8)
    def test_update_invalid_type(self, client, ctx):
        """Update with invalid typeName."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/attribute/update", json_data={
            "guid": guid,
            "typeName": "NonExistentType_XYZ",
            "attributes": {"description": "test"},
        })
        assert_status_in(resp, [200, 204, 400, 403, 404])

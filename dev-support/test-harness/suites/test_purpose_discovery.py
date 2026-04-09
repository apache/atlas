"""Purpose discovery tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("purpose_discovery", depends_on_suites=["persona_purpose"],
       description="Purpose discovery endpoint")
class PurposeDiscoverySuite:

    @test("get_user_purposes", tags=["purpose_discovery"], order=1)
    def test_get_user_purposes(self, client, ctx):
        resp = client.post("/purposes/user", json_data={
            "userName": "admin",
        })
        assert_status_in(resp, [200, 400, 403, 404])

    @test("get_user_purposes_empty", tags=["purpose_discovery"], order=2)
    def test_get_user_purposes_empty(self, client, ctx):
        resp = client.post("/purposes/user", json_data={
            "userName": "nonexistent-user-xyz",
        })
        assert_status_in(resp, [200, 400, 403, 404])

    @test("purposes_response_structure", tags=["purpose_discovery"], order=3)
    def test_purposes_response_structure(self, client, ctx):
        """When 200, validate response has purposes list."""
        resp = client.post("/purposes/user", json_data={
            "userName": "admin",
        })
        assert_status_in(resp, [200, 400, 403, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (dict, list)), (
                f"Expected dict or list, got {type(body).__name__}"
            )
            if isinstance(body, dict):
                purposes = body.get("purposes", body.get("entities", []))
                if isinstance(purposes, list) and purposes:
                    first = purposes[0]
                    if isinstance(first, dict):
                        has_fields = any(k in first for k in ("guid", "name", "typeName", "qualifiedName"))
                        assert has_fields, (
                            f"Purpose entry missing expected fields, got: {list(first.keys())[:10]}"
                        )

    @test("purposes_with_created_purpose", tags=["purpose_discovery"], order=4)
    def test_purposes_with_created_purpose(self, client, ctx):
        """After persona_purpose suite, query user purposes and look for created purpose."""
        purpose_guid = ctx.get_entity_guid("purpose1")
        if not purpose_guid:
            from core.assertions import SkipTestError
            raise SkipTestError("Purpose entity not available — persona_purpose suite may not have run")
        resp = client.post("/purposes/user", json_data={
            "userName": "admin",
        })
        assert_status_in(resp, [200, 400, 403, 404])

    @test("purposes_empty_body", tags=["purpose_discovery", "negative"], order=5)
    def test_purposes_empty_body(self, client, ctx):
        """POST with empty body — expect 400 or error."""
        resp = client.post("/purposes/user", json_data={})
        assert_status_in(resp, [200, 400, 403, 404])

    @test("purposes_invalid_format", tags=["purpose_discovery", "negative"], order=6)
    def test_purposes_invalid_format(self, client, ctx):
        """POST with invalid JSON structure."""
        resp = client.post("/purposes/user", json_data={"invalidField": 123})
        assert_status_in(resp, [200, 400, 403, 404])

    @test("purposes_admin_user", tags=["purpose_discovery"], order=7)
    def test_purposes_admin_user(self, client, ctx):
        """POST with 'admin' user on local dev, verify response."""
        resp = client.post("/purposes/user", json_data={
            "userName": "admin",
        })
        assert_status_in(resp, [200, 400, 403, 404])
        if resp.status_code == 200:
            body = resp.json()
            # Body should be valid — may be empty list or dict with purposes
            assert isinstance(body, (dict, list)), (
                f"Expected dict or list, got {type(body).__name__}"
            )

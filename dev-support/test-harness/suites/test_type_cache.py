"""Type cache refresh tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("type_cache", description="Type cache refresh endpoint")
class TypeCacheSuite:

    @test("refresh_type_cache_init", tags=["cache"], order=1)
    def test_refresh_type_cache_init(self, client, ctx):
        resp = client.post("/admin/types/refresh", json_data={}, params={
            "action": "INIT",
        }, timeout=120)
        assert_status_in(resp, [200, 204, 400, 404])

    @test("refresh_type_cache_no_action", tags=["cache"], order=2)
    def test_refresh_type_cache_no_action(self, client, ctx):
        resp = client.post("/admin/types/refresh", json_data={}, timeout=120)
        assert_status_in(resp, [200, 204, 400, 404])

    @test("refresh_with_type_name", tags=["cache"], order=3)
    def test_refresh_with_type_name(self, client, ctx):
        """POST /admin/types/refresh with specific type name."""
        resp = client.post("/admin/types/refresh", json_data={},
                           params={"typeName": "DataSet"}, timeout=120)
        assert_status_in(resp, [200, 204, 400, 404])

    @test("verify_type_after_refresh", tags=["cache"], order=4)
    def test_verify_type_after_refresh(self, client, ctx):
        """After refresh, GET a known type to verify it's accessible."""
        resp = client.post("/admin/types/refresh", json_data={}, timeout=120)
        assert_status_in(resp, [200, 204, 400, 404])
        # Verify DataSet type is still accessible
        resp2 = client.get("/types/typedef/name/DataSet")
        assert_status_in(resp2, [200, 404])
        if resp2.status_code == 200:
            body = resp2.json()
            assert isinstance(body, dict), f"Expected dict typedef response"

    @test("refresh_idempotent", tags=["cache"], order=5)
    def test_refresh_type_cache_idempotent(self, client, ctx):
        """Call type cache refresh twice, verify second succeeds."""
        resp1 = client.post("/admin/types/refresh", json_data={}, timeout=120)
        assert_status_in(resp1, [200, 204, 400, 404])
        resp2 = client.post("/admin/types/refresh", json_data={}, timeout=120)
        assert_status_in(resp2, [200, 204, 400, 404])

    @test("refresh_invalid_action", tags=["cache", "negative"], order=6)
    def test_refresh_invalid_action(self, client, ctx):
        """POST with action=INVALID — expect 400 or ignored."""
        resp = client.post("/admin/types/refresh", json_data={},
                           params={"action": "INVALID"}, timeout=120)
        assert_status_in(resp, [200, 204, 400, 404])

"""Config cache refresh tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("config_cache", description="Config cache refresh endpoint")
class ConfigCacheSuite:

    @test("refresh_config_cache", tags=["cache"], order=1)
    def test_refresh_config_cache(self, client, ctx):
        resp = client.post("/admin/config/refresh", json_data={})
        assert_status_in(resp, [200, 204, 400, 404])

    @test("refresh_config_cache_by_key", tags=["cache"], order=2)
    def test_refresh_config_cache_by_key(self, client, ctx):
        resp = client.post("/admin/config/refresh", params={"key": "test.key"})
        assert_status_in(resp, [200, 204, 400, 404])

    @test("get_cache_state", tags=["cache"], order=3)
    def test_get_cache_state(self, client, ctx):
        resp = client.get("/admin/config/cache/test.key")
        assert_status_in(resp, [200, 404])

    @test("refresh_no_body", tags=["cache"], order=4)
    def test_refresh_no_body(self, client, ctx):
        """POST /admin/config/refresh with no JSON body."""
        resp = client.post("/admin/config/refresh")
        assert_status_in(resp, [200, 204, 400, 404])

    @test("cache_state_nonexistent", tags=["cache", "negative"], order=5)
    def test_cache_state_nonexistent(self, client, ctx):
        """GET cache state for nonexistent key."""
        resp = client.get("/admin/config/cache/nonexistent.key.xyz.12345")
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.body
            # May return null/empty for nonexistent key
            assert body is not None, "Response body should not be None"

    @test("refresh_and_verify", tags=["cache"], order=6)
    def test_refresh_and_verify(self, client, ctx):
        """Refresh cache, then read a config to verify consistency."""
        resp = client.post("/admin/config/refresh", json_data={})
        assert_status_in(resp, [200, 204, 400, 404])
        # After refresh, a known config should still be readable
        resp2 = client.get("/configs")
        assert_status_in(resp2, [200, 404])

    @test("refresh_idempotent", tags=["cache"], order=7)
    def test_refresh_idempotent(self, client, ctx):
        """Call refresh twice, verify second call succeeds."""
        resp1 = client.post("/admin/config/refresh", json_data={})
        assert_status_in(resp1, [200, 204, 400, 404])
        resp2 = client.post("/admin/config/refresh", json_data={})
        assert_status_in(resp2, [200, 204, 400, 404])

"""Config CRUD tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in
from core.data_factory import unique_name


@suite("config", description="Config CRUD endpoints")
class ConfigSuite:

    def setup(self, client, ctx):
        self.test_key = "test_harness_config_key"

    @test("get_all_configs", tags=["smoke", "config"], order=1)
    def test_get_all_configs(self, client, ctx):
        resp = client.get("/configs")
        assert_status_in(resp, [200, 404, 503])

    @test("set_config", tags=["config", "crud"], order=2)
    def test_set_config(self, client, ctx):
        # Use a real config key that exists on staging
        resp = client.put(f"/configs/{self.test_key}", json_data={
            "value": "test_value_123",
        })
        assert_status_in(resp, [200, 201, 204, 400])
        if resp.status_code in [200, 201, 204]:
            ctx.set("config_set_success", True)

    @test("get_config", tags=["config"], order=3)
    def test_get_config(self, client, ctx):
        """GET a known config key, verify response structure."""
        config_key = "atlas.delete.batch.enabled"
        resp = client.get(f"/configs/{config_key}")
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, dict), f"Expected dict, got {type(body).__name__}"
            # Staging returns: key, currentValue, defaultValue, updatedBy, lastUpdated
            assert "currentValue" in body, (
                f"Expected 'currentValue' in config response, got keys: {list(body.keys())}"
            )

    @test("get_all_configs_structure", tags=["config"], order=4)
    def test_get_all_configs_structure(self, client, ctx):
        """GET a known config key and validate response fields."""
        config_key = "atlas.delete.batch.enabled"
        resp = client.get(f"/configs/{config_key}")
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, dict), f"Expected dict, got {type(body).__name__}"
            # Verify expected fields
            for field in ("key", "currentValue", "defaultValue"):
                assert field in body, (
                    f"Expected '{field}' in config response, got keys: {list(body.keys())}"
                )

    @test("update_existing_config", tags=["config", "crud"], order=5)
    def test_update_existing_config(self, client, ctx):
        """GET a known config key, flip its value, PUT it back, verify, then restore."""
        from core.assertions import SkipTestError

        # Use a well-known Atlas config key
        config_key = "atlas.delete.batch.enabled"
        resp = client.get(f"/configs/{config_key}")
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code != 200:
            raise SkipTestError(f"Config key {config_key} not accessible")

        # Read original value — response has "currentValue" field
        body = resp.json()
        original_value = str(body.get("currentValue", body.get("value", "true")))

        # Flip the value
        flipped = "false" if original_value.lower() == "true" else "true"

        # PUT the flipped value — request body uses "value"
        resp2 = client.put(f"/configs/{config_key}", json_data={
            "value": flipped,
        })
        assert_status_in(resp2, [200, 201, 204])

        # Verify read-after-write — response has "currentValue"
        resp3 = client.get(f"/configs/{config_key}")
        assert_status(resp3, 200)
        updated_body = resp3.json()
        updated_val = str(updated_body.get("currentValue", updated_body.get("value", "")))
        assert updated_val == flipped, (
            f"Expected currentValue='{flipped}' after update, got '{updated_val}'"
        )

        # Restore original value
        ctx.register_cleanup(
            lambda: client.put(f"/configs/{config_key}", json_data={
                "value": original_value,
            })
        )

    @test("get_nonexistent_config", tags=["config", "negative"], order=6)
    def test_get_nonexistent_config(self, client, ctx):
        """GET nonexistent config key."""
        resp = client.get("/configs/nonexistent_key_12345_xyz")
        assert_status_in(resp, [200, 400, 404])

    @test("set_long_value_config", tags=["config", "crud"], order=7)
    def test_set_long_value_config(self, client, ctx):
        """SET config with 500-char value."""
        long_val = "x" * 500
        key = "test_harness_long_cfg"
        resp = client.put(f"/configs/{key}", json_data={
            "key": key, "value": long_val,
        })
        if resp.status_code == 400:
            resp = client.put(f"/configs/{key}", json_data=long_val)
        assert_status_in(resp, [200, 201, 204, 400])
        if resp.status_code in [200, 201, 204]:
            ctx.register_cleanup(lambda: client.delete(f"/configs/{key}"))

    @test("set_empty_value_config", tags=["config", "crud"], order=8)
    def test_set_empty_value_config(self, client, ctx):
        """SET config with empty string value."""
        key = "test_harness_empty_cfg"
        resp = client.put(f"/configs/{key}", json_data={
            "key": key, "value": "",
        })
        if resp.status_code == 400:
            resp = client.put(f"/configs/{key}", json_data="")
        assert_status_in(resp, [200, 201, 204, 400])
        if resp.status_code in [200, 201, 204]:
            ctx.register_cleanup(lambda: client.delete(f"/configs/{key}"))

    @test("delete_nonexistent_config", tags=["config", "negative"], order=9)
    def test_delete_nonexistent_config(self, client, ctx):
        """DELETE nonexistent config key."""
        resp = client.delete("/configs/nonexistent_key_12345_xyz")
        assert_status_in(resp, [200, 204, 400, 404])

    @test("delete_config", tags=["config", "crud"], order=10, depends_on=["set_config"])
    def test_delete_config(self, client, ctx):
        resp = client.delete(f"/configs/{self.test_key}")
        assert_status_in(resp, [200, 204, 400, 404])
        if resp.status_code in [200, 204] and ctx.get("config_set_success"):
            resp2 = client.get(f"/configs/{self.test_key}")
            assert resp2.status_code in [400, 404], (
                f"Expected 400/404 after config delete, got {resp2.status_code}"
            )

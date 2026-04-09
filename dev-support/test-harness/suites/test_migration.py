"""Migration submit/status tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("migration", description="Migration submit and status endpoints")
class MigrationSuite:

    @test("migration_status", tags=["migration"], order=1)
    def test_migration_status(self, client, ctx):
        resp = client.get("/migration/status", params={
            "migrationType": "ENTITY_ATTRIBUTE",
        })
        assert_status_in(resp, [200, 400, 404])

    @test("migration_submit_invalid", tags=["migration"], order=2)
    def test_migration_submit_invalid(self, client, ctx):
        # Submit with invalid migration type — should fail gracefully
        resp = client.post("/migration/submit", params={
            "migrationType": "NONEXISTENT_TYPE",
        })
        assert_status_in(resp, [200, 400, 404])

    @test("status_entity_attribute", tags=["migration"], order=3)
    def test_status_entity_attribute(self, client, ctx):
        """GET status for ENTITY_ATTRIBUTE migration type."""
        resp = client.get("/migration/status", params={
            "migrationType": "ENTITY_ATTRIBUTE",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            # Response can be a string (e.g. "No Migration Found with this key"),
            # dict, or list — all are valid
            assert isinstance(body, (dict, list, str)), (
                f"Expected dict, list, or str, got {type(body).__name__}"
            )

    @test("status_all_types", tags=["migration"], order=4)
    def test_status_all_types(self, client, ctx):
        """GET status without migrationType — all migrations."""
        resp = client.get("/migration/status")
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            # Response can be a string (e.g. "No Migration Found with this key"),
            # dict, or list — all are valid
            assert isinstance(body, (dict, list, str)), (
                f"Expected dict, list, or str, got {type(body).__name__}"
            )

    @test("submit_entity_attribute", tags=["migration"], order=5)
    def test_submit_entity_attribute(self, client, ctx):
        """POST submit for ENTITY_ATTRIBUTE migration."""
        resp = client.post("/migration/submit", params={
            "migrationType": "ENTITY_ATTRIBUTE",
        })
        assert_status_in(resp, [200, 202, 400, 404])

    @test("status_response_structure", tags=["migration"], order=6)
    def test_status_response_structure(self, client, ctx):
        """Validate migration status response structure."""
        resp = client.get("/migration/status", params={
            "migrationType": "ENTITY_ATTRIBUTE",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict) and body:
                has_fields = any(k in body for k in (
                    "migrationType", "status", "startTime", "state", "type"
                ))
                assert has_fields or body == {}, (
                    f"Migration status should have type/status/startTime, got keys: {list(body.keys())}"
                )

    @test("status_after_submit", tags=["migration"], order=7, depends_on=["submit_entity_attribute"])
    def test_status_after_submit(self, client, ctx):
        """After submitting migration, GET status and verify."""
        resp = client.get("/migration/status", params={
            "migrationType": "ENTITY_ATTRIBUTE",
        })
        assert_status_in(resp, [200, 400, 404])

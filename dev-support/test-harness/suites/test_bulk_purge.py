"""Bulk purge lifecycle tests (slow, optional)."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, assert_field_present


@suite("bulk_purge", depends_on_suites=["entity_crud"],
       description="Bulk purge lifecycle (slow)")
class BulkPurgeSuite:

    @test("status_nonexistent", tags=["bulk_purge"], order=1)
    def test_status_nonexistent(self, client, ctx):
        resp = client.get("/bulk-purge/status", params={
            "requestId": "00000000-0000-0000-0000-000000000000",
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"

    @test("purge_invalid_prefix", tags=["bulk_purge"], order=2)
    def test_purge_invalid_prefix(self, client, ctx):
        # Prefix too short (< 10 chars) should fail validation.
        resp = client.post("/bulk-purge/qualifiedName", params={
            "prefix": "short",
        })
        assert_status_in(resp, [400, 422])
        body = resp.json()
        if isinstance(body, dict):
            assert "errorMessage" in body or "errorCode" in body or "message" in body or "error" in body, (
                f"Expected error details in response, got keys: {list(body.keys())}"
            )

    @test("system_status", tags=["bulk_purge"], order=3)
    def test_system_status(self, client, ctx):
        resp = client.get("/bulk-purge/system-status")
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"

    @test("cancel_nonexistent", tags=["bulk_purge"], order=4)
    def test_cancel_nonexistent(self, client, ctx):
        resp = client.delete("/bulk-purge/00000000-0000-0000-0000-000000000000")
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            assert resp.body is not None, "Expected non-empty cancel response"

    @test("purge_valid_prefix", tags=["bulk_purge"], order=5)
    def test_purge_valid_prefix(self, client, ctx):
        """POST /bulk-purge/qualifiedName with test-harness prefix (long enough)."""
        from core.data_factory import PREFIX
        resp = client.post("/bulk-purge/qualifiedName", params={
            "prefix": PREFIX,
            "dryRun": "true",
        })
        assert_status_in(resp, [200, 202, 400, 404])
        if resp.status_code in [200, 202]:
            body = resp.json()
            if isinstance(body, dict):
                request_id = body.get("requestId") or body.get("id")
                if request_id:
                    ctx.set("purge_request_id", request_id)

    @test("purge_status_structure", tags=["bulk_purge"], order=6, depends_on=["purge_valid_prefix"])
    def test_purge_status_structure(self, client, ctx):
        """When status returns 200, validate fields."""
        request_id = ctx.get("purge_request_id")
        if not request_id:
            # Try with any ID to check structure
            resp = client.get("/bulk-purge/status", params={
                "requestId": "00000000-0000-0000-0000-000000000000",
            })
        else:
            resp = client.get("/bulk-purge/status", params={
                "requestId": request_id,
            })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict) and body:
                has_fields = any(k in body for k in ("requestId", "status", "entityCount", "state"))
                assert has_fields, (
                    f"Purge status should have requestId/status/entityCount, got keys: {list(body.keys())}"
                )

    @test("system_status_structure", tags=["bulk_purge"], order=7)
    def test_system_status_structure(self, client, ctx):
        """When system-status returns 200, validate fields."""
        resp = client.get("/bulk-purge/system-status")
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict) and body:
                # Actual fields: activeCoordinators, queuedCoordinators,
                # maxCoordinators, activePurges, respondingPod, purges
                has_fields = any(k in body for k in (
                    "activeCoordinators", "queuedCoordinators", "maxCoordinators",
                    "activePurges", "respondingPod", "purges",
                    "activeRequests", "queueSize", "status",
                ))
                assert has_fields or body == {}, (
                    f"System status should have activeCoordinators/activePurges/purges, "
                    f"got keys: {list(body.keys())}"
                )

    @test("purge_by_connection_qn", tags=["bulk_purge"], order=8)
    def test_purge_by_connection_qn(self, client, ctx):
        """POST /bulk-purge/qualifiedName with connection-style QN prefix."""
        resp = client.post("/bulk-purge/qualifiedName", params={
            "prefix": "default/snowflake/1234567890/test-harness",
            "dryRun": "true",
        })
        assert_status_in(resp, [200, 202, 400, 404])
        if resp.status_code in [200, 202]:
            body = resp.json()
            assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"

    @test("purge_with_type_filter", tags=["bulk_purge"], order=9)
    def test_purge_with_type_filter(self, client, ctx):
        """POST /bulk-purge/qualifiedName with typeName parameter."""
        from core.data_factory import PREFIX
        resp = client.post("/bulk-purge/qualifiedName", params={
            "prefix": PREFIX,
            "typeName": "DataSet",
            "dryRun": "true",
        })
        assert_status_in(resp, [200, 202, 400, 404])
        if resp.status_code in [200, 202]:
            body = resp.json()
            assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"

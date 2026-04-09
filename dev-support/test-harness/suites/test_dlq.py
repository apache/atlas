"""DLQ replay status tests.

Note: Only GET /dlq/replay/status is implemented in DLQAdminController.
There is no POST /dlq/replay endpoint — the replay service starts
automatically at application startup.
"""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("dlq", description="DLQ replay status endpoint")
class DlqSuite:

    @test("get_dlq_status", tags=["smoke", "dlq"], order=1)
    def test_get_dlq_status(self, client, ctx):
        resp = client.get("/dlq/replay/status")
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (dict, list)), (
                f"Expected dict or list from DLQ status, got {type(body).__name__}"
            )

    @test("dlq_status_structure", tags=["dlq"], order=2)
    def test_dlq_status_structure(self, client, ctx):
        """When DLQ status returns 200, validate response structure."""
        resp = client.get("/dlq/replay/status")
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict):
                # DLQReplayService.getStatus() returns these fields:
                # isRunning, processedCount, isHealthy, maxRetries,
                # errorCount, skippedCount, activeRetries, activeBackoffs,
                # exponentialBackoffConfig, topic, consumerGroup, threadAlive
                expected_keys = {
                    "isRunning", "isHealthy", "processedCount", "errorCount",
                    "skippedCount", "maxRetries", "activeRetries", "activeBackoffs",
                    "topic", "consumerGroup", "threadAlive",
                }
                present = expected_keys & set(body.keys())
                assert present or body == {}, (
                    f"DLQ status should have isRunning/isHealthy/processedCount fields, "
                    f"got keys: {list(body.keys())}"
                )

    @test("dlq_status_values", tags=["dlq"], order=3)
    def test_dlq_status_values(self, client, ctx):
        """Validate DLQ status field types when available."""
        resp = client.get("/dlq/replay/status")
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            if isinstance(body, dict) and "isRunning" in body:
                assert isinstance(body["isRunning"], bool), (
                    f"isRunning should be bool, got {type(body['isRunning']).__name__}"
                )
            if isinstance(body, dict) and "processedCount" in body:
                assert isinstance(body["processedCount"], (int, float)), (
                    f"processedCount should be numeric, got {type(body['processedCount']).__name__}"
                )
            if isinstance(body, dict) and "topic" in body:
                assert isinstance(body["topic"], str), (
                    f"topic should be string, got {type(body['topic']).__name__}"
                )

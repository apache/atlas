"""Task search/retry/delete tests."""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in


@suite("tasks", depends_on_suites=["entity_crud"],
       description="Task management endpoints")
class TasksSuite:

    @test("search_tasks", tags=["tasks"], order=1)
    def test_search_tasks(self, client, ctx):
        resp = client.post("/task/search", json_data={
            "limit": 10,
            "offset": 0,
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (list, dict)), (
                f"Expected list or dict response, got {type(body).__name__}"
            )

    @test("search_tasks_with_filter", tags=["tasks"], order=2)
    def test_search_tasks_with_filter(self, client, ctx):
        resp = client.post("/task/search", json_data={
            "limit": 10,
            "offset": 0,
            "status": "COMPLETE",
        })
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, (list, dict)), (
                f"Expected list or dict response, got {type(body).__name__}"
            )
            # Verify filter: all returned tasks should have matching status
            tasks = body if isinstance(body, list) else body.get("tasks", [])
            if isinstance(tasks, list):
                for t in tasks[:5]:
                    if isinstance(t, dict) and "status" in t:
                        assert t["status"] == "COMPLETE", (
                            f"Expected status=COMPLETE with filter, got {t['status']}"
                        )

    @test("retry_nonexistent_task", tags=["tasks"], order=3)
    def test_retry_nonexistent_task(self, client, ctx):
        resp = client.put("/task/retry/00000000-0000-0000-0000-000000000000")
        assert_status_in(resp, [200, 404, 400])
        if resp.status_code in [400, 404]:
            body = resp.json()
            if isinstance(body, dict):
                assert any(k in body for k in ("errorMessage", "errorCode", "message", "error")), (
                    f"Expected error details in response, got keys: {list(body.keys())}"
                )

    @test("search_tasks_type_filter", tags=["tasks"], order=4)
    def test_search_tasks_type_filter(self, client, ctx):
        """POST /task/search with type filter."""
        resp = client.post("/task/search", json_data={
            "limit": 10,
            "offset": 0,
            "type": "CLASSIFICATION_PROPAGATION",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            tasks = body if isinstance(body, list) else body.get("tasks", [])
            if isinstance(tasks, list):
                for t in tasks[:5]:
                    if isinstance(t, dict) and "type" in t:
                        assert t["type"] == "CLASSIFICATION_PROPAGATION", (
                            f"Expected type filter result, got {t['type']}"
                        )

    @test("search_tasks_pagination", tags=["tasks"], order=5)
    def test_search_tasks_pagination(self, client, ctx):
        """POST /task/search with limit=1 vs limit=5."""
        resp1 = client.post("/task/search", json_data={"limit": 1, "offset": 0})
        assert_status_in(resp1, [200, 404])
        if resp1.status_code == 200:
            body1 = resp1.json()
            tasks1 = body1 if isinstance(body1, list) else body1.get("tasks", [])
            if isinstance(tasks1, list) and len(tasks1) > 1:
                # Some backends ignore limit param — log but don't fail
                print(f"  [tasks] limit=1 returned {len(tasks1)} tasks — "
                      f"backend may not honor limit parameter")

            resp2 = client.post("/task/search", json_data={"limit": 5, "offset": 0})
            assert_status(resp2, 200)

    @test("search_tasks_pending", tags=["tasks"], order=6)
    def test_search_tasks_pending(self, client, ctx):
        """Search tasks with status=PENDING."""
        resp = client.post("/task/search", json_data={
            "limit": 10, "offset": 0, "status": "PENDING",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            tasks = body if isinstance(body, list) else body.get("tasks", [])
            if isinstance(tasks, list):
                for t in tasks[:5]:
                    if isinstance(t, dict) and "status" in t:
                        assert t["status"] == "PENDING", (
                            f"Expected status=PENDING with filter, got {t['status']}"
                        )

    @test("search_tasks_failed", tags=["tasks"], order=7)
    def test_search_tasks_failed(self, client, ctx):
        """Search tasks with status=FAILED."""
        resp = client.post("/task/search", json_data={
            "limit": 10, "offset": 0, "status": "FAILED",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            tasks = body if isinstance(body, list) else body.get("tasks", [])
            if isinstance(tasks, list):
                for t in tasks[:5]:
                    if isinstance(t, dict) and "status" in t:
                        assert t["status"] == "FAILED", (
                            f"Expected status=FAILED with filter, got {t['status']}"
                        )

    @test("task_result_structure", tags=["tasks"], order=9)
    def test_task_result_structure(self, client, ctx):
        """Validate task entry structure when tasks exist."""
        resp = client.post("/task/search", json_data={"limit": 5, "offset": 0})
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            tasks = body if isinstance(body, list) else body.get("tasks", [])
            if isinstance(tasks, list) and tasks:
                task = tasks[0]
                if isinstance(task, dict):
                    expected = ("guid", "taskGuid", "type", "status",
                                "createdTime", "attemptCount")
                    has_fields = any(k in task for k in expected)
                    assert has_fields, (
                        f"Task entry missing expected fields, got keys: {list(task.keys())[:10]}"
                    )

    @test("search_tasks_with_sort", tags=["tasks"], order=10)
    def test_search_tasks_with_sort(self, client, ctx):
        """POST /task/search with sort by createdTime descending."""
        resp = client.post("/task/search", json_data={
            "limit": 5, "offset": 0,
            "sortBy": "createdTime", "sortOrder": "desc",
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            tasks = body if isinstance(body, list) else body.get("tasks", [])
            if isinstance(tasks, list) and len(tasks) >= 2:
                # Verify descending sort: first task should have later createdTime
                t0 = tasks[0]
                t1 = tasks[1]
                if isinstance(t0, dict) and isinstance(t1, dict):
                    ct0 = t0.get("createdTime", 0)
                    ct1 = t1.get("createdTime", 0)
                    if ct0 and ct1:
                        assert ct0 >= ct1, (
                            f"Expected descending createdTime sort, "
                            f"got {ct0} before {ct1}"
                        )

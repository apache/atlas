"""Entity evaluator endpoint tests.

Tests POST /entity/evaluator which evaluates whether the current user
has permission to perform specific actions on entities WITHOUT executing them.

API: POST /api/meta/entity/evaluator
Payload: List of AtlasEvaluatePolicyRequest objects
Response: List of AtlasEvaluatePolicyResponse objects with allowed=true/false

Verified against preprod:
  - Returns list of responses matching input order
  - allowed=true for permitted actions, allowed=false + errorCode for denied/invalid
  - Invalid action returns empty list (server silently ignores)
  - Nonexistent GUID returns allowed=false with ATLAS-404-00-005
  - Empty list returns empty list (200)
"""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError


@suite("entity_evaluator", depends_on_suites=["entity_crud"],
       description="Entity policy evaluator endpoint")
class EntityEvaluatorSuite:

    @test("evaluate_entity_read", tags=["evaluator"], order=1)
    def test_evaluate_entity_read(self, client, ctx):
        """POST /entity/evaluator — evaluate ENTITY_READ on ds1."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context"
        print(f"  [API] POST /entity/evaluator — action=ENTITY_READ, guid={guid}")
        resp = client.post("/entity/evaluator", json_data=[{
            "typeName": "DataSet",
            "entityGuid": guid,
            "action": "ENTITY_READ",
        }])
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list), f"Expected list response, got {type(body).__name__}"
        assert len(body) == 1, f"Expected 1 result, got {len(body)}"
        result = body[0]
        assert result.get("action") == "ENTITY_READ", (
            f"Expected action=ENTITY_READ, got {result.get('action')}"
        )
        assert result.get("entityGuid") == guid, (
            f"Expected entityGuid={guid}, got {result.get('entityGuid')}"
        )
        assert "allowed" in result, f"Response missing 'allowed' field, keys: {list(result.keys())}"
        # Argo service user should have read access
        assert result["allowed"] is True, (
            f"Expected allowed=true for ENTITY_READ as Argo user, got {result}"
        )

    @test("evaluate_entity_update", tags=["evaluator"], order=2)
    def test_evaluate_entity_update(self, client, ctx):
        """POST /entity/evaluator — evaluate ENTITY_UPDATE on ds1."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        print(f"  [API] POST /entity/evaluator — action=ENTITY_UPDATE, guid={guid}")
        resp = client.post("/entity/evaluator", json_data=[{
            "typeName": "DataSet",
            "entityGuid": guid,
            "action": "ENTITY_UPDATE",
        }])
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list) and len(body) == 1, f"Expected 1-element list, got {body}"
        assert "allowed" in body[0], f"Missing 'allowed' field"
        assert body[0]["allowed"] is True, (
            f"Expected allowed=true for ENTITY_UPDATE as Argo user"
        )

    @test("evaluate_entity_delete", tags=["evaluator"], order=3)
    def test_evaluate_entity_delete(self, client, ctx):
        """POST /entity/evaluator — evaluate ENTITY_DELETE on ds1."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/entity/evaluator", json_data=[{
            "typeName": "DataSet",
            "entityGuid": guid,
            "action": "ENTITY_DELETE",
        }])
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list) and len(body) == 1
        assert "allowed" in body[0]

    @test("evaluate_multiple_actions", tags=["evaluator"], order=4)
    def test_evaluate_multiple_actions(self, client, ctx):
        """POST /entity/evaluator — batch evaluate READ + UPDATE + DELETE."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        actions = ["ENTITY_READ", "ENTITY_UPDATE", "ENTITY_DELETE"]
        payload = [
            {"typeName": "DataSet", "entityGuid": guid, "action": action}
            for action in actions
        ]
        print(f"  [API] POST /entity/evaluator — {len(actions)} actions on guid={guid}")
        resp = client.post("/entity/evaluator", json_data=payload)
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list), f"Expected list, got {type(body).__name__}"
        assert len(body) == len(actions), (
            f"Expected {len(actions)} results, got {len(body)}"
        )
        returned_actions = [r.get("action") for r in body]
        for action in actions:
            assert action in returned_actions, (
                f"Action {action} missing from response: {returned_actions}"
            )

    @test("evaluate_nonexistent_guid", tags=["evaluator", "negative"], order=5)
    def test_evaluate_nonexistent_guid(self, client, ctx):
        """POST /entity/evaluator — nonexistent GUID returns allowed=false."""
        fake_guid = "00000000-0000-0000-0000-000000000000"
        resp = client.post("/entity/evaluator", json_data=[{
            "typeName": "DataSet",
            "entityGuid": fake_guid,
            "action": "ENTITY_READ",
        }])
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list) and len(body) == 1
        result = body[0]
        assert result.get("allowed") is False, (
            f"Expected allowed=false for nonexistent GUID, got {result}"
        )
        # Server returns ATLAS-404-00-005 errorCode
        if result.get("errorCode"):
            print(f"  [evaluator] Denied with errorCode={result['errorCode']}")

    @test("evaluate_invalid_action", tags=["evaluator", "negative"], order=6)
    def test_evaluate_invalid_action(self, client, ctx):
        """POST /entity/evaluator — invalid action returns empty list."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/entity/evaluator", json_data=[{
            "typeName": "DataSet",
            "entityGuid": guid,
            "action": "INVALID_ACTION_XYZ",
        }])
        # Server returns 200 with empty list for invalid actions
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list), f"Expected list, got {type(body).__name__}"
        assert len(body) == 0, (
            f"Expected empty list for invalid action, got {len(body)} results"
        )

    @test("evaluate_empty_list", tags=["evaluator", "negative"], order=7)
    def test_evaluate_empty_list(self, client, ctx):
        """POST /entity/evaluator — empty list returns empty list."""
        resp = client.post("/entity/evaluator", json_data=[])
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list) and len(body) == 0, (
            f"Expected empty list, got {body}"
        )

    @test("evaluate_add_classification", tags=["evaluator"], order=8)
    def test_evaluate_add_classification(self, client, ctx):
        """POST /entity/evaluator — evaluate ENTITY_ADD_CLASSIFICATION."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/entity/evaluator", json_data=[{
            "typeName": "DataSet",
            "entityGuid": guid,
            "action": "ENTITY_ADD_CLASSIFICATION",
            "classification": "Confidential",
        }])
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list)
        # May return 0 results if classification doesn't exist, or 1 with allowed true/false
        if body:
            assert "allowed" in body[0], f"Missing 'allowed' field in response"

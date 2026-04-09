"""Entity accessors endpoint tests.

Tests POST /entity/accessors which returns WHO (users, groups, roles)
can perform a specific action on an entity.

API: POST /api/meta/entity/accessors
Payload: List of AtlasAccessorRequest objects
Response: List of AtlasAccessorResponse objects with users/groups/roles sets

Verified against preprod:
  - Action format uses enum names: ENTITY_READ, ENTITY_UPDATE, etc. (NOT entity-read)
  - Returns list with users[], groups[], roles[], denyUsers[], denyGroups[], denyRoles[]
  - Invalid action returns 400 with ATLAS-400-00-029
  - Nonexistent GUID returns 400/404 with ATLAS-404-00-005
  - Empty list returns 400 ("Requires list of AtlasAccessor")
"""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError


@suite("entity_accessors", depends_on_suites=["entity_crud"],
       description="Entity accessors (who-can-access) endpoint")
class EntityAccessorsSuite:

    @test("accessors_entity_read", tags=["accessors"], order=1)
    def test_accessors_entity_read(self, client, ctx):
        """POST /entity/accessors — who can ENTITY_READ on ds1."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context"
        print(f"  [API] POST /entity/accessors — action=ENTITY_READ, guid={guid}")
        resp = client.post("/entity/accessors", json_data=[{
            "guid": guid,
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
        assert result.get("guid") == guid, (
            f"Expected guid={guid}, got {result.get('guid')}"
        )
        # Must have accessor sets
        for field in ("users", "groups", "roles"):
            assert field in result, f"Response missing '{field}' field"
            assert isinstance(result[field], list), f"'{field}' should be list"
        # Argo service user should be in users list
        users = result.get("users", [])
        assert len(users) > 0, "Expected at least 1 user with ENTITY_READ access"
        print(f"  [accessors] ENTITY_READ: {len(users)} users, "
              f"{len(result.get('groups', []))} groups, "
              f"{len(result.get('roles', []))} roles")

    @test("accessors_entity_update", tags=["accessors"], order=2)
    def test_accessors_entity_update(self, client, ctx):
        """POST /entity/accessors — who can ENTITY_UPDATE on ds1."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/entity/accessors", json_data=[{
            "guid": guid,
            "action": "ENTITY_UPDATE",
        }])
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list) and len(body) == 1
        result = body[0]
        assert "users" in result and isinstance(result["users"], list)
        # Deny sets should also be present
        for deny_field in ("denyUsers", "denyGroups", "denyRoles"):
            assert deny_field in result, f"Response missing '{deny_field}' field"
            assert isinstance(result[deny_field], list), f"'{deny_field}' should be list"

    @test("accessors_multiple_actions", tags=["accessors"], order=3)
    def test_accessors_multiple_actions(self, client, ctx):
        """POST /entity/accessors — batch query READ + UPDATE."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/entity/accessors", json_data=[
            {"guid": guid, "action": "ENTITY_READ"},
            {"guid": guid, "action": "ENTITY_UPDATE"},
        ])
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list) and len(body) == 2, (
            f"Expected 2 results, got {len(body) if isinstance(body, list) else body}"
        )
        actions = [r.get("action") for r in body]
        assert "ENTITY_READ" in actions, f"ENTITY_READ missing from {actions}"
        assert "ENTITY_UPDATE" in actions, f"ENTITY_UPDATE missing from {actions}"

    @test("accessors_with_qualified_name", tags=["accessors"], order=4)
    def test_accessors_with_qualified_name(self, client, ctx):
        """POST /entity/accessors — query by qualifiedName + typeName."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        # Get QN
        resp_e = client.get(f"/entity/guid/{guid}")
        if resp_e.status_code != 200:
            raise SkipTestError("Could not fetch ds1 entity")
        qn = resp_e.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        if not qn:
            raise SkipTestError("ds1 qualifiedName not available")
        resp = client.post("/entity/accessors", json_data=[{
            "qualifiedName": qn,
            "typeName": "DataSet",
            "action": "ENTITY_READ",
        }])
        assert_status_in(resp, [200, 400])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, list) and len(body) >= 1
            assert "users" in body[0]

    @test("accessors_nonexistent_guid", tags=["accessors", "negative"], order=5)
    def test_accessors_nonexistent_guid(self, client, ctx):
        """POST /entity/accessors — nonexistent GUID returns 400/404."""
        fake_guid = "00000000-0000-0000-0000-000000000000"
        resp = client.post("/entity/accessors", json_data=[{
            "guid": fake_guid,
            "action": "ENTITY_READ",
        }])
        # Server returns 400 with ATLAS-404-00-005 (instance not found)
        assert_status_in(resp, [400, 404])
        body = resp.json()
        if isinstance(body, dict):
            assert "errorCode" in body or "errorMessage" in body, (
                f"Expected error details, got keys: {list(body.keys())}"
            )
            print(f"  [accessors] Nonexistent GUID error: {body.get('errorCode')} — "
                  f"{body.get('errorMessage', '')[:80]}")

    @test("accessors_invalid_action", tags=["accessors", "negative"], order=6)
    def test_accessors_invalid_action(self, client, ctx):
        """POST /entity/accessors — invalid action returns 400."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/entity/accessors", json_data=[{
            "guid": guid,
            "action": "INVALID_ACTION_XYZ",
        }])
        # Server returns 400 with ATLAS-400-00-029 ("Invalid action provided")
        assert_status(resp, 400)
        body = resp.json()
        assert isinstance(body, dict)
        assert body.get("errorCode") == "ATLAS-400-00-029", (
            f"Expected ATLAS-400-00-029, got {body.get('errorCode')}"
        )

    @test("accessors_empty_list", tags=["accessors", "negative"], order=7)
    def test_accessors_empty_list(self, client, ctx):
        """POST /entity/accessors — empty list returns 400."""
        resp = client.post("/entity/accessors", json_data=[])
        # Server returns 400 ("Requires list of AtlasAccessor")
        assert_status(resp, 400)

    @test("accessors_entity_delete", tags=["accessors"], order=8)
    def test_accessors_entity_delete(self, client, ctx):
        """POST /entity/accessors — who can ENTITY_DELETE on ds1."""
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found"
        resp = client.post("/entity/accessors", json_data=[{
            "guid": guid,
            "action": "ENTITY_DELETE",
        }])
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list) and len(body) == 1
        result = body[0]
        assert "users" in result
        print(f"  [accessors] ENTITY_DELETE: {len(result.get('users', []))} users allowed")

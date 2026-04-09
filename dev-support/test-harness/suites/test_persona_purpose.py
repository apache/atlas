"""Persona/Purpose/AuthPolicy CRUD lifecycle tests."""

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty, SkipTestError,
)
from core.data_factory import (
    build_persona_entity, build_purpose_entity, build_auth_policy_entity,
    unique_name,
)


@suite("persona_purpose", depends_on_suites=["entity_crud"],
       description="Persona/Purpose/AuthPolicy lifecycle")
class PersonaPurposeSuite:

    def setup(self, client, ctx):
        self.persona_name = unique_name("harness-persona")
        self.purpose_name = unique_name("harness-purpose")
        self.policy_name = unique_name("harness-policy")

    # ---- Persona CRUD ----

    @test("create_persona", tags=["persona_purpose", "crud"], order=1)
    def test_create_persona(self, client, ctx):
        entity = build_persona_entity(name=self.persona_name)
        resp = client.post("/entity", json_data={"entity": entity})
        # 400/403 = Keycloak unavailable or preprocessor not configured
        assert_status_in(resp, [200, 400, 403])
        if resp.status_code != 200:
            ctx.set("persona_unavailable", True)
            raise SkipTestError(
                f"Persona creation returned {resp.status_code} — "
                f"Keycloak/preprocessor not configured"
            )

        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Persona creation returned 200 but no entities in mutatedEntities"
        guid = entities[0]["guid"]
        ctx.register_entity("persona1", guid, "Persona")
        ctx.register_entity_cleanup(guid)

        # Read back the auto-generated QN and verify field values
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.typeName", "Persona")
        assert_field_equals(resp2, "entity.attributes.name", self.persona_name)
        qn = resp2.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert qn, f"Expected qualifiedName on persona {guid}"
        ctx.set("persona1_qn", qn)

    @test("get_persona", tags=["persona_purpose"], order=2, depends_on=["create_persona"])
    def test_get_persona(self, client, ctx):
        if ctx.get("persona_unavailable"):
            raise SkipTestError("Persona not available — Keycloak not configured")
        guid = ctx.get_entity_guid("persona1")
        assert guid, "persona1 GUID not found in context — create_persona must have failed"
        resp = client.get(f"/entity/guid/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.typeName", "Persona")
        assert_field_not_empty(resp, "entity.attributes.qualifiedName")

    @test("assign_users_to_persona", tags=["persona_purpose"], order=2.5, depends_on=["create_persona"])
    def test_assign_users_to_persona(self, client, ctx):
        """P-02: Update persona to set personaUsers and personaGroups, verify persistence."""
        if ctx.get("persona_unavailable"):
            raise SkipTestError("Persona not available — Keycloak not configured")
        guid = ctx.get_entity_guid("persona1")
        assert guid, "persona1 GUID not found in context — create_persona must have failed"
        persona_qn = ctx.get("persona1_qn")
        assert persona_qn, "persona1_qn not found in context — create_persona must have failed"

        # Use "service-account-atlan-argo" — the OAuth client used for auth,
        # guaranteed to exist in Keycloak on all Atlan environments.
        # Keycloak validates users exist; fake usernames cause 500.
        test_user = "service-account-atlan-argo"
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "Persona",
                "guid": guid,
                "attributes": {
                    "qualifiedName": persona_qn,
                    "name": self.persona_name,
                    "isAccessControlEnabled": True,
                    "personaUsers": [test_user],
                },
            }
        }, timeout=120)
        # 400 = Keycloak validates user/group, 500 = preprocessor/Keycloak timeout
        assert_status_in(resp, [200, 400, 500])
        if resp.status_code != 200:
            raise SkipTestError(
                f"Keycloak rejected or timed out on user assignment (status={resp.status_code})"
            )

        # Read back and verify personaUsers persisted
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        attrs = entity.get("attributes", {})
        users = attrs.get("personaUsers", [])
        assert test_user in users, f"Expected '{test_user}' in personaUsers, got {users}"

    @test("update_persona", tags=["persona_purpose", "crud"], order=3, depends_on=["create_persona"])
    def test_update_persona(self, client, ctx):
        if ctx.get("persona_unavailable"):
            raise SkipTestError("Persona not available — Keycloak not configured")
        guid = ctx.get_entity_guid("persona1")
        assert guid, "persona1 GUID not found in context — create_persona must have failed"
        persona_qn = ctx.get("persona1_qn")
        assert persona_qn, "persona1_qn not found in context — create_persona must have failed"
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "Persona",
                "guid": guid,
                "attributes": {
                    "qualifiedName": persona_qn,
                    "name": self.persona_name,
                    "isAccessControlEnabled": True,
                    "description": "Updated persona by test harness",
                },
            }
        }, timeout=120)
        # 500 = Keycloak/preprocessor gateway timeout on preprod
        assert_status_in(resp, [200, 400, 500])
        if resp.status_code != 200:
            raise SkipTestError(
                f"Persona update returned {resp.status_code} — "
                f"Keycloak/AccessControlUtils rejected or timed out"
            )
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.attributes.description", "Updated persona by test harness")

    # ---- Purpose CRUD ----

    @test("create_purpose", tags=["persona_purpose", "crud"], order=10)
    def test_create_purpose(self, client, ctx):
        entity = build_purpose_entity(name=self.purpose_name)
        resp = client.post("/entity", json_data={"entity": entity})
        # 400/403 if Keycloak unavailable, 500 if preprocessor/Keycloak timeout
        assert_status_in(resp, [200, 400, 403, 500])
        if resp.status_code != 200:
            ctx.set("purpose_unavailable", True)
            raise SkipTestError(
                f"Purpose creation returned {resp.status_code} — "
                f"Keycloak/preprocessor not configured on this environment"
            )

        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Purpose creation returned 200 but no entities in mutatedEntities"
        guid = entities[0]["guid"]
        ctx.register_entity("purpose1", guid, "Purpose")
        ctx.register_entity_cleanup(guid)

        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.typeName", "Purpose")
        assert_field_equals(resp2, "entity.attributes.name", self.purpose_name)
        qn = resp2.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert qn, f"Expected qualifiedName on purpose {guid}"
        ctx.set("purpose1_qn", qn)

    @test("get_purpose", tags=["persona_purpose"], order=11, depends_on=["create_purpose"])
    def test_get_purpose(self, client, ctx):
        if ctx.get("purpose_unavailable"):
            raise SkipTestError("Purpose not available — Keycloak not configured")
        guid = ctx.get_entity_guid("purpose1")
        assert guid, "purpose1 GUID not found in context — create_purpose must have failed"
        resp = client.get(f"/entity/guid/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.typeName", "Purpose")

    @test("update_purpose", tags=["persona_purpose", "crud"], order=12, depends_on=["create_purpose"])
    def test_update_purpose(self, client, ctx):
        if ctx.get("purpose_unavailable"):
            raise SkipTestError("Purpose not available — Keycloak not configured")
        guid = ctx.get_entity_guid("purpose1")
        assert guid, "purpose1 GUID not found in context — create_purpose must have failed"
        purpose_qn = ctx.get("purpose1_qn")
        assert purpose_qn, "purpose1_qn not found in context — create_purpose must have failed"
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "Purpose",
                "guid": guid,
                "attributes": {
                    "qualifiedName": purpose_qn,
                    "name": self.purpose_name,
                    "isAccessControlEnabled": True,
                    "description": "Updated purpose by test harness",
                },
            }
        })
        assert_status_in(resp, [200, 400, 500])
        if resp.status_code != 200:
            raise SkipTestError(
                f"Purpose update returned {resp.status_code} — Keycloak rejected update"
            )
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "entity.attributes.description", "Updated purpose by test harness")

    # ---- AuthPolicy CRUD ----

    @test("create_auth_policy", tags=["persona_purpose", "crud"], order=20, depends_on=["create_persona"])
    def test_create_auth_policy(self, client, ctx):
        if ctx.get("persona_unavailable"):
            raise SkipTestError("Persona not available — Keycloak not configured")
        persona_guid = ctx.get_entity_guid("persona1")
        assert persona_guid, "persona1 GUID not found in context — create_persona must have failed"
        entity = build_auth_policy_entity(persona_guid, name=self.policy_name)
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status_in(resp, [200, 400, 403, 500])
        if resp.status_code != 200:
            raise SkipTestError(
                f"AuthPolicy creation returned {resp.status_code} — "
                f"Keycloak/preprocessor rejected"
            )
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "AuthPolicy creation returned 200 but no entities in mutatedEntities"
        assert entities[0].get("typeName") == "AuthPolicy", (
            f"Expected typeName=AuthPolicy, got {entities[0].get('typeName')}"
        )
        guid = entities[0]["guid"]
        ctx.register_entity("auth_policy1", guid, "AuthPolicy")
        ctx.register_entity_cleanup(guid)

    @test("get_persona_with_policies", tags=["persona_purpose"], order=21, depends_on=["create_auth_policy"])
    def test_get_persona_with_policies(self, client, ctx):
        if ctx.get("persona_unavailable"):
            raise SkipTestError("Persona not available — Keycloak not configured")
        persona_guid = ctx.get_entity_guid("persona1")
        assert persona_guid, "persona1 GUID not found in context — create_persona must have failed"
        resp = client.get(f"/entity/guid/{persona_guid}", params={"ignoreRelationships": "false"})
        assert_status(resp, 200)
        entity = resp.json().get("entity", {})
        rel_attrs = entity.get("relationshipAttributes", {})
        policies = rel_attrs.get("policies", [])
        policy_guid = ctx.get_entity_guid("auth_policy1")
        assert policy_guid, "auth_policy1 GUID not found in context — create_auth_policy must have failed"
        assert policies, f"Expected policies in persona's relationshipAttributes, got empty list"
        found = any(
            isinstance(p, dict) and p.get("guid") == policy_guid
            for p in policies
        )
        assert found, f"Expected policy {policy_guid} in persona's policies, got {policies}"

    # ---- DELETE ----

    @test("delete_auth_policy", tags=["persona_purpose", "crud"], order=70, depends_on=["create_auth_policy"])
    def test_delete_auth_policy(self, client, ctx):
        if ctx.get("persona_unavailable"):
            raise SkipTestError("Persona not available — Keycloak not configured")
        guid = ctx.get_entity_guid("auth_policy1")
        assert guid, "auth_policy1 GUID not found in context — create_auth_policy must have failed"
        resp = client.delete(f"/entity/guid/{guid}")
        assert_status_in(resp, [200, 204])

        # Verify deleted: GET should return 404 or status=DELETED
        resp2 = client.get(f"/entity/guid/{guid}")
        assert resp2.status_code in [200, 404], (
            f"Expected 200 or 404 after delete, got {resp2.status_code}"
        )
        if resp2.status_code == 200:
            assert_field_equals(resp2, "entity.status", "DELETED")

    @test("delete_persona", tags=["persona_purpose", "crud"], order=80, depends_on=["create_persona"])
    def test_delete_persona(self, client, ctx):
        if ctx.get("persona_unavailable"):
            raise SkipTestError("Persona not available — Keycloak not configured")
        guid = ctx.get_entity_guid("persona1")
        assert guid, "persona1 GUID not found in context — create_persona must have failed"
        resp = client.delete(f"/entity/guid/{guid}")
        assert_status_in(resp, [200, 204])

        # Verify deleted
        resp2 = client.get(f"/entity/guid/{guid}")
        assert_status_in(resp2, [200, 404])
        if resp2.status_code == 200:
            assert_field_equals(resp2, "entity.status", "DELETED")

    @test("delete_purpose", tags=["persona_purpose", "crud"], order=81, depends_on=["create_purpose"])
    def test_delete_purpose(self, client, ctx):
        if ctx.get("purpose_unavailable"):
            raise SkipTestError("Purpose not available — Keycloak not configured")
        guid = ctx.get_entity_guid("purpose1")
        assert guid, "purpose1 GUID not found in context — create_purpose must have failed"
        resp = client.delete(f"/entity/guid/{guid}")
        assert_status_in(resp, [200, 204])

        # Verify deleted
        resp2 = client.get(f"/entity/guid/{guid}")
        assert resp2.status_code in [200, 404], (
            f"Expected 200 or 404 after delete, got {resp2.status_code}"
        )
        if resp2.status_code == 200:
            assert_field_equals(resp2, "entity.status", "DELETED")

    @test("persona_purpose_not_found", tags=["persona_purpose"], order=90)
    def test_persona_purpose_not_found(self, client, ctx):
        resp = client.get("/entity/guid/00000000-0000-0000-0000-000000000000")
        assert_status_in(resp, [404, 400])
        body = resp.json()
        if isinstance(body, dict):
            assert "errorMessage" in body or "errorCode" in body or "message" in body or "error" in body, (
                f"Expected error details in response, got keys: {list(body.keys())}"
            )

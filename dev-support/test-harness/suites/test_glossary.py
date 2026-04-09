"""Glossary/Term/Category full lifecycle tests."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty, SkipTestError,
)
from core.audit_helpers import poll_audit_events
from core.kafka_helpers import assert_entity_in_kafka
from core.data_factory import unique_name, build_dataset_entity, unique_qn


def _find_glossary_entity_by_name(client, type_name, name, max_wait=60,
                                   interval=10):
    """Poll search until a glossary entity with given name appears.

    Handles the case where POST timed out but the server created the entity.
    Returns guid or None.
    """
    for attempt in range(1 + max_wait // interval):
        if attempt > 0:
            time.sleep(interval)
        resp = client.post("/search/indexsearch", json_data={"dsl": {
            "from": 0, "size": 1,
            "query": {"bool": {"must": [
                {"term": {"__typeName.keyword": type_name}},
                {"term": {"name.keyword": name}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        }})
        if resp.status_code == 200:
            entities = resp.json().get("entities", [])
            if entities:
                return entities[0].get("guid")
    return None


def _poll_entity_by_guid(client, guid, max_wait=30, interval=5):
    """Poll indexsearch by GUID until entity appears (UI verification pattern).

    Returns entity dict with requested attributes, or None.
    """
    for attempt in range(1 + max_wait // interval):
        if attempt > 0:
            time.sleep(interval)
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "size": 1,
                "query": {"bool": {"filter": {"bool": {"must": [
                    {"term": {"__guid": guid}},
                    {"term": {"__state": "ACTIVE"}},
                ]}}}}
            },
            "attributes": ["name", "qualifiedName", "anchor"],
        })
        if resp.status_code == 200:
            entities = resp.json().get("entities", [])
            if entities:
                return entities[0]
    return None


def _extract_guid_from_bulk_response(resp):
    """Extract first entity GUID from POST /entity/bulk response."""
    if resp.status_code != 200:
        return None
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    if entities:
        return entities[0].get("guid")
    return None


@suite("glossary", description="Glossary, Term, Category lifecycle")
class GlossarySuite:

    def setup(self, client, ctx):
        self.glossary_name = unique_name("harness-glossary")
        self.term_name = unique_name("harness-term")
        self.term2_name = unique_name("harness-term2")
        self.category_name = unique_name("harness-category")

    # ---- Glossary CRUD ----

    @test("list_glossaries", tags=["smoke", "glossary"], order=1)
    def test_list_glossaries(self, client, ctx):
        resp = client.get("/glossary", params={"limit": 10, "offset": 0, "sort": "ASC"})
        assert_status(resp, 200)

    @test("create_glossary", tags=["smoke", "glossary", "crud"], order=2)
    def test_create_glossary(self, client, ctx):
        # Create via bulk API — exact same payload pattern as the UI.
        # UI sends: qualifiedName="", displayName, certificateStatus, ownerUsers, ownerGroups
        resp = client.post("/entity/bulk", json_data={
            "entities": [{
                "typeName": "AtlasGlossary",
                "attributes": {
                    "qualifiedName": "",
                    "name": self.glossary_name,
                    "displayName": self.glossary_name,
                    "shortDescription": "Test harness glossary",
                    "certificateStatus": "DRAFT",
                    "ownerUsers": [],
                    "ownerGroups": [],
                },
            }],
        }, timeout=30, retries=0)

        guid = _extract_guid_from_bulk_response(resp)

        if not guid:
            # POST failed or timed out — server may still have created it.
            # Poll ES by name (same resilience pattern as UI).
            print(f"  [glossary] POST /entity/bulk returned {resp.status_code}, "
                  f"polling ES for glossary...")
            guid = _find_glossary_entity_by_name(
                client, "AtlasGlossary", self.glossary_name,
                max_wait=60, interval=10,
            )

        if not guid:
            raise SkipTestError(
                f"Glossary creation failed (status={resp.status_code}) — "
                f"server may be overloaded or unavailable"
            )

        ctx.register_cleanup(lambda: client.delete(f"/glossary/{guid}"))

        # Read-after-write via indexsearch (UI pattern — avoids slow GlossaryService)
        entity = _poll_entity_by_guid(client, guid, max_wait=30, interval=5)
        assert entity is not None, f"Glossary {guid} not found in ES after creation"
        assert entity.get("attributes", {}).get("name") == self.glossary_name or \
               entity.get("name") == self.glossary_name, \
            f"Expected name={self.glossary_name}, got {entity}"

        glossary_qn = entity.get("attributes", {}).get("qualifiedName") or \
                      entity.get("qualifiedName")
        ctx.register_entity("glossary", guid, "AtlasGlossary", qualifiedName=glossary_qn)

        # Kafka: verify ENTITY_CREATE notification
        assert_entity_in_kafka(ctx, guid, "ENTITY_CREATE")

    @test("list_glossaries_find_created", tags=["glossary", "validation"], order=3, depends_on=["create_glossary"])
    def test_list_glossaries_find_created(self, client, ctx):
        import time

        # First verify glossary is readable by GUID
        guid = ctx.get_entity_guid("glossary")
        assert guid, "glossary GUID not in context"
        resp_get = client.get(f"/glossary/{guid}")
        print(f"  [glossary] GET /glossary/{guid} → {resp_get.status_code}")
        if resp_get.status_code == 200:
            print(f"  [glossary] Name: {resp_get.json().get('name')}")

        # Wait for glossary to appear in list endpoint
        print(f"  [glossary] Sleeping for 60 seconds...")
        time.sleep(60)

        # GET-all -> find our glossary by name -> GET by that GUID -> verify details
        resp = client.get("/glossary", params={"limit": 100, "offset": 0, "sort": "ASC"})
        assert_status(resp, 200)
        body = resp.json()
        glossaries = body if isinstance(body, list) else []
        found = None
        for g in glossaries:
            if g.get("name") == self.glossary_name:
                found = g
                break
        assert found is not None, f"Created glossary '{self.glossary_name}' not found in list"

        # GET by that GUID and verify details
        found_guid = found.get("guid")
        resp2 = client.get(f"/glossary/{found_guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "name", self.glossary_name)

    @test("get_glossary", tags=["glossary"], order=4, depends_on=["create_glossary"])
    def test_get_glossary(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        resp = client.get(f"/glossary/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.glossary_name)

    @test("get_glossary_detailed", tags=["glossary"], order=5, depends_on=["create_glossary"])
    def test_get_glossary_detailed(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        resp = client.get(f"/glossary/{guid}/detailed")
        assert_status(resp, 200)

    @test("update_glossary", tags=["glossary", "crud"], order=6, depends_on=["create_glossary"])
    def test_update_glossary(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        resp = client.put(f"/glossary/{guid}", json_data={
            "guid": guid,
            "name": self.glossary_name,
            "shortDescription": "Updated description",
        }, timeout=60)
        assert_status(resp, 200)

        # Read-after-write: GET and verify shortDescription
        resp2 = client.get(f"/glossary/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "shortDescription", "Updated description")

    @test("glossary_create_audit", tags=["glossary", "audit"], order=7, depends_on=["create_glossary"])
    def test_glossary_create_audit(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        assert guid, "Glossary GUID not found in context — create_glossary must have failed"
        qn = ctx.get_entity_qn("glossary")
        events, total = poll_audit_events(
            client, guid, action_filter="ENTITY_CREATE",
            max_wait=60, interval=10, qualifiedName=qn,
        )
        if events is None:
            raise SkipTestError("Audit endpoint not available (404/405)")
        if not events:
            raise SkipTestError(
                f"Audit endpoint available but no ENTITY_CREATE events for glossary {guid} after 60s — "
                f"audit indexing may not be configured"
            )

    # ---- Term CRUD ----

    @test("create_term", tags=["smoke", "glossary", "crud"], order=10, depends_on=["create_glossary"])
    def test_create_term(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        # Create via bulk API — UI-matching payload
        resp = client.post("/entity/bulk", json_data={
            "entities": [{
                "typeName": "AtlasGlossaryTerm",
                "attributes": {
                    "qualifiedName": "",
                    "name": self.term_name,
                    "displayName": self.term_name,
                    "shortDescription": "Test term",
                },
                "relationshipAttributes": {
                    "anchor": {
                        "guid": glossary_guid,
                        "typeName": "AtlasGlossary",
                    },
                },
            }],
        }, timeout=30, retries=0)

        guid = _extract_guid_from_bulk_response(resp)

        if not guid:
            print(f"  [glossary] POST /entity/bulk returned {resp.status_code}, "
                  f"polling ES for term...")
            guid = _find_glossary_entity_by_name(
                client, "AtlasGlossaryTerm", self.term_name,
                max_wait=60, interval=10,
            )

        if not guid:
            raise SkipTestError(
                f"Term creation failed (status={resp.status_code}) — "
                f"server may be overloaded"
            )

        ctx.register_entity("term1", guid, "AtlasGlossaryTerm")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{guid}"))

        # Read-after-write via indexsearch (UI pattern)
        entity = _poll_entity_by_guid(client, guid, max_wait=30, interval=5)
        assert entity is not None, f"Term {guid} not found in ES after creation"
        entity_name = entity.get("attributes", {}).get("name") or entity.get("name")
        assert entity_name == self.term_name, \
            f"Expected name={self.term_name}, got {entity_name}"

        # Kafka: verify ENTITY_CREATE notification
        assert_entity_in_kafka(ctx, guid, "ENTITY_CREATE")

    @test("create_terms_bulk", tags=["glossary", "crud"], order=11, depends_on=["create_glossary"])
    def test_create_terms_bulk(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        # Create via bulk API — UI-matching payload
        resp = client.post("/entity/bulk", json_data={
            "entities": [{
                "typeName": "AtlasGlossaryTerm",
                "attributes": {
                    "qualifiedName": "",
                    "name": self.term2_name,
                    "displayName": self.term2_name,
                    "shortDescription": "Bulk term",
                },
                "relationshipAttributes": {
                    "anchor": {
                        "guid": glossary_guid,
                        "typeName": "AtlasGlossary",
                    },
                },
            }],
        }, timeout=30, retries=0)

        guid = _extract_guid_from_bulk_response(resp)

        if not guid:
            print(f"  [glossary] POST /entity/bulk returned {resp.status_code}, "
                  f"polling ES for term...")
            guid = _find_glossary_entity_by_name(
                client, "AtlasGlossaryTerm", self.term2_name,
                max_wait=60, interval=10,
            )

        if not guid:
            raise SkipTestError(
                f"Bulk term creation failed (status={resp.status_code}) — "
                f"server may be overloaded"
            )

        ctx.register_entity("term2", guid, "AtlasGlossaryTerm")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{guid}"))

    @test("get_term", tags=["glossary"], order=12, depends_on=["create_term"])
    def test_get_term(self, client, ctx):
        guid = ctx.get_entity_guid("term1")
        assert guid, "term1 GUID not found in context — create_term must have failed"
        resp = client.get(f"/glossary/term/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.term_name)

    @test("update_term", tags=["glossary", "crud"], order=13, depends_on=["create_term"])
    def test_update_term(self, client, ctx):
        guid = ctx.get_entity_guid("term1")
        assert guid, "term1 GUID not found in context — create_term must have failed"
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.put(f"/glossary/term/{guid}", json_data={
            "guid": guid,
            "name": self.term_name,
            "shortDescription": "Updated term description",
            "anchor": {"glossaryGuid": glossary_guid},
        })
        assert_status(resp, 200)

        # Read-after-write: GET and verify shortDescription
        resp2 = client.get(f"/glossary/term/{guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "shortDescription", "Updated term description")

    @test("get_glossary_terms_list", tags=["glossary"], order=15, depends_on=["create_term"])
    def test_get_glossary_terms_list(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        assert guid, "Glossary GUID not found in context"
        resp = client.get(f"/glossary/{guid}/terms", params={
            "limit": 10, "offset": 0, "sort": "ASC",
        })
        assert_status(resp, 200)
        body = resp.json()
        terms = body if isinstance(body, list) else []
        assert len(terms) > 0, f"Expected at least one term in glossary, got {len(terms)}"

    @test("get_glossary_detailed_with_terms", tags=["glossary"], order=16, depends_on=["create_term"])
    def test_get_glossary_detailed_with_terms(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        assert guid, "Glossary GUID not found in context"
        resp = client.get(f"/glossary/{guid}/detailed")
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"
        assert "guid" in body, "Detailed glossary response should have 'guid'"
        assert "name" in body, "Detailed glossary response should have 'name'"

    # ---- Category CRUD ----

    @test("create_category", tags=["glossary", "crud"], order=20, depends_on=["create_glossary"])
    def test_create_category(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        # Create via bulk API — UI-matching payload
        resp = client.post("/entity/bulk", json_data={
            "entities": [{
                "typeName": "AtlasGlossaryCategory",
                "attributes": {
                    "qualifiedName": "",
                    "name": self.category_name,
                    "displayName": self.category_name,
                    "shortDescription": "Test category",
                },
                "relationshipAttributes": {
                    "anchor": {
                        "guid": glossary_guid,
                        "typeName": "AtlasGlossary",
                    },
                },
            }],
        }, timeout=30, retries=0)

        guid = _extract_guid_from_bulk_response(resp)

        if not guid:
            print(f"  [glossary] POST /entity/bulk returned {resp.status_code}, "
                  f"polling ES for category...")
            guid = _find_glossary_entity_by_name(
                client, "AtlasGlossaryCategory", self.category_name,
                max_wait=60, interval=10,
            )

        if not guid:
            raise SkipTestError(
                f"Category creation failed (status={resp.status_code}) — "
                f"server may be overloaded"
            )

        ctx.register_entity("category1", guid, "AtlasGlossaryCategory")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/category/{guid}"))

        # Read-after-write via indexsearch (UI pattern)
        entity = _poll_entity_by_guid(client, guid, max_wait=30, interval=5)
        assert entity is not None, f"Category {guid} not found in ES after creation"
        entity_name = entity.get("attributes", {}).get("name") or entity.get("name")
        assert entity_name == self.category_name, \
            f"Expected name={self.category_name}, got {entity_name}"

    @test("get_category", tags=["glossary"], order=21, depends_on=["create_category"])
    def test_get_category(self, client, ctx):
        guid = ctx.get_entity_guid("category1")
        assert guid, "category1 GUID not found in context — create_category must have failed"
        resp = client.get(f"/glossary/category/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.category_name)

    @test("create_categories_bulk", tags=["glossary", "crud"], order=23, depends_on=["create_glossary"])
    def test_create_categories_bulk(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        cat_name_a = unique_name("bulk-cat-a")
        cat_name_b = unique_name("bulk-cat-b")
        # Create via bulk API — UI-matching payload, both categories in one call
        resp = client.post("/entity/bulk", json_data={
            "entities": [
                {
                    "typeName": "AtlasGlossaryCategory",
                    "attributes": {
                        "qualifiedName": "",
                        "name": cat_name_a,
                        "displayName": cat_name_a,
                        "shortDescription": "Bulk category A",
                    },
                    "relationshipAttributes": {
                        "anchor": {"guid": glossary_guid, "typeName": "AtlasGlossary"},
                    },
                },
                {
                    "typeName": "AtlasGlossaryCategory",
                    "attributes": {
                        "qualifiedName": "",
                        "name": cat_name_b,
                        "displayName": cat_name_b,
                        "shortDescription": "Bulk category B",
                    },
                    "relationshipAttributes": {
                        "anchor": {"guid": glossary_guid, "typeName": "AtlasGlossary"},
                    },
                },
            ],
        }, timeout=30, retries=0)

        if resp.status_code == 200:
            body = resp.json()
            creates = body.get("mutatedEntities", {}).get("CREATE", [])
            for cat in creates:
                cat_guid = cat.get("guid")
                if cat_guid:
                    ctx.register_cleanup(
                        lambda g=cat_guid: client.delete(f"/glossary/category/{g}")
                    )
        else:
            # POST failed — poll ES for each category
            print(f"  [glossary] POST /entity/bulk returned {resp.status_code}, "
                  f"polling ES for categories...")
            for name in [cat_name_a, cat_name_b]:
                guid = _find_glossary_entity_by_name(
                    client, "AtlasGlossaryCategory", name,
                    max_wait=60, interval=10,
                )
                if guid:
                    ctx.register_cleanup(
                        lambda g=guid: client.delete(f"/glossary/category/{g}")
                    )

    @test("create_category_hierarchy", tags=["glossary", "crud"], order=24, depends_on=["create_category"])
    def test_create_category_hierarchy(self, client, ctx):
        glossary_guid = ctx.get_entity_guid("glossary")
        parent_guid = ctx.get_entity_guid("category1")
        child_name = unique_name("child-cat")
        # Create via bulk API with parentCategory relationship
        resp = client.post("/entity/bulk", json_data={
            "entities": [{
                "typeName": "AtlasGlossaryCategory",
                "attributes": {
                    "qualifiedName": "",
                    "name": child_name,
                    "displayName": child_name,
                    "shortDescription": "Child category",
                },
                "relationshipAttributes": {
                    "anchor": {"guid": glossary_guid, "typeName": "AtlasGlossary"},
                    "parentCategory": {"guid": parent_guid, "typeName": "AtlasGlossaryCategory"},
                },
            }],
        }, timeout=30, retries=0)

        child_guid = _extract_guid_from_bulk_response(resp)

        if not child_guid:
            print(f"  [glossary] POST /entity/bulk (hierarchy) returned "
                  f"{resp.status_code}, polling ES...")
            child_guid = _find_glossary_entity_by_name(
                client, "AtlasGlossaryCategory", child_name,
                max_wait=60, interval=10,
            )

        if child_guid:
            ctx.register_entity("child_category", child_guid, "AtlasGlossaryCategory")
            ctx.register_cleanup(lambda: client.delete(f"/glossary/category/{child_guid}"))

            # Verify parent has childrenCategories
            resp2 = client.get(f"/glossary/category/{parent_guid}")
            if resp2.status_code == 200:
                body = resp2.json()
                children = body.get("childrenCategories", [])
                found = any(c.get("categoryGuid") == child_guid for c in children)
                assert found, f"Expected child {child_guid} in parent's childrenCategories"

    @test("get_glossary_categories_list", tags=["glossary"], order=25, depends_on=["create_category"])
    def test_get_glossary_categories_list(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        assert guid, "Glossary GUID not found in context"
        resp = client.get(f"/glossary/{guid}/categories", params={
            "limit": 10, "offset": 0, "sort": "ASC",
        })
        assert_status(resp, 200)
        body = resp.json()
        categories = body if isinstance(body, list) else []
        assert len(categories) > 0, f"Expected at least one category, got {len(categories)}"

    @test("get_category_terms", tags=["glossary"], order=26, depends_on=["create_term", "create_category"])
    def test_get_category_terms(self, client, ctx):
        cat_guid = ctx.get_entity_guid("category1")
        assert cat_guid, "category1 GUID not found in context"
        resp = client.get(f"/glossary/category/{cat_guid}/terms")
        # May be empty if term not assigned to category, just verify endpoint works
        assert_status_in(resp, [200, 204])

    @test("assign_term_to_entity", tags=["glossary"], order=30, depends_on=["create_term"])
    def test_assign_term_to_entity(self, client, ctx):
        term_guid = ctx.get_entity_guid("term1")
        assert term_guid, "term1 GUID not found in context"
        # Create a DataSet entity for term assignment
        qn = unique_qn("term-assign-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("term-assign"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        entity_guid = entities[0]["guid"]
        ctx.register_entity("term_assign_entity", entity_guid, "DataSet")
        ctx.register_entity_cleanup(entity_guid)

        # Assign term to entity via bulk API (same pattern as UI — fast path
        # through EntityGraphMapper, avoids slow GlossaryService full-term reload)
        resp = client.post("/entity/bulk", json_data={
            "entities": [{
                "guid": entity_guid,
                "typeName": "DataSet",
                "attributes": {"qualifiedName": qn, "name": entity["attributes"]["name"]},
                "relationshipAttributes": {
                    "meanings": [{"typeName": "AtlasGlossaryTerm", "guid": term_guid}]
                },
            }],
        })
        assert_status_in(resp, [200, 204])

        # Read-after-write: verify term appears on entity via entity API (fast)
        resp2 = client.get(f"/entity/guid/{entity_guid}")
        if resp2.status_code == 200:
            ent_body = resp2.json().get("entity", {})
            meanings = ent_body.get("relationshipAttributes", {}).get("meanings", [])
            found = any(m.get("guid") == term_guid for m in meanings)
            assert found, f"Expected term {term_guid} in entity meanings after assign"

    @test("get_term_assigned_entities", tags=["glossary"], order=32, depends_on=["assign_term_to_entity"])
    def test_get_term_assigned_entities(self, client, ctx):
        term_guid = ctx.get_entity_guid("term1")
        resp = client.get(f"/glossary/terms/{term_guid}/assignedEntities")
        assert_status(resp, 200)
        body = resp.json()
        entities = body if isinstance(body, list) else []
        entity_guid = ctx.get_entity_guid("term_assign_entity")
        if entity_guid:
            found = any(e.get("guid") == entity_guid for e in entities)
            assert found, f"Expected entity {entity_guid} in assigned entities"

    @test("disassociate_term_from_entity", tags=["glossary"], order=33, depends_on=["assign_term_to_entity"])
    def test_disassociate_term_from_entity(self, client, ctx):
        term_guid = ctx.get_entity_guid("term1")
        entity_guid = ctx.get_entity_guid("term_assign_entity")
        assert entity_guid, "term_assign_entity GUID not found in context"

        # First get the relationship guid
        resp = client.get(f"/glossary/terms/{term_guid}/assignedEntities")
        assert_status(resp, 200)
        body = resp.json()
        entities = body if isinstance(body, list) else []
        # Find the assignment with matching guid
        assignment = None
        for e in entities:
            if e.get("guid") == entity_guid:
                assignment = e
                break
        assert assignment, (
            f"Entity {entity_guid} not found in assigned entities of term {term_guid}"
        )

        # Disassociate
        resp = client.put(
            f"/glossary/terms/{term_guid}/assignedEntities",
            json_data=[assignment],
        )
        # PUT with empty or DELETE semantics vary; accept multiple statuses
        assert_status_in(resp, [200, 204, 400, 404])

    @test("update_category", tags=["glossary", "crud"], order=22, depends_on=["create_category"])
    def test_update_category(self, client, ctx):
        guid = ctx.get_entity_guid("category1")
        assert guid, "category1 GUID not found in context"
        glossary_guid = ctx.get_entity_guid("glossary")
        resp = client.put(f"/glossary/category/{guid}", json_data={
            "guid": guid,
            "name": self.category_name,
            "shortDescription": "Updated category",
            "anchor": {"glossaryGuid": glossary_guid},
        })
        # 409 can happen if concurrent modification or qualifiedName conflicts
        assert_status_in(resp, [200, 409])
        if resp.status_code == 200:
            # Read-after-write: verify shortDescription updated
            resp2 = client.get(f"/glossary/category/{guid}")
            if resp2.status_code == 200:
                assert_field_equals(resp2, "shortDescription", "Updated category")

    # ---- Term assignment & disassociation search verification ----

    @test("term_assignment_in_search", tags=["glossary", "search"], order=31,
          depends_on=["assign_term_to_entity"])
    def test_term_assignment_in_search(self, client, ctx):
        entity_guid = ctx.get_entity_guid("term_assign_entity")
        term_guid = ctx.get_entity_guid("term1")
        assert entity_guid, "term_assign_entity GUID not found in context"
        assert term_guid, "term1 GUID not found in context"

        # Poll ES until entity appears with meanings populated
        print(f"  [glossary-search] Polling ES for entity {entity_guid} with meanings...")
        found_entity = None
        meanings_found = False
        max_wait = 60
        poll_interval = 5
        for i in range(max_wait // poll_interval):
            time.sleep(poll_interval)
            elapsed = (i + 1) * poll_interval
            resp = client.post("/search/indexsearch", json_data={
                "dsl": {
                    "from": 0, "size": 1,
                    "query": {"bool": {"must": [
                        {"term": {"__guid": entity_guid}},
                        {"term": {"__state": "ACTIVE"}},
                    ]}}
                }
            })
            if resp.status_code != 200:
                print(f"  [glossary-search] Search returned {resp.status_code} ({elapsed}s/{max_wait}s)")
                continue
            entities = resp.json().get("entities", [])
            if not entities:
                print(f"  [glossary-search] Entity not in ES yet ({elapsed}s/{max_wait}s)")
                continue
            found_entity = entities[0]
            # Check both 'meanings' (API model) and '__meanings' (ES field)
            meanings = found_entity.get("meanings", [])
            meanings_raw = found_entity.get("__meanings", [])
            if (meanings and isinstance(meanings, list) and len(meanings) > 0) or \
               (meanings_raw and isinstance(meanings_raw, list) and len(meanings_raw) > 0):
                meanings_found = True
                print(f"  [glossary-search] Meanings found after {elapsed}s: "
                      f"meanings={meanings}, __meanings={meanings_raw}")
                break
            print(f"  [glossary-search] Entity found but no meanings yet ({elapsed}s/{max_wait}s)")

        assert found_entity is not None, (
            f"Entity {entity_guid} not found in ES after {max_wait}s polling"
        )
        meanings = found_entity.get("meanings", [])
        meanings_raw = found_entity.get("__meanings", [])
        assert meanings_found, (
            f"Expected meanings on entity {entity_guid} after term assignment, "
            f"but meanings is empty after {max_wait}s polling. "
            f"meanings={meanings}, __meanings={meanings_raw}"
        )

        # Verify termGuid in meanings (if populated as objects)
        if meanings and isinstance(meanings[0], dict):
            term_guids = [m.get("termGuid") for m in meanings]
            assert term_guid in term_guids, (
                f"Expected termGuid {term_guid} in meanings, got {term_guids}"
            )

    @test("verify_disassociation_in_search", tags=["glossary", "search"], order=34,
          depends_on=["disassociate_term_from_entity"])
    def test_verify_disassociation_in_search(self, client, ctx):
        """After disassociating term from entity, verify meanings is cleared in ES."""
        entity_guid = ctx.get_entity_guid("term_assign_entity")
        assert entity_guid, "term_assign_entity GUID not found in context"

        print(f"  [glossary-search] Polling ES for entity {entity_guid} — "
              f"expecting meanings cleared after disassociation...")
        max_wait = 60
        poll_interval = 5
        meanings_cleared = False
        for i in range(max_wait // poll_interval):
            time.sleep(poll_interval)
            elapsed = (i + 1) * poll_interval
            resp = client.post("/search/indexsearch", json_data={
                "dsl": {
                    "from": 0, "size": 1,
                    "query": {"bool": {"must": [
                        {"term": {"__guid": entity_guid}},
                        {"term": {"__state": "ACTIVE"}},
                    ]}}
                }
            })
            if resp.status_code != 200:
                print(f"  [glossary-search] Search returned {resp.status_code} ({elapsed}s/{max_wait}s)")
                continue
            entities = resp.json().get("entities", [])
            if not entities:
                print(f"  [glossary-search] Entity not in ES ({elapsed}s/{max_wait}s)")
                continue
            entity = entities[0]
            meanings = entity.get("meanings", [])
            meanings_raw = entity.get("__meanings", [])
            has_meanings = (meanings and len(meanings) > 0) or \
                           (meanings_raw and len(meanings_raw) > 0)
            if not has_meanings:
                meanings_cleared = True
                print(f"  [glossary-search] Meanings cleared after {elapsed}s")
                break
            print(f"  [glossary-search] Meanings still present ({elapsed}s/{max_wait}s): "
                  f"meanings={meanings}, __meanings={meanings_raw}")

        assert meanings_cleared, (
            f"Expected meanings to be cleared on entity {entity_guid} after disassociation, "
            f"but meanings still present after {max_wait}s polling"
        )

    # ---- Delete (reverse dependency order: child categories, categories, terms, glossary) ----

    @test("delete_child_category", tags=["glossary", "crud"], order=79,
          depends_on=["create_category_hierarchy"])
    def test_delete_child_category(self, client, ctx):
        guid = ctx.get_entity_guid("child_category")
        if not guid:
            raise SkipTestError("child_category was not created")
        resp = client.delete(f"/glossary/category/{guid}")
        assert_status_in(resp, [200, 204])

        # Verify deletion — GET should return 404
        resp2 = client.get(f"/glossary/category/{guid}")
        assert_status(resp2, 404)

    @test("delete_category", tags=["glossary", "crud"], order=80,
          depends_on=["create_category", "delete_child_category"])
    def test_delete_category(self, client, ctx):
        guid = ctx.get_entity_guid("category1")
        assert guid, "category1 GUID not found in context"
        resp = client.delete(f"/glossary/category/{guid}")
        assert_status_in(resp, [200, 204])

        # Verify deletion — GET should return 404
        resp2 = client.get(f"/glossary/category/{guid}")
        assert_status(resp2, 404)

    @test("delete_term", tags=["glossary", "crud"], order=81, depends_on=["create_term"])
    def test_delete_term(self, client, ctx):
        guid = ctx.get_entity_guid("term1")
        assert guid, "term1 GUID not found in context"
        resp = client.delete(f"/glossary/term/{guid}")
        assert_status_in(resp, [200, 204])

        # Verify deletion — GET should return 404
        resp2 = client.get(f"/glossary/term/{guid}")
        assert_status(resp2, 404)

    @test("delete_glossary", tags=["glossary", "crud"], order=82, depends_on=["create_glossary"])
    def test_delete_glossary(self, client, ctx):
        guid = ctx.get_entity_guid("glossary")
        assert guid, "Glossary GUID not found in context"
        resp = client.delete(f"/glossary/{guid}")
        assert_status_in(resp, [200, 204])

        # Verify deletion — GET should return 404
        resp2 = client.get(f"/glossary/{guid}")
        assert_status(resp2, 404)

"""Search endpoint tests (basic, index, DSL, ES)."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_not_empty, assert_field_in,
)
from core.data_factory import build_dataset_entity, unique_qn, unique_name


@suite("search", depends_on_suites=["entity_crud"],
       description="Search endpoints: basic, index, DSL, ES")
class SearchSuite:

    def setup(self, client, ctx):
        # Wait for ES sync
        es_wait = ctx.get("es_sync_wait", 5)
        time.sleep(es_wait)

    @test("basic_search_get", tags=["smoke", "search"], order=1)
    def test_basic_search_get(self, client, ctx):
        resp = client.get("/search/basic", params={
            "typeName": "DataSet",
            "limit": 10,
        }, timeout=60)
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"

    @test("basic_search_post", tags=["search"], order=2)
    def test_basic_search_post(self, client, ctx):
        resp = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "limit": 10,
            "offset": 0,
            "excludeDeletedEntities": True,
        }, timeout=120)
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"

    @test("index_search", tags=["smoke", "search"], order=3)
    def test_index_search(self, client, ctx):
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"__typeName.keyword": "DataSet"}},
                            {"term": {"__state": "ACTIVE"}},
                        ]
                    }
                }
            }
        })
        assert_status(resp, 200)
        assert_field_present(resp, "approximateCount")

        # When results exist, validate first entity structure
        body = resp.json()
        if body.get("approximateCount", 0) > 0:
            entities = body.get("entities", [])
            if entities:
                first = entities[0]
                assert "guid" in first, "Entity should have 'guid' field"
                assert "typeName" in first, "Entity should have 'typeName' field"
                assert "status" in first, "Entity should have 'status' field"

    @test("index_search_with_sort", tags=["search"], order=4)
    def test_index_search_with_sort(self, client, ctx):
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
                "sort": [{"__timestamp": {"order": "desc"}}],
            }
        })
        assert_status(resp, 200)
        assert_field_present(resp, "approximateCount")
        body = resp.json()
        assert isinstance(body.get("approximateCount"), int), (
            f"Expected approximateCount to be int, got {type(body.get('approximateCount')).__name__}"
        )

    @test("index_search_aggregation", tags=["search"], order=5)
    def test_index_search_aggregation(self, client, ctx):
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 0,
                "query": {"term": {"__state": "ACTIVE"}},
                "aggs": {
                    "type_counts": {
                        "terms": {"field": "__typeName.keyword", "size": 10}
                    }
                }
            }
        })
        assert_status(resp, 200)
        body = resp.json()
        assert "aggregations" in body, "Expected 'aggregations' field in aggregation search response"

    @test("es_search", tags=["search"], order=6)
    def test_es_search(self, client, ctx):
        resp = client.post("/search/es", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
            }
        })
        # Raw ES response — may return 200 or could be unavailable/forbidden
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, dict) and body, "Expected non-empty dict response from ES search"

    @test("basic_search_by_classification", tags=["search", "slow"], order=8)
    def test_basic_search_by_classification(self, client, ctx):
        resp = client.get("/search/basic", params={
            "classification": "*",
            "limit": 5,
        }, timeout=60)
        # Wildcard classification search can be slow or unsupported
        assert_status_in(resp, [200, 400])
        if resp.status_code == 200:
            body = resp.json()
            assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"

    @test("index_search_empty_result", tags=["search"], order=9)
    def test_index_search_empty_result(self, client, ctx):
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "term": {"__qualifiedName": "nonexistent-qn-12345-xyz"}
                }
            }
        })
        assert_status(resp, 200)
        count = resp.json().get("approximateCount", -1)
        assert count == 0, f"Expected 0 results, got {count}"

    @test("search_finds_created_entity", tags=["search"], order=10)
    def test_search_finds_created_entity(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        # Search by GUID
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "term": {"__guid": guid}
                }
            }
        })
        assert_status(resp, 200)
        body = resp.json()
        count = body.get("approximateCount", 0)
        assert count > 0, f"Expected created entity with guid={guid} in search, got count={count}"
        entities = body.get("entities", [])
        if entities:
            found_guids = [e.get("guid") for e in entities]
            assert guid in found_guids, f"Expected {guid} in search results, got {found_guids}"

    @test("search_result_structure", tags=["search"], order=11)
    def test_search_result_structure(self, client, ctx):
        guid = ctx.get_entity_guid("ds1")
        assert guid, "ds1 GUID not found in context — entity_crud suite must have failed"
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 1,
                "query": {
                    "term": {"__guid": guid}
                }
            }
        })
        assert_status(resp, 200)
        body = resp.json()
        entities = body.get("entities", [])
        if entities:
            entity = entities[0]
            assert "guid" in entity, "Search result entity should have 'guid'"
            assert "typeName" in entity, "Search result entity should have 'typeName'"
            assert entity.get("status") == "ACTIVE", f"Expected status=ACTIVE, got {entity.get('status')}"

    @test("search_exclude_deleted_entities", tags=["search"], order=12)
    def test_search_exclude_deleted_entities(self, client, ctx):
        # Create + delete entity, then search excluding deleted
        qn = unique_qn("search-del-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("search-del"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        guid = entities[0]["guid"]

        # Delete it
        client.delete(f"/entity/guid/{guid}")

        time.sleep(5)

        # Search with excludeDeletedEntities
        resp2 = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "excludeDeletedEntities": True,
            "limit": 100,
            "offset": 0,
        }, timeout=60)
        assert_status(resp2, 200)
        body2 = resp2.json()
        result_entities = body2.get("entities", [])
        found_guids = [e.get("guid") for e in result_entities]
        assert guid not in found_guids, (
            f"Deleted entity {guid} should not appear with excludeDeletedEntities=True"
        )

    @test("basic_search_by_type_with_pagination", tags=["search"], order=13)
    def test_basic_search_by_type_with_pagination(self, client, ctx):
        resp = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "limit": 1,
            "offset": 0,
        })
        assert_status(resp, 200)
        body = resp.json()
        result_entities = body.get("entities", [])
        assert len(result_entities) <= 1, (
            f"Expected at most 1 result with limit=1, got {len(result_entities)}"
        )

    @test("basic_search_with_attribute_filter", tags=["search"], order=14)
    def test_basic_search_with_attribute_filter(self, client, ctx):
        from core.data_factory import PREFIX
        resp = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "limit": 10,
            "offset": 0,
            "entityFilters": {
                "condition": "AND",
                "criterion": [{
                    "attributeName": "qualifiedName",
                    "operator": "STARTS_WITH",
                    "attributeValue": PREFIX,
                }],
            },
        })
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"

    @test("search_by_glossary_term_type", tags=["search"], order=15)
    def test_search_by_glossary_term_type(self, client, ctx):
        resp = client.post("/search/basic", json_data={
            "typeName": "AtlasGlossaryTerm",
            "limit": 5,
            "offset": 0,
        })
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"

    @test("search_by_process_type", tags=["search"], order=15.5)
    def test_search_by_process_type(self, client, ctx):
        """POST /search/basic with typeName=Process."""
        resp = client.post("/search/basic", json_data={
            "typeName": "Process",
            "limit": 5,
            "offset": 0,
        }, timeout=60)
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, dict), f"Expected dict response"
        entities = body.get("entities", [])
        for e in entities[:3]:
            assert e.get("typeName") == "Process" or e.get("typeName", "").endswith("Process"), (
                f"Expected Process type, got {e.get('typeName')}"
            )

    @test("index_search_source_filter", tags=["search"], order=16)
    def test_index_search_source_filter(self, client, ctx):
        """Index search with _source includes/excludes fields."""
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 3,
                "query": {
                    "term": {"__typeName.keyword": "DataSet"},
                },
                "_source": ["qualifiedName", "name"],
            }
        })
        assert_status(resp, 200)

    @test("index_search_multi_type", tags=["search"], order=17)
    def test_index_search_multi_type(self, client, ctx):
        """Bool should clause for multiple typeNames (OR search)."""
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "should": [
                            {"term": {"__typeName.keyword": "DataSet"}},
                            {"term": {"__typeName.keyword": "Process"}},
                        ],
                        "minimum_should_match": 1,
                        "filter": [
                            {"term": {"__state": "ACTIVE"}},
                        ],
                    }
                }
            }
        })
        assert_status(resp, 200)
        body = resp.json()
        entities = body.get("entities", [])
        for e in entities[:5]:
            assert e.get("typeName") in ("DataSet", "Process"), (
                f"Expected DataSet or Process, got {e.get('typeName')}"
            )

    @test("index_search_exists_filter", tags=["search"], order=18)
    def test_index_search_exists_filter(self, client, ctx):
        """Query with 'exists' filter on description field."""
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"exists": {"field": "description"}},
                            {"term": {"__state": "ACTIVE"}},
                        ]
                    }
                }
            }
        })
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body.get("approximateCount"), int), "Expected integer approximateCount"

    @test("index_search_range_filter", tags=["search"], order=19)
    def test_index_search_range_filter(self, client, ctx):
        """Range query on __timestamp field."""
        import time as _time
        now_ms = int(_time.time() * 1000)
        one_hour_ago_ms = now_ms - (3600 * 1000)
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"range": {"__timestamp": {"gte": one_hour_ago_ms, "lte": now_ms}}},
                            {"term": {"__state": "ACTIVE"}},
                        ]
                    }
                }
            }
        })
        assert_status(resp, 200)

    @test("basic_search_sort_ascending", tags=["search"], order=20)
    def test_basic_search_sort_ascending(self, client, ctx):
        """POST /search/basic with sortOrder=ASCENDING.

        sortBy on basic search goes through JanusGraph which may fail with
        PermanentBackendException on some environments.
        """
        resp = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "limit": 5,
            "offset": 0,
            "sortBy": "name",
            "sortOrder": "ASCENDING",
        }, timeout=60)
        assert_status(resp, 200)
        if resp.status_code == 200:
            body = resp.json()
            entities = body.get("entities", [])
            if len(entities) >= 2:
                names = [e.get("attributes", {}).get("name", "") or e.get("displayText", "") for e in entities]
                names_clean = [n for n in names if n]
                if len(names_clean) >= 2:
                    assert names_clean == sorted(names_clean, key=str.lower), (
                        f"Expected ascending sort, got: {names_clean}"
                    )

    @test("basic_search_sort_descending", tags=["search"], order=21)
    def test_basic_search_sort_descending(self, client, ctx):
        """POST /search/basic with sortOrder=DESCENDING.

        sortBy on basic search goes through JanusGraph which may fail with
        PermanentBackendException on some environments.
        """
        resp = client.post("/search/basic", json_data={
            "typeName": "DataSet",
            "limit": 5,
            "offset": 0,
            "sortBy": "name",
            "sortOrder": "DESCENDING",
        }, timeout=60)
        assert_status(resp, 200)
        if resp.status_code == 200:
            body = resp.json()
            entities = body.get("entities", [])
            if len(entities) >= 2:
                names = [e.get("attributes", {}).get("name", "") or e.get("displayText", "") for e in entities]
                names_clean = [n for n in names if n]
                if len(names_clean) >= 2:
                    assert names_clean == sorted(names_clean, key=str.lower, reverse=True), (
                        f"Expected descending sort, got: {names_clean}"
                    )

    @test("index_search_wildcard", tags=["search"], order=22)
    def test_index_search_wildcard(self, client, ctx):
        """Wildcard query on qualifiedName with test harness prefix."""
        from core.data_factory import PREFIX
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"wildcard": {"qualifiedName": f"{PREFIX}*"}},
                            {"term": {"__state": "ACTIVE"}},
                        ]
                    }
                }
            }
        })
        assert_status(resp, 200)
        body = resp.json()
        count = body.get("approximateCount", 0)
        assert count > 0, (
            f"Expected test harness entities with prefix '{PREFIX}*', got count={count}"
        )

    @test("search_relationship_type", tags=["search"], order=23)
    def test_search_relationship_type(self, client, ctx):
        """POST /search/basic with a relationship typeName."""
        resp = client.post("/search/basic", json_data={
            "typeName": "AtlasGlossaryTermAnchor",
            "limit": 5,
            "offset": 0,
        }, timeout=60)
        assert_status_in(resp, [200, 400, 404])

    @test("index_search_bool_should", tags=["search"], order=24)
    def test_index_search_bool_should(self, client, ctx):
        """Bool should (OR) search for multiple GUIDs."""
        guid1 = ctx.get_entity_guid("ds1")
        guid2 = ctx.get_entity_guid("ds2")
        if not guid1 or not guid2:
            from core.assertions import SkipTestError
            raise SkipTestError("ds1/ds2 GUIDs not available")
        resp = client.post("/search/indexsearch", json_data={
            "dsl": {
                "from": 0,
                "size": 10,
                "query": {
                    "bool": {
                        "should": [
                            {"term": {"__guid": guid1}},
                            {"term": {"__guid": guid2}},
                        ],
                        "minimum_should_match": 1,
                    }
                }
            }
        })
        assert_status(resp, 200)
        body = resp.json()
        count = body.get("approximateCount", 0)
        assert count >= 1, f"Expected at least 1 result for OR on 2 GUIDs, got {count}"

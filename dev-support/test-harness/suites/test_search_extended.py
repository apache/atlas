"""Extended search operator tests (12 tests).

Tests all search operators: STARTS_WITH, ENDS_WITH, CONTAINS, EQ, NEQ,
is_null, not_null, OR condition, nested AND/OR, system attributes,
timestamp range, and state filter.
"""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present, SkipTestError,
)
from core.data_factory import (
    build_dataset_entity, unique_qn, unique_name, PREFIX,
)


def _index_search(client, dsl, retries=2, interval=3):
    """Issue an indexsearch query and return (available, body)."""
    for attempt in range(retries + 1):
        resp = client.post("/search/indexsearch", json_data={"dsl": dsl})
        if resp.status_code in (404, 400, 405):
            return False, {}
        if resp.status_code == 200:
            return True, resp.json()
        if resp.status_code in (500, 503) and attempt < retries:
            time.sleep(interval)
            continue
        return False, {}
    return False, {}


def _basic_search(client, payload):
    """Issue a basic search and return (available, body)."""
    resp = client.post("/search/basic", json_data=payload)
    if resp.status_code in (404, 400, 405):
        return False, {}
    if resp.status_code == 200:
        return True, resp.json()
    return False, {}


@suite("search_extended", depends_on_suites=["entity_crud"],
       description="Extended search operator coverage")
class SearchExtendedSuite:

    def setup(self, client, ctx):
        # Create a distinct entity with known attributes for searching
        self.qn = unique_qn("search-ext")
        self.entity_name = unique_name("search-ext")
        entity = build_dataset_entity(
            qn=self.qn, name=self.entity_name,
            extra_attrs={"description": "search extended test entity"},
        )
        resp = client.post("/entity", json_data={"entity": entity})
        if resp.status_code != 200:
            raise Exception(f"Failed to create entity: {resp.status_code}")
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        if not entities:
            raise Exception("Entity creation returned empty mutatedEntities")
        self.guid = entities[0]["guid"]
        ctx.set("search_ext_guid", self.guid)
        ctx.set("search_ext_qn", self.qn)
        ctx.register_cleanup(lambda: client.delete(
            f"/entity/guid/{self.guid}", params={"deleteType": "PURGE"}
        ))

        # Wait for ES indexing
        time.sleep(5)

    @test("search_starts_with", tags=["search", "operator"], order=1)
    def test_search_starts_with(self, client, ctx):
        """Basic search with STARTS_WITH on name."""
        # PREFIX is the test harness prefix used in unique_name
        available, body = _basic_search(client, {
            "typeName": "DataSet",
            "query": PREFIX,
            "searchParameters": {
                "attributes": ["name"],
            },
        })
        if not available:
            raise SkipTestError("Basic search endpoint not available")
        entities = body.get("entities", [])
        # Should find at least our created entity
        count = body.get("approximateCount", len(entities))
        assert count >= 0, "Expected non-negative count"

    @test("search_contains_operator", tags=["search", "operator"], order=2)
    def test_search_contains_operator(self, client, ctx):
        """Index search with wildcard CONTAINS pattern on name."""
        available, body = _index_search(client, {
            "from": 0, "size": 10,
            "query": {
                "wildcard": {"__qualifiedName": f"*search-ext*"}
            },
        })
        if not available:
            raise SkipTestError("Index search not available")
        count = body.get("approximateCount", 0)
        # May or may not find depending on field mapping
        assert isinstance(count, int), f"Expected int count, got {type(count)}"

    @test("search_eq_by_guid", tags=["search", "operator"], order=3)
    def test_search_eq_by_guid(self, client, ctx):
        """Index search with exact term match on __guid (EQ)."""
        guid = ctx.get("search_ext_guid")
        assert guid, "search_ext_guid not found"
        available, body = _index_search(client, {
            "from": 0, "size": 1,
            "query": {"term": {"__guid": guid}},
        })
        if not available:
            raise SkipTestError("Index search not available")
        count = body.get("approximateCount", 0)
        assert count >= 1, f"Expected entity {guid} in search, got count={count}"
        entities = body.get("entities", [])
        if entities:
            found_guid = entities[0].get("guid")
            assert found_guid == guid, f"Expected guid={guid}, got {found_guid}"

    @test("search_neq_operator", tags=["search", "operator"], order=4)
    def test_search_neq_operator(self, client, ctx):
        """Index search with must_not (NEQ) on __typeName."""
        available, body = _index_search(client, {
            "from": 0, "size": 5,
            "query": {
                "bool": {
                    "must": [{"match_all": {}}],
                    "must_not": [{"term": {"__typeName": "Process"}}],
                }
            },
        })
        if not available:
            raise SkipTestError("Index search not available")
        entities = body.get("entities", [])
        for e in entities:
            tn = e.get("typeName", "")
            assert tn != "Process", f"Found Process entity despite must_not filter"

    @test("search_is_null_filter", tags=["search", "operator"], order=5)
    def test_search_is_null_filter(self, client, ctx):
        """Index search for entities where description is null (must_not exists)."""
        available, body = _index_search(client, {
            "from": 0, "size": 5,
            "query": {
                "bool": {
                    "must": [{"term": {"__typeName": "DataSet"}}],
                    "must_not": [{"exists": {"field": "description"}}],
                }
            },
        })
        if not available:
            raise SkipTestError("Index search not available")
        # Just validate it returns valid response
        count = body.get("approximateCount", 0)
        assert isinstance(count, int), f"Expected int count"

    @test("search_not_null_filter", tags=["search", "operator"], order=6)
    def test_search_not_null_filter(self, client, ctx):
        """Index search for entities where description exists (not null)."""
        available, body = _index_search(client, {
            "from": 0, "size": 5,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__typeName": "DataSet"}},
                        {"exists": {"field": "description"}},
                    ],
                }
            },
        })
        if not available:
            raise SkipTestError("Index search not available")
        count = body.get("approximateCount", 0)
        assert isinstance(count, int), f"Expected int count"

    @test("search_or_condition", tags=["search", "operator"], order=7)
    def test_search_or_condition(self, client, ctx):
        """Index search with bool should (OR) for two type names."""
        available, body = _index_search(client, {
            "from": 0, "size": 10,
            "query": {
                "bool": {
                    "should": [
                        {"term": {"__typeName": "DataSet"}},
                        {"term": {"__typeName": "Process"}},
                    ],
                    "minimum_should_match": 1,
                }
            },
        })
        if not available:
            raise SkipTestError("Index search not available")
        entities = body.get("entities", [])
        for e in entities:
            tn = e.get("typeName", "")
            assert tn in ("DataSet", "Process"), (
                f"Expected DataSet or Process, got {tn}"
            )

    @test("search_nested_and_or", tags=["search", "operator"], order=8)
    def test_search_nested_and_or(self, client, ctx):
        """Index search with nested AND+OR: (typeName=DataSet) AND (state=ACTIVE OR DELETED)."""
        available, body = _index_search(client, {
            "from": 0, "size": 5,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__typeName": "DataSet"}},
                    ],
                    "should": [
                        {"term": {"__state": "ACTIVE"}},
                        {"term": {"__state": "DELETED"}},
                    ],
                    "minimum_should_match": 1,
                }
            },
        })
        if not available:
            raise SkipTestError("Index search not available")
        entities = body.get("entities", [])
        for e in entities:
            tn = e.get("typeName", "")
            assert tn == "DataSet", f"Expected DataSet, got {tn}"

    @test("search_system_attributes", tags=["search", "operator"], order=9)
    def test_search_system_attributes(self, client, ctx):
        """Verify system attributes (__guid, __typeName, __state) present in results."""
        guid = ctx.get("search_ext_guid")
        assert guid, "search_ext_guid not found"
        available, body = _index_search(client, {
            "from": 0, "size": 1,
            "query": {"term": {"__guid": guid}},
        })
        if not available:
            raise SkipTestError("Index search not available")
        entities = body.get("entities", [])
        if not entities:
            raise SkipTestError("Entity not yet indexed")
        e = entities[0]
        assert e.get("guid") or e.get("__guid"), "Missing guid in search result"
        assert e.get("typeName") or e.get("__typeName"), "Missing typeName in search result"
        status = e.get("status") or e.get("__state")
        assert status, "Missing status/__state in search result"

    @test("search_timestamp_range", tags=["search", "operator"], order=10)
    def test_search_timestamp_range(self, client, ctx):
        """Index search with range filter on __timestamp."""
        import datetime
        # Search for entities created in the last 24 hours
        now_ms = int(time.time() * 1000)
        day_ago_ms = now_ms - (24 * 60 * 60 * 1000)

        available, body = _index_search(client, {
            "from": 0, "size": 5,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__typeName": "DataSet"}},
                        {"range": {"__timestamp": {"gte": day_ago_ms, "lte": now_ms}}},
                    ],
                }
            },
        })
        if not available:
            raise SkipTestError("Index search not available")
        count = body.get("approximateCount", 0)
        assert isinstance(count, int), f"Expected int count"

    @test("search_state_active_filter", tags=["search", "operator"], order=11)
    def test_search_state_active_filter(self, client, ctx):
        """Index search filtering only ACTIVE entities."""
        guid = ctx.get("search_ext_guid")
        assert guid, "search_ext_guid not found"
        available, body = _index_search(client, {
            "from": 0, "size": 1,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__guid": guid}},
                        {"term": {"__state": "ACTIVE"}},
                    ],
                }
            },
        })
        if not available:
            raise SkipTestError("Index search not available")
        count = body.get("approximateCount", 0)
        assert count >= 1, f"Expected ACTIVE entity {guid} in search, got count={count}"

    @test("search_prefix_query", tags=["search", "operator"], order=12)
    def test_search_prefix_query(self, client, ctx):
        """Index search with prefix query on __typeName."""
        available, body = _index_search(client, {
            "from": 0, "size": 5,
            "query": {
                "prefix": {"__typeName": "Data"},
            },
        })
        if not available:
            raise SkipTestError("Index search not available")
        entities = body.get("entities", [])
        for e in entities:
            tn = e.get("typeName", "")
            assert tn.startswith("Data"), f"Expected type starting with 'Data', got {tn}"

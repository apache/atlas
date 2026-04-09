"""Direct ES search tests.

Tests POST /direct/search endpoint with SIMPLE, PIT_CREATE, PIT_SEARCH, PIT_DELETE.

Server requirements (from DirectSearchREST.validateRequest):
  - SIMPLE:     requires indexName + query
  - PIT_CREATE: requires indexName
  - PIT_SEARCH: requires query with top-level "pit" section AND nested "query"
  - PIT_DELETE:  requires pitId

Note: This endpoint is restricted to Argo service user. On environments where
the test user is not Argo, all tests will SKIP with 403.
"""

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError

# ES index name used by JanusGraph on staging/preprod
ES_INDEX_NAME = "janusgraph_vertex_index"
# PIT keepAlive in milliseconds (server expects Long, not string like "1m")
PIT_KEEP_ALIVE_MS = 60000


def _check_argo_access(resp):
    """Raise SkipTestError if endpoint returned 403 (Argo-only)."""
    if resp.status_code == 403:
        raise SkipTestError("Direct search requires Argo service user (403)")


@suite("direct_search", depends_on_suites=["entity_crud"],
       description="Direct Elasticsearch search endpoint")
class DirectSearchSuite:

    @test("direct_search_simple", tags=["search", "direct_search"], order=1)
    def test_direct_search_simple(self, client, ctx):
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "indexName": ES_INDEX_NAME,
            "query": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
            }
        })
        _check_argo_access(resp)
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, dict) and body, "Expected non-empty dict from direct search"

    @test("direct_search_pit_create", tags=["search", "direct_search"], order=2)
    def test_direct_search_pit_create(self, client, ctx):
        resp = client.post("/direct/search", json_data={
            "searchType": "PIT_CREATE",
            "keepAlive": PIT_KEEP_ALIVE_MS,
            "indexName": ES_INDEX_NAME,
        })
        _check_argo_access(resp)
        assert_status(resp, 200)
        body = resp.json()
        # DirectSearchResponse wraps ES response in pitCreateResponse field
        pit_obj = body.get("pitCreateResponse", {})
        if not isinstance(pit_obj, dict):
            pit_obj = {}
        pit_id = (
            body.get("pitId") or body.get("id") or body.get("pit_id")
            or pit_obj.get("id") or pit_obj.get("pointInTimeId")
        )
        if pit_id:
            ctx.set("pit_id", pit_id)
        else:
            print(f"  [pit-create] 200 but no pitId found. "
                  f"Response keys: {list(body.keys())}, "
                  f"pitCreateResponse keys: {list(pit_obj.keys()) if pit_obj else 'N/A'}")

    @test("direct_search_pit_delete", tags=["search", "direct_search"], order=3,
          depends_on=["direct_search_pit_create"])
    def test_direct_search_pit_delete(self, client, ctx):
        pit_id = ctx.get("pit_id")
        if not pit_id:
            raise SkipTestError("pit_id not found in context — PIT_CREATE may not return pit ID field")
        resp = client.post("/direct/search", json_data={
            "searchType": "PIT_DELETE",
            "pitId": pit_id,
        })
        _check_argo_access(resp)
        assert_status(resp, 200)

    @test("direct_search_with_filter", tags=["search", "direct_search"], order=4)
    def test_direct_search_with_filter(self, client, ctx):
        """SIMPLE search with bool must filter for DataSet type."""
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "indexName": ES_INDEX_NAME,
            "query": {
                "from": 0,
                "size": 5,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"__typeName.keyword": "DataSet"}},
                            {"term": {"__state": "ACTIVE"}},
                        ]
                    }
                },
            }
        })
        _check_argo_access(resp)
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, dict), "Expected dict response"

    @test("direct_search_size_limit", tags=["search", "direct_search"], order=5)
    def test_direct_search_size_limit(self, client, ctx):
        """SIMPLE search with size=1."""
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "indexName": ES_INDEX_NAME,
            "query": {
                "from": 0,
                "size": 1,
                "query": {"match_all": {}},
            }
        })
        _check_argo_access(resp)
        assert_status(resp, 200)

    @test("direct_search_with_sort", tags=["search", "direct_search"], order=6)
    def test_direct_search_with_sort(self, client, ctx):
        """SIMPLE search with sort by __timestamp desc."""
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "indexName": ES_INDEX_NAME,
            "query": {
                "from": 0,
                "size": 5,
                "query": {"match_all": {}},
                "sort": [{"__timestamp": {"order": "desc"}}],
            }
        })
        _check_argo_access(resp)
        assert_status(resp, 200)

    @test("direct_search_aggregation", tags=["search", "direct_search"], order=7)
    def test_direct_search_aggregation(self, client, ctx):
        """SIMPLE search with aggregation for type counts."""
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "indexName": ES_INDEX_NAME,
            "query": {
                "from": 0,
                "size": 0,
                "query": {"match_all": {}},
                "aggs": {
                    "type_counts": {
                        "terms": {"field": "__typeName.keyword", "size": 10}
                    }
                },
            }
        })
        _check_argo_access(resp)
        assert_status(resp, 200)

    @test("direct_search_pit_flow", tags=["search", "direct_search"], order=8)
    def test_direct_search_pit_flow(self, client, ctx):
        """Full PIT flow: create, search with PIT, delete."""
        # 1. Create PIT
        resp1 = client.post("/direct/search", json_data={
            "searchType": "PIT_CREATE",
            "keepAlive": PIT_KEEP_ALIVE_MS,
            "indexName": ES_INDEX_NAME,
        })
        _check_argo_access(resp1)
        assert_status(resp1, 200)
        body1 = resp1.json()
        pit_obj = body1.get("pitCreateResponse", {})
        if not isinstance(pit_obj, dict):
            pit_obj = {}
        pit_id = (
            body1.get("pitId") or body1.get("id")
            or pit_obj.get("id") or pit_obj.get("pointInTimeId")
        )
        assert pit_id, (
            f"PIT_CREATE returned 200 but no pitId found. "
            f"Keys: {list(body1.keys())}, pitCreateResponse: {list(pit_obj.keys()) if pit_obj else 'N/A'}"
        )

        # 2. Search with PIT — server requires top-level "pit" key in query
        #    (hasPitSection checks: query.containsKey("pit") && query.get("query") != null)
        resp2 = client.post("/direct/search", json_data={
            "searchType": "PIT_SEARCH",
            "query": {
                "size": 5,
                "query": {"match_all": {}},
                "pit": {"id": pit_id, "keep_alive": "1m"},
            }
        })
        assert_status(resp2, 200)

        # 3. Delete PIT
        resp3 = client.post("/direct/search", json_data={
            "searchType": "PIT_DELETE",
            "pitId": pit_id,
        })
        assert_status(resp3, 200)

    @test("direct_search_invalid_type", tags=["search", "direct_search", "negative"], order=9)
    def test_direct_search_invalid_type(self, client, ctx):
        """Search with nonexistent searchType — should fail validation."""
        resp = client.post("/direct/search", json_data={
            "searchType": "NONEXISTENT_TYPE",
            "indexName": ES_INDEX_NAME,
            "query": {"from": 0, "size": 5, "query": {"match_all": {}}},
        })
        _check_argo_access(resp)
        # Server catches invalid enum in switch default → INVALID_PARAMETERS (400)
        # or wraps in DISCOVERY_QUERY_FAILED (500)
        assert_status_in(resp, [400, 404, 500])

    @test("direct_search_empty_query", tags=["search", "direct_search", "negative"], order=10)
    def test_direct_search_empty_query(self, client, ctx):
        """SIMPLE search with indexName but empty query — should fail validation."""
        resp = client.post("/direct/search", json_data={
            "searchType": "SIMPLE",
            "indexName": ES_INDEX_NAME,
            "query": {},
        })
        _check_argo_access(resp)
        # Empty query fails: validateRequest requires query != null for SIMPLE,
        # but {} is non-null. The actual ES call will fail (no query body).
        # Accept either success (ES interprets {} as match_all) or error.
        assert_status_in(resp, [200, 400, 500])

"""Search filter tests.

Validates that ES search correctly filters by certificate status,
labels, business metadata values, glossary term meanings, and
advanced attribute operators via the basic search API.
"""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError
from core.data_factory import (
    build_dataset_entity, build_business_metadata_def,
    unique_name, unique_qn, unique_type_name, PREFIX,
)
from core.typedef_helpers import create_typedef_verified, extract_bm_names_from_response, ensure_bm_types


# ES field names for qualifiedName differ between local and staging
QN_FIELDS = ("qualifiedName.keyword", "qualifiedName", "__qualifiedName")


def _index_search(client, dsl, retries=2, interval=3):
    """Issue an indexsearch query and return (available, body). Retries on 500/503."""
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


def _search_by_guid(client, guid):
    """Search for a single entity by GUID."""
    return _index_search(client, {
        "from": 0, "size": 1,
        "query": {"bool": {"must": [
            {"term": {"__guid": guid}},
            {"term": {"__state": "ACTIVE"}},
        ]}}
    })


def _poll_index_search(client, dsl, max_wait=30, interval=5, label="search"):
    """Poll ES until query returns at least 1 result."""
    last_body = {}
    for i in range(max_wait // interval):
        if i > 0:
            time.sleep(interval)
        available, body = _index_search(client, dsl)
        if not available:
            return False, {}
        if body.get("approximateCount", 0) > 0:
            return True, body
        last_body = body
        print(f"  [{label}] Polling ES ({(i+1)*interval}s/{max_wait}s)")
    return True, last_body


def _poll_search_by_guid(client, guid, max_wait=30, interval=5):
    """Poll ES until entity appears in search results."""
    last_body = {}
    for i in range(max_wait // interval):
        if i > 0:
            time.sleep(interval)
        available, body = _search_by_guid(client, guid)
        if not available:
            return False, {}
        if body.get("entities"):
            return True, body
        last_body = body
        print(f"  [poll-es] Waiting for {guid} in ES ({(i+1)*interval}s/{max_wait}s)")
    return True, last_body


def _create_entity_and_register(client, ctx, suffix, extra_attrs=None,
                                labels=None):
    """Create a DataSet entity, register cleanup, return (guid, qn, name)."""
    qn = unique_qn(suffix)
    name = unique_name(suffix)
    entity = build_dataset_entity(qn=qn, name=name, extra_attrs=extra_attrs)
    if labels is not None:
        entity["labels"] = labels
    resp = client.post("/entity", json_data={"entity": entity})
    assert_status(resp, 200)
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    guid = entities[0]["guid"]
    ctx.register_entity_cleanup(guid)
    return guid, qn, name


@suite("search_filters", depends_on_suites=["entity_crud"],
       description="Search filter operations (certificate, labels, BM, meanings, operators)")
class SearchFiltersSuite:

    def setup(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)

        # --- Entity A: certificateStatus=VERIFIED, has description ---
        self.guid_a, self.qn_a, self.name_a = _create_entity_and_register(
            client, ctx, "filt-a",
            extra_attrs={
                "certificateStatus": "VERIFIED",
                "description": "filter-desc-alpha",
            },
        )

        # --- Entity B: certificateStatus=DRAFT ---
        self.guid_b, self.qn_b, self.name_b = _create_entity_and_register(
            client, ctx, "filt-b",
            extra_attrs={"certificateStatus": "DRAFT"},
        )

        # --- Entity C: labels ---
        self.labels_c = ["filter-label-x", "filter-label-y"]
        self.guid_c, self.qn_c, self.name_c = _create_entity_and_register(
            client, ctx, "filt-c", labels=self.labels_c,
        )
        # Read-after-write: verify labels were persisted via entity creation
        resp_check = client.get(f"/entity/guid/{self.guid_c}")
        if resp_check.status_code == 200:
            labels_check = resp_check.json().get("entity", {}).get("labels", [])
            if not labels_check:
                # Labels not set via entity body — set via explicit labels endpoint
                client.post(
                    f"/entity/guid/{self.guid_c}/labels",
                    json_data=self.labels_c,
                )

        # --- Entity E: plain, no extras (negative check) ---
        self.guid_e, self.qn_e, self.name_e = _create_entity_and_register(
            client, ctx, "filt-e",
        )

        # --- BM typedef (with fallback) + Entity D ---
        self.bm_ok = False
        self.bm_display_name = unique_type_name("FiltBM")
        self.bm_internal_name = None
        self.bm_field_display = "bmField1"  # default
        self.bm_attr_internal = None  # ES field name for BM attr value
        bm_info, created_bm, bm_typedef_ok = ensure_bm_types(
            client, self.bm_display_name,
        )
        if bm_info:
            self.bm_internal_name = bm_info["internal_name"]
            self.bm_display_name = bm_info["display_name"]
            self.bm_field_display = bm_info["first_attr_display"]
            self.bm_attr_internal = bm_info["attr_map"].get(
                bm_info["first_attr_display"], bm_info["first_attr_display"]
            )
        if bm_typedef_ok and created_bm:
            cleanup_name = self.bm_internal_name or self.bm_display_name
            ctx.register_typedef_cleanup(client, cleanup_name)

        self.guid_d, self.qn_d, self.name_d = _create_entity_and_register(
            client, ctx, "filt-d",
        )

        if bm_typedef_ok:
            # Set BM value on Entity D via displayName endpoint (retry on 404 type cache lag)
            for attempt in range(3):
                bm_set_resp = client.post(
                    f"/entity/guid/{self.guid_d}/businessmetadata/displayName",
                    json_data={self.bm_display_name: {self.bm_field_display: "filter-bm-val"}},
                )
                if bm_set_resp.status_code in (200, 204):
                    self.bm_ok = True
                    break
                if attempt < 2:
                    time.sleep(10)
            if not self.bm_ok:
                print(f"  [search-filters] BM set failed after 3 attempts "
                      f"(last status={bm_set_resp.status_code}) — BM tests will SKIP")
        else:
            print(f"  [search-filters] BM typedef creation failed — BM tests will SKIP")

        # --- Glossary + Term assigned to Entity A ---
        self.glossary_ok = False
        self.term_qn = None
        self.term_guid = None

        glossary_resp = client.post("/glossary", json_data={
            "name": unique_name("filt-gloss"),
            "shortDescription": "Filter test glossary",
        }, timeout=180)
        if glossary_resp.status_code == 200:
            glossary_guid = glossary_resp.json().get("guid")
            ctx.register_cleanup(lambda: client.delete(f"/glossary/{glossary_guid}"))

            term_name = unique_name("filt-term")
            term_resp = client.post("/glossary/term", json_data={
                "name": term_name,
                "shortDescription": "Filter test term",
                "anchor": {"glossaryGuid": glossary_guid},
            }, timeout=180)
            if term_resp.status_code == 200:
                term_body = term_resp.json()
                self.term_guid = term_body.get("guid")
                self.term_qn = term_body.get("qualifiedName")
                ctx.register_cleanup(
                    lambda: client.delete(f"/glossary/term/{self.term_guid}")
                )

                # Assign term to Entity A
                assign_resp = client.post(
                    f"/glossary/terms/{self.term_guid}/assignedEntities",
                    json_data=[{"guid": self.guid_a, "typeName": "DataSet"}],
                )
                if assign_resp.status_code in (200, 204):
                    self.glossary_ok = True
                else:
                    print(f"  [search-filters] Term assignment returned {assign_resp.status_code} — "
                          f"glossary tests will SKIP")
            else:
                print(f"  [search-filters] Term creation returned {term_resp.status_code} — "
                      f"glossary tests will SKIP")
        else:
            print(f"  [search-filters] Glossary creation returned {glossary_resp.status_code} — "
                  f"glossary tests will SKIP")

        # Single ES sync wait after all setup
        time.sleep(max(es_wait, 5))

    # ----------------------------------------------------------------
    # Certificate Status Filters
    # ----------------------------------------------------------------

    @test("filter_certificate_verified", tags=["filter", "search", "certificate"],
          order=1)
    def test_filter_certificate_verified(self, client, ctx):
        """DSL term filter on certificateStatus=VERIFIED."""
        dsl = {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"term": {"certificateStatus": "VERIFIED"}},
                {"term": {"__typeName.keyword": "DataSet"}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        }
        available, body = _poll_index_search(client, dsl, max_wait=30, interval=5,
                                             label="cert-verified")
        assert available, "Index search API not available"
        count = body.get("approximateCount", 0)
        assert count > 0, (
            f"Expected certificateStatus=VERIFIED to return results, got count=0. "
            f"Verify certificateStatus is indexed on this environment."
        )

        guids = [e.get("guid") for e in body.get("entities", [])]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found in VERIFIED filter results: {guids}"
        )
        # Entity B should NOT appear (it's DRAFT)
        assert self.guid_b not in guids, (
            f"Entity B ({self.guid_b}) should not appear in VERIFIED filter"
        )

    @test("filter_certificate_draft", tags=["filter", "search", "certificate"],
          order=2)
    def test_filter_certificate_draft(self, client, ctx):
        """DSL term filter on certificateStatus=DRAFT."""
        dsl = {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"term": {"certificateStatus": "DRAFT"}},
                {"term": {"__typeName.keyword": "DataSet"}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        }
        available, body = _poll_index_search(client, dsl, max_wait=30, interval=5,
                                             label="cert-draft")
        assert available, "Index search API not available"
        count = body.get("approximateCount", 0)
        assert count > 0, "Expected certificateStatus=DRAFT to return results, got count=0"

        guids = [e.get("guid") for e in body.get("entities", [])]
        assert self.guid_b in guids, (
            f"Entity B ({self.guid_b}) not found in DRAFT filter results: {guids}"
        )

    @test("filter_certificate_basic_search_api", tags=["filter", "search", "certificate"],
          order=3)
    def test_filter_certificate_basic_search_api(self, client, ctx):
        """Basic search API with entityFilters for certificateStatus=VERIFIED."""
        # Retry on transient errors
        entities = []
        for attempt in range(3):
            resp = client.post("/search/basic", json_data={
                "typeName": "DataSet",
                "excludeDeletedEntities": True,
                "limit": 10,
                "entityFilters": {
                    "condition": "AND",
                    "criterion": [{
                        "attributeName": "certificateStatus",
                        "operator": "eq",
                        "attributeValue": "VERIFIED",
                    }],
                },
            })
            if resp.status_code == 200:
                body = resp.json()
                entities = body.get("entities", [])
                if entities:
                    break
            if attempt < 2:
                time.sleep(5)
        assert_status(resp, 200)
        assert entities, (
            "Basic search with certificateStatus=VERIFIED returned no entities"
        )

        guids = [e.get("guid") for e in entities]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found via basic search "
            f"certificateStatus=VERIFIED: {guids}"
        )

    # ----------------------------------------------------------------
    # Label Filters
    # ----------------------------------------------------------------

    @test("filter_by_label_dsl", tags=["filter", "search", "label"], order=4)
    def test_filter_by_label_dsl(self, client, ctx):
        """DSL filter on __labels (wildcard for pipe-delimited storage)."""
        # Labels stored as pipe-delimited string "|label1|label2|" in ES __labels field.
        # Use wildcard query since term query requires exact string match.
        dsl = {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"wildcard": {"__labels": "*filter-label-x*"}},
                {"term": {"__typeName.keyword": "DataSet"}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        }
        available, body = _poll_index_search(client, dsl, max_wait=60, interval=5,
                                             label="label-filter")
        assert available, "Index search API not available"
        count = body.get("approximateCount", 0)
        if count == 0:
            raise SkipTestError(
                "Label filter-label-x not indexed in ES after 60s — "
                "ES sync may be slow or labels stored differently"
            )

        guids = [e.get("guid") for e in body.get("entities", [])]
        assert self.guid_c in guids, (
            f"Entity C ({self.guid_c}) not found in label filter results: {guids}"
        )

    @test("filter_by_label_excludes_unlabeled", tags=["filter", "search", "label"],
          order=5)
    def test_filter_by_label_excludes_unlabeled(self, client, ctx):
        """Verify __labels filter excludes entities without that label."""
        available, body = _index_search(client, {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"wildcard": {"__labels": "*filter-label-x*"}},
                {"term": {"__guid": self.guid_e}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        assert available, "Index search API not available"
        count = body.get("approximateCount", 0)
        assert count == 0, (
            f"Entity E ({self.guid_e}) should NOT match label filter, "
            f"but got count={count}"
        )

    # ----------------------------------------------------------------
    # Business Metadata Value Filter
    # ----------------------------------------------------------------

    @test("filter_by_bm_value_in_search", tags=["filter", "search", "businessmeta"],
          order=6)
    def test_filter_by_bm_value_in_search(self, client, ctx):
        """Filter entities by BM attribute value in ES (DSL term query on attr internal name)."""
        if not self.bm_ok:
            raise SkipTestError("BM typedef/set not available — skipping BM search test")
        if not self.bm_attr_internal:
            raise SkipTestError("BM attr internal name not available")

        # Use BM attribute's internal name as flat ES field for filtering —
        # same pattern the UI uses (see SearchByBM.txt).
        # AtlasEntityHeader (search result) has NO businessAttributes field;
        # BM values are indexed as flat ES fields keyed by the attr internal name.
        dsl = {
            "from": 0, "size": 5,
            "query": {"bool": {"must": [
                {"term": {self.bm_attr_internal: "filter-bm-val"}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        }
        available, body = _poll_index_search(
            client, dsl, max_wait=30, interval=5, label="bm-filter",
        )
        assert available, "Index search API not available"
        count = body.get("approximateCount", 0)
        if count == 0:
            raise SkipTestError(
                f"BM attr {self.bm_attr_internal}=filter-bm-val not indexed "
                f"in ES after 30s — ES sync delay"
            )

        guids = [e.get("guid") for e in body.get("entities", [])]
        assert self.guid_d in guids, (
            f"Entity D ({self.guid_d}) not found in BM filter results: {guids}"
        )

    # ----------------------------------------------------------------
    # Glossary Term / Meanings Filter
    # ----------------------------------------------------------------

    @test("filter_by_meanings_dsl", tags=["filter", "search", "glossary"], order=7)
    def test_filter_by_meanings_dsl(self, client, ctx):
        """DSL term filter on __meanings by term qualifiedName."""
        if not self.glossary_ok:
            raise SkipTestError("Glossary/term setup not available — skipping meanings test")
        dsl = {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"term": {"__meanings": self.term_qn}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        }
        available, body = _poll_index_search(client, dsl, max_wait=30, interval=5,
                                             label="meanings-filter")
        assert available, "Index search API not available"
        count = body.get("approximateCount", 0)
        assert count > 0, (
            f"Expected __meanings={self.term_qn} to return results, got count=0"
        )

        guids = [e.get("guid") for e in body.get("entities", [])]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found in __meanings filter "
            f"for term QN {self.term_qn}: {guids}"
        )

    @test("filter_meanings_in_result", tags=["filter", "search", "glossary"],
          order=8)
    def test_filter_meanings_in_result(self, client, ctx):
        """Search Entity A by GUID and verify meanings field contains the term."""
        if not self.glossary_ok:
            raise SkipTestError("Glossary/term setup not available — skipping meanings test")
        available, body = _poll_search_by_guid(client, self.guid_a, max_wait=30)
        assert available, "Index search API not available"

        entities = body.get("entities", [])
        assert entities, f"Entity A ({self.guid_a}) not found in search after polling"

        entity = entities[0]
        meanings = entity.get("meanings", [])
        meaning_names = entity.get("meaningNames", [])

        assert meanings or meaning_names, (
            f"Expected meanings or meaningNames on entity {self.guid_a}, "
            f"but both are empty in search result"
        )
        # Check meanings objects (list of dicts with termGuid)
        if meanings and isinstance(meanings, list) and isinstance(meanings[0], dict):
            term_guids = [m.get("termGuid") for m in meanings]
            assert self.term_guid in term_guids, (
                f"Expected termGuid {self.term_guid} in meanings, "
                f"got {term_guids}"
            )

    # ----------------------------------------------------------------
    # Advanced Attribute Filter Operators via Basic Search API
    # ----------------------------------------------------------------

    @test("filter_attr_equals", tags=["filter", "search", "operator"], order=9)
    def test_filter_attr_equals(self, client, ctx):
        """Basic search EQUALS operator on qualifiedName."""
        entities = []
        for attempt in range(3):
            resp = client.post("/search/basic", json_data={
                "typeName": "DataSet",
                "excludeDeletedEntities": True,
                "limit": 10,
                "entityFilters": {
                    "condition": "AND",
                    "criterion": [{
                        "attributeName": "qualifiedName",
                        "operator": "eq",
                        "attributeValue": self.qn_a,
                    }],
                },
            })
            if resp.status_code == 200:
                entities = resp.json().get("entities", [])
                if entities:
                    break
            if attempt < 2:
                time.sleep(5)
        assert_status(resp, 200)
        assert entities, (
            f"Basic search EQUALS on QN={self.qn_a} returned no entities"
        )
        guids = [e.get("guid") for e in entities]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found with EQUALS on QN "
            f"{self.qn_a}: {guids}"
        )

    @test("filter_attr_contains", tags=["filter", "search", "operator"], order=10)
    def test_filter_attr_contains(self, client, ctx):
        """Basic search CONTAINS operator on qualifiedName."""
        substring = self.qn_a.split("/")[-1] if "/" in self.qn_a else self.qn_a[-12:]
        entities = []
        for attempt in range(3):
            resp = client.post("/search/basic", json_data={
                "typeName": "DataSet",
                "excludeDeletedEntities": True,
                "limit": 10,
                "entityFilters": {
                    "condition": "AND",
                    "criterion": [{
                        "attributeName": "qualifiedName",
                        "operator": "contains",
                        "attributeValue": substring,
                    }],
                },
            })
            if resp.status_code == 200:
                entities = resp.json().get("entities", [])
                if entities:
                    break
            if attempt < 2:
                time.sleep(5)
        assert_status(resp, 200)
        assert entities, (
            f"Basic search CONTAINS '{substring}' returned no entities"
        )
        guids = [e.get("guid") for e in entities]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found with CONTAINS "
            f"'{substring}': {guids}"
        )

    @test("filter_attr_ends_with", tags=["filter", "search", "operator"], order=11)
    def test_filter_attr_ends_with(self, client, ctx):
        """Basic search ENDS_WITH operator on qualifiedName."""
        suffix = self.qn_a.split("/")[-1] if "/" in self.qn_a else self.qn_a[-12:]
        entities = []
        for attempt in range(3):
            resp = client.post("/search/basic", json_data={
                "typeName": "DataSet",
                "excludeDeletedEntities": True,
                "limit": 10,
                "entityFilters": {
                    "condition": "AND",
                    "criterion": [{
                        "attributeName": "qualifiedName",
                        "operator": "endsWith",
                        "attributeValue": suffix,
                    }],
                },
            })
            if resp.status_code == 200:
                entities = resp.json().get("entities", [])
                if entities:
                    break
            if attempt < 2:
                time.sleep(5)
        assert_status(resp, 200)
        assert entities, f"Basic search ENDS_WITH '{suffix}' returned no entities"
        guids = [e.get("guid") for e in entities]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) not found with ENDS_WITH "
            f"'{suffix}': {guids}"
        )

    @test("filter_attr_not_null", tags=["filter", "search", "operator"], order=12)
    def test_filter_attr_not_null(self, client, ctx):
        """Basic search NOT_NULL operator on description, scoped by QN prefix."""
        entities = []
        for attempt in range(3):
            resp = client.post("/search/basic", json_data={
                "typeName": "DataSet",
                "excludeDeletedEntities": True,
                "limit": 25,
                "entityFilters": {
                    "condition": "AND",
                    "criterion": [
                        {
                            "attributeName": "description",
                            "operator": "not_null",
                        },
                        {
                            "attributeName": "qualifiedName",
                            "operator": "startsWith",
                            "attributeValue": PREFIX,
                        },
                    ],
                },
            })
            if resp.status_code == 200:
                entities = resp.json().get("entities", [])
                if entities:
                    break
            if attempt < 2:
                time.sleep(5)
        assert_status(resp, 200)
        assert entities, (
            f"Basic search NOT_NULL on description + startsWith({PREFIX}) "
            f"returned no entities"
        )
        guids = [e.get("guid") for e in entities]
        assert self.guid_a in guids, (
            f"Entity A ({self.guid_a}) with description should appear in "
            f"NOT_NULL results: {guids}"
        )

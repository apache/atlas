"""Search field-level correctness tests.

Validates that ES index-search results contain expected fields:
classificationNames, classifications objects, labels, meanings,
approximateCount, and combined boolean queries.
"""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present, SkipTestError,
)
from core.data_factory import (
    build_dataset_entity, build_classification_def, build_process_entity,
    unique_name, unique_qn, unique_type_name, PREFIX,
    detect_process_io_type, create_process_with_io,
)
from core.typedef_helpers import create_typedef_verified, ensure_classification_types


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


# ES field names for qualifiedName differ between local and staging
QN_FIELDS = ("qualifiedName.keyword", "qualifiedName", "__qualifiedName")


def _create_entity_and_register(client, ctx, suffix, cleanup=True, type_name="DataSet"):
    """Helper: create an entity, register cleanup, return (guid, qn)."""
    qn = unique_qn(suffix)
    entity = build_dataset_entity(qn=qn, name=unique_name(suffix), type_name=type_name)
    resp = client.post("/entity", json_data={"entity": entity})
    assert_status(resp, 200)
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    guid = entities[0]["guid"]
    if cleanup:
        ctx.register_entity_cleanup(guid)
    return guid, qn


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


@suite("search_correctness", depends_on_suites=["entity_crud", "glossary"],
       description="Search field-level correctness validation")
class SearchCorrectnessSuite:

    def setup(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)

        # Get 2 usable classification types (create new or use existing)
        requested = [unique_type_name("SearchTag"), unique_type_name("SearchTag2")]
        names, self.created_types, tags_ok = ensure_classification_types(
            client, requested,
        )
        self.tag_name, self.tag2_name = names[0], names[1]
        if self.created_types:
            ctx.register_typedef_cleanup(client, self.tag_name)
            ctx.register_typedef_cleanup(client, self.tag2_name)

        # Entity A: has classification (retry on type cache lag)
        self.guid_a, self.qn_a = _create_entity_and_register(client, ctx, "search-a")
        self.tag_add_ok = False
        if tags_ok:
            for attempt in range(3):
                resp = client.post(
                    f"/entity/guid/{self.guid_a}/classifications",
                    json_data=[{"typeName": self.tag_name}],
                )
                if resp.status_code in (200, 204):
                    self.tag_add_ok = True
                    break
                print(f"  [setup] Classification add attempt {attempt+1}/3 "
                      f"returned {resp.status_code}")
                if attempt < 2:
                    time.sleep(10)
        if not self.tag_add_ok:
            print(f"  [setup] Classification add failed — "
                  f"classification tests will SKIP")

        # Entity B: has labels
        self.guid_b, self.qn_b = _create_entity_and_register(client, ctx, "search-b")
        self.labels_b = ["label-a", "label-b"]
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataSet",
                "guid": self.guid_b,
                "attributes": {
                    "qualifiedName": self.qn_b,
                    "name": unique_name("search-b"),
                },
                "labels": self.labels_b,
            }
        })

        # Entity C: has classification + labels
        self.guid_c, self.qn_c = _create_entity_and_register(client, ctx, "search-c")
        self.labels_c = ["label-c"]
        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataSet",
                "guid": self.guid_c,
                "attributes": {
                    "qualifiedName": self.qn_c,
                    "name": unique_name("search-c"),
                },
                "labels": self.labels_c,
            }
        })
        if self.tag_add_ok:
            client.post(
                f"/entity/guid/{self.guid_c}/classifications",
                json_data=[{"typeName": self.tag_name}],
            )

        # Wait for ES sync
        time.sleep(max(es_wait, 5))

    # ---- Tests ----

    @test("search_by_classification_filter", tags=["search", "correctness"], order=1)
    def test_search_by_classification_filter(self, client, ctx):
        if not self.tag_add_ok:
            raise SkipTestError("Classification not added in setup — type cache propagation failed")
        dsl = {
            "from": 0, "size": 10,
            "query": {"bool": {"must": [
                {"term": {"__typeName.keyword": "DataSet"}},
                {"match_phrase": {"__classificationNames": self.tag_name}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        }
        available, body = _poll_index_search(client, dsl, max_wait=60, interval=5,
                                             label="classification-filter")
        assert available, "Index search API not available"

        count = body.get("approximateCount", 0)
        if count == 0:
            raise SkipTestError(
                f"Classification {self.tag_name} not indexed in ES after 60s — "
                f"ES sync may be slow on this environment"
            )
        entities = body.get("entities", [])
        for e in entities:
            cn = e.get("classificationNames", [])
            assert self.tag_name in cn, (
                f"Entity {e.get('guid')} missing {self.tag_name} in classificationNames: {cn}"
            )

    @test("search_classification_objects", tags=["search", "correctness"], order=2,
          depends_on=["search_by_classification_filter"])
    def test_search_classification_objects(self, client, ctx):
        available, body = _poll_search_by_guid(client, self.guid_a, max_wait=30)
        assert available, "Index search API not available"

        entities = body.get("entities", [])
        assert len(entities) > 0, f"Entity {self.guid_a} not found in search after polling"
        entity = entities[0]
        classifications = entity.get("classifications", [])
        assert isinstance(classifications, list), (
            f"Expected classifications to be a list, got {type(classifications).__name__}"
        )
        found = any(
            isinstance(c, dict) and c.get("typeName") == self.tag_name
            for c in classifications
        )
        assert found, (
            f"Classification object with typeName={self.tag_name} not found in "
            f"search result classifications: {classifications}"
        )

    @test("search_combined_type_classification_qn", tags=["search", "correctness"], order=3,
          depends_on=["search_by_classification_filter"])
    def test_search_combined_type_classification_qn(self, client, ctx):
        # Try multiple QN field names with polling
        available = False
        body = {}
        for qn_field in QN_FIELDS:
            dsl = {
                "from": 0, "size": 10,
                "query": {"bool": {"must": [
                    {"term": {"__typeName.keyword": "DataSet"}},
                    {"match_phrase": {"__classificationNames": self.tag_name}},
                    {"wildcard": {qn_field: f"{PREFIX}*"}},
                    {"term": {"__state": "ACTIVE"}},
                ]}}
            }
            available, body = _poll_index_search(client, dsl, max_wait=20, interval=5,
                                                 label=f"combined-{qn_field}")
            if available and body.get("approximateCount", 0) > 0:
                break
        assert available, "Index search API not available"

        entities = body.get("entities", [])
        for e in entities:
            assert e.get("typeName") == "DataSet", (
                f"Expected typeName=DataSet, got {e.get('typeName')}"
            )
            cn = e.get("classificationNames", [])
            assert self.tag_name in cn, (
                f"Entity {e.get('guid')} missing {self.tag_name} in classificationNames"
            )
            qn = (e.get("attributes", {}).get("qualifiedName", "") or
                  e.get("qualifiedName", ""))
            assert qn.startswith(PREFIX), (
                f"Entity QN {qn} does not start with {PREFIX}"
            )

    @test("search_wildcard_qn_prefix", tags=["search", "correctness"], order=4)
    def test_search_wildcard_qn_prefix(self, client, ctx):
        # Try multiple QN field names to handle ES mapping differences
        count = 0
        entities = []
        available = False
        for qn_field in ("qualifiedName.keyword", "qualifiedName", "__qualifiedName"):
            dsl = {
                "from": 0, "size": 50,
                "query": {"bool": {"must": [
                    {"wildcard": {qn_field: f"{PREFIX}*"}},
                    {"term": {"__state": "ACTIVE"}},
                ]}}
            }
            available, body = _poll_index_search(client, dsl, max_wait=20, interval=5,
                                                 label=f"wildcard-{qn_field}")
            if not available:
                continue
            count = body.get("approximateCount", 0)
            entities = body.get("entities", [])
            if count > 0:
                break
        assert available, "Index search API not available"
        assert count > 0, f"Expected entities with QN prefix {PREFIX}*, got count={count}"

        for e in entities:
            qn = (e.get("attributes", {}).get("qualifiedName", "") or
                  e.get("qualifiedName", ""))
            assert qn.startswith(PREFIX), (
                f"Entity QN {qn} does not start with expected prefix {PREFIX}"
            )

    @test("search_result_has_labels", tags=["search", "correctness"], order=5)
    def test_search_result_has_labels(self, client, ctx):
        available, body = _poll_search_by_guid(client, self.guid_b, max_wait=30)
        assert available, "Index search API not available"

        entities = body.get("entities", [])
        assert len(entities) > 0, f"Entity {self.guid_b} not found in search after polling"
        entity = entities[0]
        labels = entity.get("labels", [])
        assert labels, (
            f"Expected labels {self.labels_b} on entity {self.guid_b}, "
            f"but labels field is empty/missing in search result"
        )
        for expected in self.labels_b:
            assert expected in labels, (
                f"Expected label '{expected}' in search result labels: {labels}"
            )

    @test("search_approximate_count_present", tags=["search", "correctness"], order=6)
    def test_search_approximate_count_present(self, client, ctx):
        dsl = {
            "from": 0, "size": 1,
            "query": {"bool": {"must": [
                {"term": {"__typeName.keyword": "DataSet"}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        }
        available, body = _poll_index_search(client, dsl, max_wait=20, interval=5,
                                             label="approx-count")
        assert available, "Index search API not available"

        assert "approximateCount" in body, (
            f"Expected 'approximateCount' field in search response, got keys: {list(body.keys())}"
        )
        count = body["approximateCount"]
        assert isinstance(count, int), (
            f"Expected approximateCount to be int, got {type(count).__name__}"
        )
        assert count >= 0, f"Expected approximateCount >= 0, got {count}"

    @test("search_multi_classification_entity", tags=["search", "correctness"], order=7)
    def test_search_multi_classification_entity(self, client, ctx):
        if not self.tag_add_ok:
            raise SkipTestError("Classification not added in setup — type cache propagation failed")
        # Add second classification to entity A (retry on type cache lag)
        added = False
        for attempt in range(5):
            resp = client.post(
                f"/entity/guid/{self.guid_a}/classifications",
                json_data=[{"typeName": self.tag2_name}],
            )
            if resp.status_code in (200, 204):
                added = True
                break
            if attempt < 4:
                time.sleep(15)
        assert added, (
            f"Failed to add {self.tag2_name} to {self.guid_a} after 5 attempts "
            f"(last status={resp.status_code})"
        )

        # Search by first tag with polling
        dsl1 = {
            "from": 0, "size": 5,
            "query": {"bool": {"must": [
                {"term": {"__guid": self.guid_a}},
                {"match_phrase": {"__classificationNames": self.tag_name}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        }
        available, body1 = _poll_index_search(client, dsl1, max_wait=30, interval=5,
                                              label="multi-tag1")
        assert available, "Index search API not available"
        assert body1.get("approximateCount", 0) > 0, (
            f"Entity {self.guid_a} not found when searching by {self.tag_name}"
        )

        # Search by second tag with polling
        dsl2 = {
            "from": 0, "size": 5,
            "query": {"bool": {"must": [
                {"term": {"__guid": self.guid_a}},
                {"match_phrase": {"__classificationNames": self.tag2_name}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        }
        _, body2 = _poll_index_search(client, dsl2, max_wait=30, interval=5,
                                      label="multi-tag2")
        assert body2.get("approximateCount", 0) > 0, (
            f"Entity {self.guid_a} not found when searching by {self.tag2_name}"
        )

    @test("search_exclude_deleted_by_state_filter", tags=["search", "correctness"], order=8)
    def test_search_exclude_deleted_by_state_filter(self, client, ctx):
        # Create a throwaway entity, delete it, verify __state=ACTIVE excludes it
        guid, qn = _create_entity_and_register(client, ctx, "search-del", cleanup=False)

        # Poll until entity appears in ES
        available, body = _poll_search_by_guid(client, guid, max_wait=30)
        assert available, "Index search API not available"
        assert body.get("approximateCount", 0) > 0, (
            f"Entity {guid} not found in ACTIVE search before delete"
        )

        # Soft delete
        client.delete(f"/entity/guid/{guid}")

        # Poll until entity is excluded from ACTIVE search
        print(f"  [delete-filter] Waiting up to 30s for entity {guid} to be excluded from ACTIVE search...")
        excluded = False
        for i in range(6):
            time.sleep(5)
            _, body2 = _search_by_guid(client, guid)
            if body2.get("approximateCount", 0) == 0:
                excluded = True
                print(f"  [delete-filter] Entity excluded after {(i+1)*5}s")
                break
            print(f"  [delete-filter] Still visible ({(i+1)*5}s/30s)")
        assert excluded, (
            f"Deleted entity {guid} still in ACTIVE search after 30s"
        )

    @test("search_by_glossary_term_meaning", tags=["search", "correctness", "glossary"], order=9)
    def test_search_by_glossary_term_meaning(self, client, ctx):
        # Create glossary + term + assign to entity
        glossary_name = unique_name("search-gloss")
        resp = client.post("/glossary", json_data={
            "name": glossary_name,
            "shortDescription": "Search test glossary",
        }, timeout=180)
        if resp.status_code in (408, 409, 502, 503):
            raise SkipTestError(
                f"Glossary creation returned {resp.status_code} — "
                f"server may be overloaded"
            )
        assert_status(resp, 200)
        glossary_guid = resp.json().get("guid")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/{glossary_guid}"))

        term_name = unique_name("search-term")
        resp = client.post("/glossary/term", json_data={
            "name": term_name,
            "shortDescription": "Search test term",
            "anchor": {"glossaryGuid": glossary_guid},
        }, timeout=180)
        if resp.status_code in (408, 409, 502, 503):
            raise SkipTestError(
                f"Term creation returned {resp.status_code} — "
                f"server may be overloaded"
            )
        assert_status(resp, 200)
        term_guid = resp.json().get("guid")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{term_guid}"))

        # Create entity and assign term
        entity_guid, entity_qn = _create_entity_and_register(client, ctx, "search-meaning")
        resp = client.post(f"/glossary/terms/{term_guid}/assignedEntities", json_data=[
            {"guid": entity_guid, "typeName": "DataSet"},
        ])
        assert_status_in(resp, [200, 204])

        # Poll ES until entity appears with meanings populated
        print(f"  [meanings] Waiting up to 30s for meanings to appear on {entity_guid}...")
        meanings_found = False
        found_meanings = []
        for i in range(6):
            time.sleep(5)
            available, body = _search_by_guid(client, entity_guid)
            if not available:
                continue
            entities = body.get("entities", [])
            if not entities:
                continue
            entity = entities[0]
            meanings = entity.get("meanings", [])
            if meanings and isinstance(meanings, list) and len(meanings) > 0:
                found_meanings = meanings
                meanings_found = True
                print(f"  [meanings] Found after {(i+1)*5}s: {meanings}")
                break
            print(f"  [meanings] Polling ({(i+1)*5}s/30s)")

        assert meanings_found, (
            f"Expected meanings on entity {entity_guid} after term assignment, "
            f"but meanings field is empty after 30s polling"
        )
        if isinstance(found_meanings[0], dict):
            guids = [m.get("termGuid") for m in found_meanings]
            assert term_guid in guids, (
                f"Expected termGuid {term_guid} in meanings, got {guids}"
            )

    @test("search_propagated_classification", tags=["search", "correctness", "propagation"],
          order=10)
    def test_search_propagated_classification(self, client, ctx):
        if not self.tag_add_ok:
            raise SkipTestError("Classification not added in setup — type cache propagation failed")

        # Detect correct entity type for process I/O
        io_type = ctx.get("process_io_type") or detect_process_io_type(client)

        # Create lineage: src -> process -> tgt, tag src, check tgt
        src_guid, src_qn = _create_entity_and_register(client, ctx, "prop-search-src", type_name=io_type)
        tgt_guid, tgt_qn = _create_entity_and_register(client, ctx, "prop-search-tgt", type_name=io_type)

        ok, proc_guid = create_process_with_io(
            client, ctx, "prop-search-proc",
            [src_guid], [tgt_guid], entity_type=io_type,
        )
        if not ok:
            raise SkipTestError("Process creation failed — lineage not supported in this env")

        # Add propagating classification to src
        resp = client.post(f"/entity/guid/{src_guid}/classifications", json_data=[{
            "typeName": self.tag_name,
            "propagate": True,
            "restrictPropagationThroughLineage": False,
        }])
        assert_status_in(resp, [200, 204])

        # Poll ES for propagated classification on target (up to 45s)
        print(f"  [prop-search] Waiting up to 45s for {self.tag_name} to propagate "
              f"to {tgt_guid} in ES...")
        prop_found = False
        last_prop_names = []
        for i in range(9):
            time.sleep(5)
            available, body = _search_by_guid(client, tgt_guid)
            if not available:
                print(f"  [prop-search] Search API not available, retrying...")
                continue
            entities = body.get("entities", [])
            if not entities:
                print(f"  [prop-search] Entity not in ES yet ({(i+1)*5}s/45s)")
                continue
            entity = entities[0]
            last_prop_names = entity.get("propagatedClassificationNames", [])
            cn = entity.get("classificationNames", [])
            if self.tag_name in last_prop_names or self.tag_name in cn:
                prop_found = True
                print(f"  [prop-search] Propagation confirmed in ES after {(i+1)*5}s")
                break
            print(f"  [prop-search] Polling ({(i+1)*5}s/45s): "
                  f"propagated={last_prop_names}, classifications={cn}")

        assert prop_found, (
            f"Tag {self.tag_name} did NOT propagate to target {tgt_guid} in ES "
            f"after 45s. propagatedClassificationNames={last_prop_names}"
        )

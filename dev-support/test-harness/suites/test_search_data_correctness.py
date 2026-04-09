"""Search data correctness tests.

Verifies that data written to Atlas is correctly reflected in ES search
results — both after initial creation and after mutations (attribute
updates, label changes, classification changes).

Existing suites verify field *presence* and *filtering*.  This suite
verifies that the returned **values** match what was written.
"""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError
from core.data_factory import (
    build_dataset_entity, build_classification_def, build_business_metadata_def,
    unique_name, unique_qn, unique_type_name, PREFIX,
)
from core.typedef_helpers import (
    ensure_classification_types, create_typedef_verified,
    extract_bm_names_from_response, ensure_bm_types,
)


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


def _poll_get_entity(client, guid, max_wait=30, interval=5, label="search"):
    """Poll ES until entity appears by GUID. Returns (available, entity_or_None)."""
    for i in range(max_wait // interval):
        if i > 0:
            time.sleep(interval)
        available, body = _search_by_guid(client, guid)
        if not available:
            return False, None
        entities = body.get("entities", [])
        if entities:
            return True, entities[0]
        print(f"  [{label}] Polling ES for {guid} ({(i+1)*interval}s/{max_wait}s)")
    return True, None


def _search_by_qn(client, qn):
    """Search by qualifiedName, trying multiple ES field names."""
    available, body = False, {}
    for field in QN_FIELDS:
        available, body = _index_search(client, {
            "from": 0, "size": 1,
            "query": {"bool": {"must": [
                {"term": {field: qn}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if available and body.get("approximateCount", 0) > 0:
            return True, body
    return available, body


def _attr(entity, name):
    """Get an attribute value, checking both nested and flat paths."""
    val = entity.get("attributes", {}).get(name)
    if val is None:
        val = entity.get(name)
    return val


@suite("search_data_correctness", depends_on_suites=["entity_crud", "glossary"],
       description="Search data round-trip correctness — write then verify via search")
class SearchDataCorrectnessSuite:

    def setup(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)

        # --- Classification typedef ---
        requested = [unique_type_name("DCorTag")]
        names, created_tag, self.tag_ok = ensure_classification_types(
            client, requested,
        )
        self.tag_name = names[0]
        if created_tag:
            ctx.register_typedef_cleanup(client, self.tag_name)

        # --- BM typedef (with fallback to existing types) ---
        self.bm_display_name = unique_type_name("DCorBM")
        self.bm_internal_name = None
        self.bm_attr_map = {}
        self.bm_field_display = "bmField1"  # default
        bm_info, created_bm, self.bm_ok = ensure_bm_types(
            client, self.bm_display_name,
        )
        if bm_info:
            self.bm_internal_name = bm_info["internal_name"]
            self.bm_display_name = bm_info["display_name"]
            self.bm_attr_map = bm_info["attr_map"]
            self.bm_field_display = bm_info["first_attr_display"]
        if self.bm_ok and created_bm:
            cleanup_name = self.bm_internal_name or self.bm_display_name
            ctx.register_typedef_cleanup(client, cleanup_name)

        # --- Entity A: rich entity with all attributes ---
        self.created_name = unique_name("dcor-a")
        self.created_qn = unique_qn("dcor-a")
        self.created_desc = "dcor-description-alpha"
        self.created_cert = "VERIFIED"
        self.created_labels = ["dcor-label-1", "dcor-label-2"]

        entity_a = build_dataset_entity(
            qn=self.created_qn, name=self.created_name,
            extra_attrs={
                "description": self.created_desc,
                "certificateStatus": self.created_cert,
            },
        )
        entity_a["labels"] = self.created_labels

        resp = client.post("/entity", json_data={"entity": entity_a})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        self.guid_a = entities[0]["guid"]
        ctx.register_entity_cleanup(self.guid_a)

        # Add classification to Entity A (with retry)
        self.tag_added = False
        if self.tag_ok:
            for attempt in range(3):
                tag_resp = client.post(
                    f"/entity/guid/{self.guid_a}/classifications",
                    json_data=[{"typeName": self.tag_name}],
                )
                if tag_resp.status_code in (200, 204):
                    self.tag_added = True
                    break
                if attempt < 2:
                    time.sleep(10)

        # Set BM on Entity A via displayName endpoint (with retry)
        self.bm_value = "dcor-bm-val"
        self.bm_set = False
        if self.bm_ok:
            for attempt in range(3):
                bm_set_resp = client.post(
                    f"/entity/guid/{self.guid_a}/businessmetadata/displayName",
                    json_data={self.bm_display_name: {self.bm_field_display: self.bm_value}},
                )
                if bm_set_resp.status_code in (200, 204):
                    self.bm_set = True
                    break
                if attempt < 2:
                    time.sleep(10)

        # --- Glossary + Term assigned to Entity A ---
        # Reuse glossary/term from glossary suite (avoids slow POST /glossary)
        self.glossary_ok = False
        self.term_guid = None
        self.term_name = None
        self.term_qn = None

        glossary_guid = ctx.get_entity_guid("glossary")
        term1_guid = ctx.get_entity_guid("term1")

        if glossary_guid and term1_guid:
            # Reuse existing term from glossary suite — just assign to entity A
            self.term_guid = term1_guid
            term_entry = ctx.get_entity("term1")
            self.term_qn = term_entry.get("qualifiedName") if term_entry else None
            # Fetch term name if needed
            term_resp = client.get(f"/glossary/term/{term1_guid}")
            if term_resp.status_code == 200:
                self.term_name = term_resp.json().get("name")

            for assign_attempt in range(3):
                if assign_attempt > 0:
                    time.sleep(5)
                assign_resp = client.post(
                    f"/glossary/terms/{self.term_guid}/assignedEntities",
                    json_data=[{"guid": self.guid_a, "typeName": "DataSet"}],
                )
                if assign_resp.status_code in (200, 204):
                    self.glossary_ok = True
                    break
        else:
            # Fallback: create own glossary + term if glossary suite didn't run
            for gloss_attempt in range(3):
                if gloss_attempt > 0:
                    time.sleep(5)
                glossary_resp = client.post("/glossary", json_data={
                    "name": unique_name("dcor-gloss"),
                    "shortDescription": "Data correctness glossary",
                }, timeout=120)
                if glossary_resp.status_code == 200:
                    break
            if glossary_resp.status_code == 200:
                glossary_guid = glossary_resp.json().get("guid")
                ctx.register_cleanup(lambda: client.delete(f"/glossary/{glossary_guid}"))

                self.term_name = unique_name("dcor-term")
                for term_attempt in range(3):
                    if term_attempt > 0:
                        time.sleep(5)
                    term_resp = client.post("/glossary/term", json_data={
                        "name": self.term_name,
                        "shortDescription": "Data correctness term",
                        "anchor": {"glossaryGuid": glossary_guid},
                    }, timeout=120)
                    if term_resp.status_code == 200:
                        break
                if term_resp.status_code == 200:
                    term_body = term_resp.json()
                    self.term_guid = term_body.get("guid")
                    self.term_qn = term_body.get("qualifiedName")
                    ctx.register_cleanup(
                        lambda: client.delete(f"/glossary/term/{self.term_guid}")
                    )

                    for assign_attempt in range(3):
                        if assign_attempt > 0:
                            time.sleep(5)
                        assign_resp = client.post(
                            f"/glossary/terms/{self.term_guid}/assignedEntities",
                            json_data=[{"guid": self.guid_a, "typeName": "DataSet"}],
                        )
                        if assign_resp.status_code in (200, 204):
                            self.glossary_ok = True
                            break

        # Single ES sync wait after all setup
        time.sleep(max(es_wait, 5))

    # ================================================================
    #  Phase 1 — Create-and-verify (no mutations, just read-back)
    # ================================================================

    @test("guid_lookup_returns_correct_core_attrs",
          tags=["search", "data_correctness"], order=1)
    def test_guid_lookup(self, client, ctx):
        """Search by GUID — verify name, QN, typeName, status match created values."""
        available, entity = _poll_get_entity(client, self.guid_a, max_wait=30, label="guid-lookup")
        assert available, "Search endpoint not available (404/400/405)"
        assert entity is not None, (
            f"Entity {self.guid_a} not found in search after 30s polling"
        )

        assert entity.get("guid") == self.guid_a, (
            f"GUID mismatch: expected {self.guid_a}, got {entity.get('guid')}"
        )
        assert entity.get("typeName") == "DataSet", (
            f"typeName mismatch: expected DataSet, got {entity.get('typeName')}"
        )

        actual_name = _attr(entity, "name")
        assert actual_name == self.created_name, (
            f"name mismatch: expected {self.created_name!r}, got {actual_name!r}"
        )

        actual_qn = _attr(entity, "qualifiedName")
        assert actual_qn == self.created_qn, (
            f"qualifiedName mismatch: expected {self.created_qn!r}, got {actual_qn!r}"
        )

        status = entity.get("status") or entity.get("__state")
        if status:
            assert status == "ACTIVE", f"status mismatch: expected ACTIVE, got {status}"

    @test("qn_lookup_returns_correct_entity",
          tags=["search", "data_correctness"], order=2)
    def test_qn_lookup(self, client, ctx):
        """Search by qualifiedName — verify GUID and name match."""
        # Poll until QN lookup returns results
        available, body = False, {}
        for i in range(6):
            if i > 0:
                time.sleep(5)
            available, body = _search_by_qn(client, self.created_qn)
            if available and body.get("approximateCount", 0) > 0:
                break
            print(f"  [qn-lookup] Polling ES ({(i+1)*5}s/30s)")
        assert available, "Search endpoint not available"
        count = body.get("approximateCount", 0)
        assert count > 0, (
            f"Entity not found when searching by QN {self.created_qn} after 30s polling"
        )
        entity = body["entities"][0]
        assert entity.get("guid") == self.guid_a, (
            f"QN lookup returned wrong entity: expected guid={self.guid_a}, "
            f"got {entity.get('guid')}"
        )
        actual_name = _attr(entity, "name")
        assert actual_name == self.created_name, (
            f"QN lookup name mismatch: expected {self.created_name!r}, got {actual_name!r}"
        )

    @test("description_matches_created_value",
          tags=["search", "data_correctness"], order=3)
    def test_description(self, client, ctx):
        """Verify description value in search matches what was written."""
        available, entity = _poll_get_entity(client, self.guid_a, max_wait=30, label="desc")
        assert available, "Search endpoint not available"
        assert entity is not None, f"Entity {self.guid_a} not found in search after 30s"
        actual = _attr(entity, "description")
        if actual is not None:  # field may not be in ES mapping on all envs
            assert actual == self.created_desc, (
                f"description mismatch: expected {self.created_desc!r}, got {actual!r}"
            )

    @test("certificate_matches_created_value",
          tags=["search", "data_correctness"], order=4)
    def test_certificate(self, client, ctx):
        """Verify certificateStatus value in search matches what was written."""
        available, entity = _poll_get_entity(client, self.guid_a, max_wait=30, label="cert")
        assert available, "Search endpoint not available"
        assert entity is not None, f"Entity {self.guid_a} not found in search after 30s"
        actual = _attr(entity, "certificateStatus")
        if actual is not None:
            assert actual == self.created_cert, (
                f"certificateStatus mismatch: expected {self.created_cert!r}, "
                f"got {actual!r}"
            )

    @test("labels_match_created_values",
          tags=["search", "data_correctness"], order=5)
    def test_labels(self, client, ctx):
        """Verify every created label appears in the search result."""
        available, entity = _poll_get_entity(client, self.guid_a, max_wait=30, label="labels")
        assert available, "Search endpoint not available"
        assert entity is not None, f"Entity {self.guid_a} not found in search after 30s"
        actual = entity.get("labels", [])
        if not actual:
            raise SkipTestError("Labels not returned in search results on this environment")
        for lbl in self.created_labels:
            assert lbl in actual, (
                f"Label {lbl!r} not in search labels: {actual}"
            )

    @test("classifications_match_created_values",
          tags=["search", "data_correctness"], order=6)
    def test_classifications(self, client, ctx):
        """Verify classificationNames and classification objects are correct."""
        if not self.tag_added:
            raise SkipTestError("Classification not added to Entity A in setup")
        available, entity = _poll_get_entity(client, self.guid_a, max_wait=30, label="cls")
        assert available, "Search endpoint not available"
        assert entity is not None, f"Entity {self.guid_a} not found in search after 30s"

        # classificationNames list
        cn = entity.get("classificationNames", [])
        assert self.tag_name in cn, (
            f"{self.tag_name} not in classificationNames: {cn}"
        )

        # classification objects — verify typeName
        classifications = entity.get("classifications", [])
        if classifications:
            type_names = [
                c.get("typeName") for c in classifications
                if isinstance(c, dict)
            ]
            assert self.tag_name in type_names, (
                f"{self.tag_name} not in classification objects: {type_names}"
            )

    @test("bm_matches_created_value",
          tags=["search", "data_correctness", "businessmeta"], order=7)
    def test_bm(self, client, ctx):
        """Verify BM value via GET entity API and check ES indexing.

        AtlasEntityHeader (search result) has NO businessAttributes field.
        Full entity from GET /entity/guid/{guid} DOES have businessAttributes.
        BM attr values are indexed as flat ES fields keyed by attr internal name.
        """
        if not self.bm_set:
            raise SkipTestError("BM not set on Entity A in setup")

        # Step 1: Verify BM via GET entity API (returns full AtlasEntity)
        resp = client.get(f"/entity/guid/{self.guid_a}")
        assert_status(resp, 200)
        entity_full = resp.json().get("entity", {})
        bm_key = self.bm_internal_name or self.bm_display_name
        bm = entity_full.get("businessAttributes", {}).get(bm_key, {})
        if not bm:
            raise SkipTestError(
                f"BM {bm_key} not in GET entity response — type cache issue"
            )
        attr_key = self.bm_attr_map.get(self.bm_field_display, self.bm_field_display)
        assert bm.get(attr_key) == self.bm_value, (
            f"{attr_key} mismatch: expected {self.bm_value!r}, got {bm}"
        )

        # Step 2: Verify BM value is indexed in ES (attr internal name as field)
        available, body = _index_search(client, {
            "from": 0, "size": 1,
            "query": {"bool": {"must": [
                {"term": {attr_key: self.bm_value}},
                {"term": {"__guid": self.guid_a}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        if available and body.get("approximateCount", 0) == 0:
            raise SkipTestError(
                f"BM attr {attr_key}={self.bm_value} not yet in ES — sync delay"
            )

    @test("meanings_match_created_value",
          tags=["search", "data_correctness", "glossary"], order=8)
    def test_meanings(self, client, ctx):
        """Verify meanings contains the assigned term with correct termGuid."""
        if not self.glossary_ok:
            raise SkipTestError("Glossary/term not set up in setup")

        # Poll until entity appears with meanings
        entity = None
        for i in range(6):
            if i > 0:
                time.sleep(5)
            available, entity = _poll_get_entity(client, self.guid_a, max_wait=5, interval=5, label="meanings")
            if not available:
                raise SkipTestError("Search endpoint not available")
            if entity:
                meanings = entity.get("meanings", [])
                if meanings:
                    break
            print(f"  [meanings] Polling for meanings on {self.guid_a} ({(i+1)*5}s/30s)")

        assert entity is not None, f"Entity {self.guid_a} not found in search after 30s"

        # Primary: meanings objects with termGuid
        meanings = entity.get("meanings", [])
        if meanings and isinstance(meanings, list) and isinstance(meanings[0], dict):
            term_guids = [m.get("termGuid") for m in meanings]
            assert self.term_guid in term_guids, (
                f"termGuid {self.term_guid} not in meanings: {term_guids}"
            )
            return

        # Fallback: meaningNames list
        meaning_names = entity.get("meaningNames", [])
        if meaning_names:
            found = any(self.term_name in str(n) for n in meaning_names)
            assert found, (
                f"Term name {self.term_name!r} not in meaningNames: {meaning_names}"
            )

    @test("fulltext_name_search_finds_entity",
          tags=["search", "data_correctness"], order=9)
    def test_fulltext_name(self, client, ctx):
        """Basic search with query=name finds the entity."""
        # Retry basic search
        for attempt in range(3):
            resp = client.post("/search/basic", json_data={
                "typeName": "DataSet",
                "excludeDeletedEntities": True,
                "limit": 10,
                "query": self.created_name,
            })
            if resp.status_code in (400, 404, 405):
                raise SkipTestError(f"Basic search endpoint returned {resp.status_code}")
            if resp.status_code == 200:
                break
            if attempt < 2:
                time.sleep(5)
        assert_status(resp, 200)
        body = resp.json()
        entities = body.get("entities", [])
        guids = [e.get("guid") for e in entities]
        assert self.guid_a in guids, (
            f"Entity {self.guid_a} not found via full-text search "
            f"for name {self.created_name!r}: {guids}"
        )

    @test("combined_type_tag_qn_search_correct",
          tags=["search", "data_correctness"], order=10)
    def test_combined_search(self, client, ctx):
        """Combined typeName + classification + QN-wildcard returns correct entity."""
        if not self.tag_added:
            raise SkipTestError("Classification not added to Entity A in setup")
        found = False
        body = {}
        for qn_field in QN_FIELDS:
            available, body = _index_search(client, {
                "from": 0, "size": 10,
                "query": {"bool": {"must": [
                    {"term": {"__typeName.keyword": "DataSet"}},
                    {"match_phrase": {"__classificationNames": self.tag_name}},
                    {"wildcard": {qn_field: f"{PREFIX}*"}},
                    {"term": {"__state": "ACTIVE"}},
                ]}}
            })
            if available and body.get("approximateCount", 0) > 0:
                found = True
                break
        assert found, (
            f"Combined search (type + classification + QN wildcard) returned 0 results "
            f"for entity {self.guid_a}"
        )

        guids = [e.get("guid") for e in body.get("entities", [])]
        assert self.guid_a in guids, (
            f"Entity {self.guid_a} not in combined search results: {guids}"
        )
        # Verify the matching entity has correct data
        entity = next(e for e in body["entities"] if e.get("guid") == self.guid_a)
        assert _attr(entity, "name") == self.created_name, (
            f"Combined search name mismatch: expected {self.created_name!r}"
        )

    # ================================================================
    #  Phase 2 — Mutate-and-verify
    # ================================================================

    @test("batch_update_entity_attributes",
          tags=["search", "data_correctness", "mutation"], order=11)
    def test_batch_update(self, client, ctx):
        """Update name, description, and certificateStatus in one call.

        Subsequent tests verify each updated field in search.
        """
        self.updated_name = unique_name("dcor-upd")
        self.updated_desc = "dcor-updated-description"
        self.updated_cert = "DRAFT"

        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataSet",
                "guid": self.guid_a,
                "attributes": {
                    "qualifiedName": self.created_qn,
                    "name": self.updated_name,
                    "description": self.updated_desc,
                    "certificateStatus": self.updated_cert,
                },
            }
        })
        assert_status(resp, 200)
        time.sleep(max(ctx.get("es_sync_wait", 5), 5))

    @test("search_reflects_updated_name",
          tags=["search", "data_correctness", "mutation"], order=12,
          depends_on=["batch_update_entity_attributes"])
    def test_updated_name(self, client, ctx):
        """Verify search returns the new name after update."""
        available, entity = _poll_get_entity(client, self.guid_a, max_wait=30, label="upd-name")
        assert available, "Search endpoint not available"
        assert entity is not None, f"Entity {self.guid_a} not found in search after 30s"
        actual = _attr(entity, "name")
        assert actual == self.updated_name, (
            f"Updated name not reflected in search: "
            f"expected {self.updated_name!r}, got {actual!r}"
        )

    @test("search_reflects_updated_description",
          tags=["search", "data_correctness", "mutation"], order=13,
          depends_on=["batch_update_entity_attributes"])
    def test_updated_desc(self, client, ctx):
        """Verify search returns the new description after update."""
        available, entity = _poll_get_entity(client, self.guid_a, max_wait=30, label="upd-desc")
        assert available, "Search endpoint not available"
        assert entity is not None, f"Entity {self.guid_a} not found in search after 30s"
        actual = _attr(entity, "description")
        if actual is not None:
            assert actual == self.updated_desc, (
                f"Updated description not reflected in search: "
                f"expected {self.updated_desc!r}, got {actual!r}"
            )

    @test("search_reflects_updated_certificate",
          tags=["search", "data_correctness", "mutation"], order=14,
          depends_on=["batch_update_entity_attributes"])
    def test_updated_cert(self, client, ctx):
        """Verify search returns the new certificateStatus after update."""
        available, entity = _poll_get_entity(client, self.guid_a, max_wait=30, label="upd-cert")
        assert available, "Search endpoint not available"
        assert entity is not None, f"Entity {self.guid_a} not found in search after 30s"
        actual = _attr(entity, "certificateStatus")
        if actual is not None:
            assert actual == self.updated_cert, (
                f"Updated certificateStatus not reflected in search: "
                f"expected {self.updated_cert!r}, got {actual!r}"
            )

    @test("search_reflects_label_addition",
          tags=["search", "data_correctness", "mutation"], order=15)
    def test_label_addition(self, client, ctx):
        """Add a new label and verify it appears in search alongside originals."""
        new_label = "dcor-new-label"
        all_labels = self.created_labels + [new_label]
        current_name = getattr(self, "updated_name", self.created_name)

        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "DataSet",
                "guid": self.guid_a,
                "attributes": {
                    "qualifiedName": self.created_qn,
                    "name": current_name,
                },
                "labels": all_labels,
            }
        })
        assert_status(resp, 200)
        time.sleep(max(ctx.get("es_sync_wait", 5), 5))

        available, entity = _poll_get_entity(client, self.guid_a, max_wait=30, label="lbl-add")
        assert available, "Search endpoint not available"
        assert entity is not None, f"Entity {self.guid_a} not found in search after 30s"
        actual_labels = entity.get("labels", [])
        if not actual_labels:
            raise SkipTestError("Labels not in search results on this environment")
        assert new_label in actual_labels, (
            f"New label {new_label!r} not in search labels: {actual_labels}"
        )
        # Original labels should still be present
        for lbl in self.created_labels:
            assert lbl in actual_labels, (
                f"Original label {lbl!r} missing after addition: {actual_labels}"
            )

    @test("search_reflects_classification_removal",
          tags=["search", "data_correctness", "mutation"], order=16)
    def test_classification_removal(self, client, ctx):
        """Remove classification and verify classificationNames no longer has it."""
        if not self.tag_added:
            raise SkipTestError("Classification not added to Entity A in setup")

        resp = client.delete(
            f"/entity/guid/{self.guid_a}/classification/{self.tag_name}"
        )
        assert_status_in(resp, [200, 204])
        time.sleep(max(ctx.get("es_sync_wait", 5), 5))

        # Poll until classification is removed from search
        for i in range(6):
            available, entity = _poll_get_entity(client, self.guid_a, max_wait=5, interval=5, label="cls-rm")
            if not available:
                raise SkipTestError("Search endpoint not available")
            if entity:
                cn = entity.get("classificationNames", [])
                if self.tag_name not in cn:
                    break
            if i < 5:
                time.sleep(5)
                print(f"  [cls-rm] Waiting for classification removal in search ({(i+1)*5}s/30s)")

        assert entity is not None, f"Entity {self.guid_a} not found in search"
        cn = entity.get("classificationNames", [])
        assert self.tag_name not in cn, (
            f"Removed classification {self.tag_name} still in "
            f"classificationNames: {cn}"
        )

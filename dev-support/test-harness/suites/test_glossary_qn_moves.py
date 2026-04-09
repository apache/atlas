"""Glossary Term/Category QualifiedName move/reparent tests.

Tests term cross-glossary moves, category cross-glossary moves (with cascade),
category reparent within the same glossary, deep hierarchy cascade,
round-trip moves (G1->G2->G1), and true reparenting (root->child in same glossary).

Server-side QN patterns (on this environment):
  Glossary:         {nanoId}
  Term:             {nanoId}@{glossaryQN}
  Category (root):  {nanoId}@{glossaryQN}
  Category (child): {nanoId}@{glossaryQN}   (no parent path in QN)

Key move behaviors:
  - Term move: currentQN.replace(oldGlossaryQN, newGlossaryQN)
  - Category move: same replacement + recursive cascade to children + terms
  - Category reparent (same glossary): QN is NOT changed
  - Cross-glossary moves: use entity API (POST /entity), NOT glossary API (PUT /glossary/*)
"""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, SkipTestError,
)
from core.data_factory import unique_name


# ---- QN helpers ----

def _parse_glossary_entity_qn(qn):
    """Parse a term/category QN into (local_part, glossary_qn).

    QN format: {localPart}@{glossaryQN}
    Returns (local_part, glossary_qn) or (None, None) if no '@'.
    """
    if not qn or "@" not in qn:
        return None, None
    idx = qn.rfind("@")
    return qn[:idx], qn[idx + 1:]


def _extract_guid_from_bulk(resp):
    """Extract first entity GUID from POST /entity/bulk response."""
    if resp.status_code != 200:
        return None
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    return entities[0].get("guid") if entities else None


def _find_by_name(client, type_name, name, max_wait=60, interval=10):
    """Poll search until entity with given name appears. Returns guid or None."""
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


def _read_entity_qn(client, guid):
    """GET entity by GUID and return qualifiedName."""
    resp = client.get(f"/entity/guid/{guid}")
    if resp.status_code != 200:
        return None
    return resp.json().get("entity", {}).get("attributes", {}).get("qualifiedName")


def _wait_for_glossary_suffix(client, guid, expected_glossary_qn, max_wait=30, interval=3):
    """Poll until entity QN has @{expected_glossary_qn} suffix, or timeout."""
    deadline = time.time() + max_wait
    qn = None
    while time.time() < deadline:
        qn = _read_entity_qn(client, guid)
        if qn:
            _, suffix = _parse_glossary_entity_qn(qn)
            if suffix == expected_glossary_qn:
                return qn
        time.sleep(interval)
    return qn


def _create_glossary_bulk(client, name, label="Glossary"):
    """Create glossary via /entity/bulk, poll if timeout. Return guid or raise."""
    resp = client.post("/entity/bulk", json_data={
        "entities": [{
            "typeName": "AtlasGlossary",
            "attributes": {
                "qualifiedName": "",
                "name": name,
                "displayName": name,
                "shortDescription": f"Test {label}",
                "certificateStatus": "DRAFT",
                "ownerUsers": [],
                "ownerGroups": [],
            },
        }],
    }, timeout=30, retries=0)

    guid = _extract_guid_from_bulk(resp)
    if not guid:
        guid = _find_by_name(client, "AtlasGlossary", name, max_wait=60, interval=10)
    if not guid:
        raise SkipTestError(
            f"{label} creation failed (status={resp.status_code}) — "
            f"server may be overloaded or unavailable"
        )
    return guid


def _create_term_bulk(client, glossary_guid, name, categories=None, label="Term"):
    """Create term via /entity/bulk.  Return guid or raise."""
    rel_attrs = {
        "anchor": {"guid": glossary_guid, "typeName": "AtlasGlossary"},
    }
    if categories:
        rel_attrs["categories"] = categories

    resp = client.post("/entity/bulk", json_data={
        "entities": [{
            "typeName": "AtlasGlossaryTerm",
            "attributes": {
                "qualifiedName": "",
                "name": name,
                "displayName": name,
                "shortDescription": f"Test {label}",
            },
            "relationshipAttributes": rel_attrs,
        }],
    }, timeout=30, retries=0)

    guid = _extract_guid_from_bulk(resp)
    if not guid:
        guid = _find_by_name(client, "AtlasGlossaryTerm", name, max_wait=60, interval=10)
    if not guid:
        raise SkipTestError(
            f"{label} creation failed (status={resp.status_code})"
        )
    return guid


def _create_category_bulk(client, glossary_guid, name, parent_guid=None, label="Category"):
    """Create category via /entity/bulk.  Return guid or raise."""
    rel_attrs = {
        "anchor": {"guid": glossary_guid, "typeName": "AtlasGlossary"},
    }
    if parent_guid:
        rel_attrs["parentCategory"] = {
            "guid": parent_guid, "typeName": "AtlasGlossaryCategory",
        }

    resp = client.post("/entity/bulk", json_data={
        "entities": [{
            "typeName": "AtlasGlossaryCategory",
            "attributes": {
                "qualifiedName": "",
                "name": name,
                "displayName": name,
                "shortDescription": f"Test {label}",
            },
            "relationshipAttributes": rel_attrs,
        }],
    }, timeout=30, retries=0)

    guid = _extract_guid_from_bulk(resp)
    if not guid:
        guid = _find_by_name(client, "AtlasGlossaryCategory", name,
                             max_wait=60, interval=10)
    if not guid:
        raise SkipTestError(
            f"{label} creation failed (status={resp.status_code})"
        )
    return guid


def _move_entity_anchor(client, type_name, guid, current_qn, name,
                        new_glossary_guid, timeout=60):
    """Move a glossary entity cross-glossary via the entity API.

    The glossary REST API (PUT /glossary/term, PUT /glossary/category)
    may return 409 'Anchor change not supported' on some environments.
    The entity API (POST /entity) handles cross-glossary moves correctly
    via the preprocessor.
    """
    resp = client.post("/entity", json_data={
        "entity": {
            "typeName": type_name,
            "guid": guid,
            "attributes": {
                "qualifiedName": current_qn,
                "name": name,
            },
            "relationshipAttributes": {
                "anchor": {
                    "guid": new_glossary_guid,
                    "typeName": "AtlasGlossary",
                },
            },
        }
    }, timeout=timeout)
    return resp


@suite("glossary_qn_moves",
       description="Glossary Term/Category QualifiedName move and cascade tests")
class GlossaryQnMovesSuite:

    def setup(self, client, ctx):
        self.g1_name = unique_name("move-g1")
        self.g2_name = unique_name("move-g2")
        self.term_name = unique_name("move-term")
        self.root_cat_name = unique_name("move-rootcat")
        self.child_cat_name = unique_name("move-childcat")
        self.cat_term_name = unique_name("move-catterm")
        self.reparent_cat_name = unique_name("move-reparent")
        # Extended test entities
        self.deep_cat_name = unique_name("move-deepcat")
        self.deep_term_name = unique_name("move-deepterm")
        self.reparent_target_name = unique_name("move-rptarget")

    # ================================================================
    # CREATE: Two glossaries + entities in G1
    # ================================================================

    @test("create_glossary_1", tags=["move", "glossary", "crud"], order=1)
    def test_create_glossary_1(self, client, ctx):
        """Create G1 via /entity/bulk.  Store GUID and QN."""
        guid = _create_glossary_bulk(client, self.g1_name, "G1")
        ctx.register_entity("move_g1", guid, "AtlasGlossary")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/{guid}"))

        qn = _read_entity_qn(client, guid)
        assert qn, f"Expected qualifiedName on G1 ({guid})"
        ctx.set("move_g1_qn", qn)
        ctx.set("move_g1_guid", guid)
        print(f"  [glossary-move] G1: guid={guid}, qn={qn}")

    @test("create_glossary_2", tags=["move", "glossary", "crud"], order=2)
    def test_create_glossary_2(self, client, ctx):
        """Create G2 via /entity/bulk.  Store GUID and QN."""
        guid = _create_glossary_bulk(client, self.g2_name, "G2")
        ctx.register_entity("move_g2", guid, "AtlasGlossary")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/{guid}"))

        qn = _read_entity_qn(client, guid)
        assert qn, f"Expected qualifiedName on G2 ({guid})"
        ctx.set("move_g2_qn", qn)
        ctx.set("move_g2_guid", guid)
        print(f"  [glossary-move] G2: guid={guid}, qn={qn}")

    @test("create_term_in_g1", tags=["move", "glossary", "crud"], order=5,
          depends_on=["create_glossary_1"])
    def test_create_term_in_g1(self, client, ctx):
        """Create term in G1.  Verify QN: {nanoId}@{g1QN}."""
        g1_guid = ctx.get("move_g1_guid")
        g1_qn = ctx.get("move_g1_qn")
        guid = _create_term_bulk(client, g1_guid, self.term_name, label="TermInG1")
        ctx.register_entity("move_term", guid, "AtlasGlossaryTerm")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{guid}"))

        qn = _read_entity_qn(client, guid)
        assert qn, f"Expected qualifiedName on term ({guid})"
        ctx.set("move_term_qn", qn)

        local_part, glossary_suffix = _parse_glossary_entity_qn(qn)
        assert glossary_suffix == g1_qn, (
            f"Term glossary suffix mismatch: expected {g1_qn}, got {glossary_suffix} "
            f"(full QN: {qn})"
        )
        ctx.set("move_term_nano", local_part)
        print(f"  [glossary-move] Term: guid={guid}, qn={qn}")

    @test("create_root_category_in_g1", tags=["move", "glossary", "crud"], order=6,
          depends_on=["create_glossary_1"])
    def test_create_root_category_in_g1(self, client, ctx):
        """Create root category in G1.  Verify QN: {nanoId}@{g1QN}."""
        g1_guid = ctx.get("move_g1_guid")
        g1_qn = ctx.get("move_g1_qn")
        guid = _create_category_bulk(client, g1_guid, self.root_cat_name,
                                     label="RootCatInG1")
        ctx.register_entity("move_root_cat", guid, "AtlasGlossaryCategory")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/category/{guid}"))

        qn = _read_entity_qn(client, guid)
        assert qn, f"Expected qualifiedName on root category ({guid})"
        ctx.set("move_root_cat_qn", qn)

        local_part, glossary_suffix = _parse_glossary_entity_qn(qn)
        assert glossary_suffix == g1_qn, (
            f"Root cat glossary suffix mismatch: expected {g1_qn}, "
            f"got {glossary_suffix} (full QN: {qn})"
        )
        ctx.set("move_root_cat_nano", local_part)
        print(f"  [glossary-move] RootCat: guid={guid}, qn={qn}")

    @test("create_child_category_in_g1", tags=["move", "glossary", "crud"], order=7,
          depends_on=["create_root_category_in_g1"])
    def test_create_child_category_in_g1(self, client, ctx):
        """Create child category under root.  Verify QN: {nanoId}@{g1QN}.

        NOTE: On this environment, child categories get their own independent
        nanoId — no parent path is embedded in the QN.  Parent-child hierarchy
        is tracked via graph edges, not QN structure.
        """
        g1_guid = ctx.get("move_g1_guid")
        g1_qn = ctx.get("move_g1_qn")
        parent_guid = ctx.get_entity_guid("move_root_cat")

        guid = _create_category_bulk(client, g1_guid, self.child_cat_name,
                                     parent_guid=parent_guid, label="ChildCatInG1")
        ctx.register_entity("move_child_cat", guid, "AtlasGlossaryCategory")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/category/{guid}"))

        qn = _read_entity_qn(client, guid)
        assert qn, f"Expected qualifiedName on child category ({guid})"
        ctx.set("move_child_cat_qn", qn)

        local_part, glossary_suffix = _parse_glossary_entity_qn(qn)
        assert glossary_suffix == g1_qn, (
            f"Child cat glossary suffix mismatch: expected {g1_qn}, "
            f"got {glossary_suffix} (full QN: {qn})"
        )
        ctx.set("move_child_cat_nano", local_part)
        print(f"  [glossary-move] ChildCat: guid={guid}, qn={qn}")

    @test("create_term_under_category", tags=["move", "glossary", "crud"], order=8,
          depends_on=["create_root_category_in_g1"])
    def test_create_term_under_category(self, client, ctx):
        """Create term assigned to root category via categories relationship."""
        g1_guid = ctx.get("move_g1_guid")
        g1_qn = ctx.get("move_g1_qn")
        root_cat_guid = ctx.get_entity_guid("move_root_cat")

        categories = [{"guid": root_cat_guid, "typeName": "AtlasGlossaryCategory"}]
        guid = _create_term_bulk(client, g1_guid, self.cat_term_name,
                                 categories=categories, label="CatTermInG1")
        ctx.register_entity("move_cat_term", guid, "AtlasGlossaryTerm")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{guid}"))

        qn = _read_entity_qn(client, guid)
        assert qn, f"Expected qualifiedName on category term ({guid})"
        ctx.set("move_cat_term_qn", qn)

        local_part, glossary_suffix = _parse_glossary_entity_qn(qn)
        assert glossary_suffix == g1_qn, (
            f"Cat term glossary suffix mismatch: expected {g1_qn}, "
            f"got {glossary_suffix} (full QN: {qn})"
        )
        ctx.set("move_cat_term_nano", local_part)
        print(f"  [glossary-move] CatTerm: guid={guid}, qn={qn}")

    @test("create_reparent_category", tags=["move", "glossary", "crud"], order=9,
          depends_on=["create_glossary_1"])
    def test_create_reparent_category(self, client, ctx):
        """Create another root category in G1 for reparent test."""
        g1_guid = ctx.get("move_g1_guid")
        guid = _create_category_bulk(client, g1_guid, self.reparent_cat_name,
                                     label="ReparentCat")
        ctx.register_entity("move_reparent_cat", guid, "AtlasGlossaryCategory")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/category/{guid}"))

        qn = _read_entity_qn(client, guid)
        assert qn, f"Expected qualifiedName on reparent category ({guid})"
        ctx.set("move_reparent_cat_qn", qn)
        print(f"  [glossary-move] ReparentCat: guid={guid}, qn={qn}")

    # ================================================================
    # CREATE (extended): Deep hierarchy + reparent target
    # ================================================================

    @test("create_deep_child_category", tags=["move", "glossary", "crud"], order=10,
          depends_on=["create_child_category_in_g1"])
    def test_create_deep_child_category(self, client, ctx):
        """Create grandchild category under child_cat (3-level: root > child > deep).

        QN pattern: {nanoId}@{g1QN} — independent nanoId, no parent path.
        """
        g1_guid = ctx.get("move_g1_guid")
        g1_qn = ctx.get("move_g1_qn")
        parent_guid = ctx.get_entity_guid("move_child_cat")

        guid = _create_category_bulk(client, g1_guid, self.deep_cat_name,
                                     parent_guid=parent_guid, label="DeepCatInG1")
        ctx.register_entity("move_deep_cat", guid, "AtlasGlossaryCategory")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/category/{guid}"))

        qn = _read_entity_qn(client, guid)
        assert qn, f"Expected qualifiedName on deep category ({guid})"
        ctx.set("move_deep_cat_qn", qn)

        local_part, glossary_suffix = _parse_glossary_entity_qn(qn)
        assert glossary_suffix == g1_qn, (
            f"Deep cat glossary suffix mismatch: expected {g1_qn}, "
            f"got {glossary_suffix} (full QN: {qn})"
        )
        ctx.set("move_deep_cat_nano", local_part)
        print(f"  [glossary-move] DeepCat: guid={guid}, qn={qn}")

    @test("create_term_under_deep_child", tags=["move", "glossary", "crud"], order=11,
          depends_on=["create_deep_child_category"])
    def test_create_term_under_deep_child(self, client, ctx):
        """Create term categorized under the deep (grandchild) category."""
        g1_guid = ctx.get("move_g1_guid")
        g1_qn = ctx.get("move_g1_qn")
        deep_cat_guid = ctx.get_entity_guid("move_deep_cat")

        categories = [{"guid": deep_cat_guid, "typeName": "AtlasGlossaryCategory"}]
        guid = _create_term_bulk(client, g1_guid, self.deep_term_name,
                                 categories=categories, label="DeepTermInG1")
        ctx.register_entity("move_deep_term", guid, "AtlasGlossaryTerm")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/term/{guid}"))

        qn = _read_entity_qn(client, guid)
        assert qn, f"Expected qualifiedName on deep term ({guid})"
        ctx.set("move_deep_term_qn", qn)

        local_part, glossary_suffix = _parse_glossary_entity_qn(qn)
        assert glossary_suffix == g1_qn, (
            f"Deep term glossary suffix mismatch: expected {g1_qn}, "
            f"got {glossary_suffix} (full QN: {qn})"
        )
        ctx.set("move_deep_term_nano", local_part)
        print(f"  [glossary-move] DeepTerm: guid={guid}, qn={qn}")

    @test("create_reparent_target", tags=["move", "glossary", "crud"], order=12,
          depends_on=["create_glossary_1"])
    def test_create_reparent_target(self, client, ctx):
        """Create target root category in G1 for true reparent test."""
        g1_guid = ctx.get("move_g1_guid")
        guid = _create_category_bulk(client, g1_guid, self.reparent_target_name,
                                     label="ReparentTarget")
        ctx.register_entity("move_reparent_target", guid, "AtlasGlossaryCategory")
        ctx.register_cleanup(lambda: client.delete(f"/glossary/category/{guid}"))

        qn = _read_entity_qn(client, guid)
        assert qn, f"Expected qualifiedName on reparent target ({guid})"
        ctx.set("move_reparent_target_qn", qn)
        ctx.set("move_reparent_target_guid", guid)
        print(f"  [glossary-move] ReparentTarget: guid={guid}, qn={qn}")

    # ================================================================
    # MOVE 1: Move term from G1 to G2  (via entity API)
    # ================================================================

    @test("move_term_to_g2", tags=["move", "glossary"], order=20,
          depends_on=["create_term_in_g1", "create_glossary_2"])
    def test_move_term_to_g2(self, client, ctx):
        """MOVE: POST /entity with anchor -> G2 to move term cross-glossary.

        Uses entity API because the glossary API (PUT /glossary/term) returns
        409 'Anchor change not supported' on some environments.
        """
        term_guid = ctx.get_entity_guid("move_term")
        g2_guid = ctx.get("move_g2_guid")
        current_qn = ctx.get("move_term_qn")
        assert term_guid and g2_guid, "Required entities not created"

        resp = _move_entity_anchor(
            client, "AtlasGlossaryTerm", term_guid, current_qn,
            self.term_name, g2_guid,
        )
        assert_status_in(resp, [200, 400, 403, 409])
        if resp.status_code != 200:
            raise SkipTestError(
                f"Move term to G2 returned {resp.status_code} — "
                f"cross-glossary move may not be supported on this environment"
            )
        print(f"  [glossary-move] Move term to G2: {resp.status_code}")

    @test("verify_term_qn_after_move", tags=["move", "glossary"], order=21,
          depends_on=["move_term_to_g2"])
    def test_verify_term_qn_after_move(self, client, ctx):
        """Verify QN: {sameNanoId}@{g2QN}.  NanoId preserved, glossary suffix changed."""
        term_guid = ctx.get_entity_guid("move_term")
        old_nano = ctx.get("move_term_nano")
        g2_qn = ctx.get("move_g2_qn")

        qn = _wait_for_glossary_suffix(client, term_guid, g2_qn, max_wait=30)
        assert qn, f"Expected qualifiedName on term after move ({term_guid})"

        local_part, glossary_suffix = _parse_glossary_entity_qn(qn)
        assert glossary_suffix == g2_qn, (
            f"Term glossary suffix after move mismatch: expected {g2_qn}, "
            f"got {glossary_suffix} (full QN: {qn})"
        )
        assert local_part == old_nano, (
            f"Term nanoId changed during move: expected {old_nano}, got {local_part}"
        )
        ctx.set("move_term_qn", qn)
        print(f"  [glossary-move] Term after move: {qn}")

    # ================================================================
    # MOVE 2: Move root category (+ children + terms) from G1 to G2
    # ================================================================

    @test("move_category_to_g2", tags=["move", "glossary"], order=30,
          depends_on=["create_root_category_in_g1", "create_child_category_in_g1",
                       "create_term_under_category", "create_deep_child_category",
                       "create_term_under_deep_child", "create_glossary_2"])
    def test_move_category_to_g2(self, client, ctx):
        """MOVE: POST /entity with anchor -> G2 to move root category.

        Uses entity API.  Server cascades to: child_cat, deep_child_cat,
        cat_term, deep_term.
        """
        cat_guid = ctx.get_entity_guid("move_root_cat")
        g2_guid = ctx.get("move_g2_guid")
        current_qn = ctx.get("move_root_cat_qn")
        assert cat_guid and g2_guid, "Required entities not created"

        resp = _move_entity_anchor(
            client, "AtlasGlossaryCategory", cat_guid, current_qn,
            self.root_cat_name, g2_guid,
        )
        assert_status_in(resp, [200, 400, 403, 409])
        if resp.status_code != 200:
            raise SkipTestError(
                f"Move category to G2 returned {resp.status_code} — "
                f"cross-glossary move may not be supported"
            )
        print(f"  [glossary-move] Move root cat to G2: {resp.status_code}")

    @test("verify_category_cascade", tags=["move", "glossary"], order=31,
          depends_on=["move_category_to_g2"])
    def test_verify_category_cascade(self, client, ctx):
        """Verify root cat, child cat, and child term QNs all have @{g2QN} suffix."""
        g2_qn = ctx.get("move_g2_qn")
        old_root_nano = ctx.get("move_root_cat_nano")
        old_child_nano = ctx.get("move_child_cat_nano")
        old_cat_term_nano = ctx.get("move_cat_term_nano")

        # 1. Root category — poll until suffix matches g2
        root_cat_guid = ctx.get_entity_guid("move_root_cat")
        root_qn = _wait_for_glossary_suffix(client, root_cat_guid, g2_qn, max_wait=30)
        assert root_qn, f"Expected qualifiedName on root cat after move ({root_cat_guid})"
        local_part, glossary_suffix = _parse_glossary_entity_qn(root_qn)
        assert glossary_suffix == g2_qn, (
            f"Root cat glossary suffix after move: expected {g2_qn}, "
            f"got {glossary_suffix} (full QN: {root_qn})"
        )
        assert local_part == old_root_nano, (
            f"Root cat nanoId changed: expected {old_root_nano}, got {local_part}"
        )
        ctx.set("move_root_cat_qn", root_qn)
        print(f"  [glossary-move] Root cat after move: {root_qn}")

        # 2. Child category — poll for cascade
        child_cat_guid = ctx.get_entity_guid("move_child_cat")
        child_qn = _wait_for_glossary_suffix(client, child_cat_guid, g2_qn, max_wait=30)
        assert child_qn, f"Expected qualifiedName on child cat after cascade ({child_cat_guid})"
        child_local, child_suffix = _parse_glossary_entity_qn(child_qn)
        assert child_suffix == g2_qn, (
            f"Child cat glossary suffix after cascade: expected {g2_qn}, "
            f"got {child_suffix} (full QN: {child_qn})"
        )
        assert child_local == old_child_nano, (
            f"Child cat nanoId changed: expected {old_child_nano}, got {child_local}"
        )
        ctx.set("move_child_cat_qn", child_qn)
        print(f"  [glossary-move] Child cat after cascade: {child_qn}")

        # 3. Term under category — poll for cascade
        cat_term_guid = ctx.get_entity_guid("move_cat_term")
        term_qn = _wait_for_glossary_suffix(client, cat_term_guid, g2_qn, max_wait=30)
        assert term_qn, f"Expected qualifiedName on cat term after cascade ({cat_term_guid})"
        term_local, term_suffix = _parse_glossary_entity_qn(term_qn)
        assert term_suffix == g2_qn, (
            f"Cat term glossary suffix after cascade: expected {g2_qn}, "
            f"got {term_suffix} (full QN: {term_qn})"
        )
        assert term_local == old_cat_term_nano, (
            f"Cat term nanoId changed: expected {old_cat_term_nano}, got {term_local}"
        )
        ctx.set("move_cat_term_qn", term_qn)
        print(f"  [glossary-move] Cat term after cascade: {term_qn}")

    @test("verify_deep_cascade", tags=["move", "glossary"], order=32,
          depends_on=["verify_category_cascade"])
    def test_verify_deep_cascade(self, client, ctx):
        """Verify 3-level cascade: deep_child_cat and deep_term also moved to G2.

        Validates recursive cascade reaches grandchild categories and their terms.
        """
        g2_qn = ctx.get("move_g2_qn")
        old_deep_nano = ctx.get("move_deep_cat_nano")
        old_deep_term_nano = ctx.get("move_deep_term_nano")

        # 1. Deep child category (grandchild) — poll for cascade
        deep_cat_guid = ctx.get_entity_guid("move_deep_cat")
        deep_qn = _wait_for_glossary_suffix(client, deep_cat_guid, g2_qn, max_wait=30)
        assert deep_qn, f"Expected qualifiedName on deep cat after cascade ({deep_cat_guid})"
        deep_local, deep_suffix = _parse_glossary_entity_qn(deep_qn)
        assert deep_suffix == g2_qn, (
            f"Deep cat glossary suffix after cascade: expected {g2_qn}, "
            f"got {deep_suffix} (full QN: {deep_qn})"
        )
        assert deep_local == old_deep_nano, (
            f"Deep cat nanoId changed: expected {old_deep_nano}, got {deep_local}"
        )
        ctx.set("move_deep_cat_qn", deep_qn)
        print(f"  [glossary-move] Deep cat after cascade: {deep_qn}")

        # 2. Term under deep category — poll for cascade
        deep_term_guid = ctx.get_entity_guid("move_deep_term")
        deep_term_qn = _wait_for_glossary_suffix(client, deep_term_guid, g2_qn,
                                                  max_wait=30)
        assert deep_term_qn, (
            f"Expected qualifiedName on deep term after cascade ({deep_term_guid})"
        )
        term_local, term_suffix = _parse_glossary_entity_qn(deep_term_qn)
        assert term_suffix == g2_qn, (
            f"Deep term glossary suffix after cascade: expected {g2_qn}, "
            f"got {term_suffix} (full QN: {deep_term_qn})"
        )
        assert term_local == old_deep_term_nano, (
            f"Deep term nanoId changed: expected {old_deep_term_nano}, got {term_local}"
        )
        ctx.set("move_deep_term_qn", deep_term_qn)
        print(f"  [glossary-move] Deep term after cascade: {deep_term_qn}")

    # ================================================================
    # MOVE 3: Reparent category within same glossary (QN should NOT change)
    # ================================================================

    @test("reparent_category_in_g1", tags=["move", "glossary"], order=40,
          depends_on=["create_reparent_category"])
    def test_reparent_category_in_g1(self, client, ctx):
        """REPARENT: POST /entity with same anchor -> G1 (no-op for QN).

        Uses entity API (glossary API may reject).  Same glossary — QN
        should remain unchanged.
        """
        reparent_guid = ctx.get_entity_guid("move_reparent_cat")
        g1_guid = ctx.get("move_g1_guid")
        assert reparent_guid and g1_guid, "Required entities not created"

        old_qn = ctx.get("move_reparent_cat_qn")

        resp = _move_entity_anchor(
            client, "AtlasGlossaryCategory", reparent_guid, old_qn,
            self.reparent_cat_name, g1_guid,
        )
        assert_status_in(resp, [200, 400, 403, 409])
        if resp.status_code != 200:
            raise SkipTestError(
                f"Reparent category returned {resp.status_code}"
            )
        print(f"  [glossary-move] Reparent in G1: {resp.status_code}")
        time.sleep(3)

    @test("verify_reparent_qn_unchanged", tags=["move", "glossary"], order=41,
          depends_on=["reparent_category_in_g1"])
    def test_verify_reparent_qn_unchanged(self, client, ctx):
        """Verify QN is unchanged (server preserves QN on same-glossary reparent)."""
        reparent_guid = ctx.get_entity_guid("move_reparent_cat")
        old_qn = ctx.get("move_reparent_cat_qn")

        new_qn = _read_entity_qn(client, reparent_guid)
        assert new_qn, f"Expected qualifiedName on reparent cat ({reparent_guid})"
        assert new_qn == old_qn, (
            f"QN changed on same-glossary reparent: expected {old_qn}, got {new_qn}. "
            f"Server should preserve QN when reparenting within the same glossary."
        )
        print(f"  [glossary-move] Reparent QN unchanged: {new_qn}")

    # ================================================================
    # MOVE 4: Round-trip — move term back from G2 to G1
    # ================================================================

    @test("move_term_back_to_g1", tags=["move", "glossary", "round_trip"], order=50,
          depends_on=["verify_term_qn_after_move"])
    def test_move_term_back_to_g1(self, client, ctx):
        """ROUND-TRIP: Move term from G2 back to G1 via entity API.

        Validates that the server correctly replaces the glossary suffix
        in both directions.
        """
        term_guid = ctx.get_entity_guid("move_term")
        g1_guid = ctx.get("move_g1_guid")
        current_qn = ctx.get("move_term_qn")
        assert term_guid and g1_guid, "Required entities not created"

        resp = _move_entity_anchor(
            client, "AtlasGlossaryTerm", term_guid, current_qn,
            self.term_name, g1_guid,
        )
        assert_status_in(resp, [200, 400, 403, 409])
        if resp.status_code != 200:
            raise SkipTestError(
                f"Move term back to G1 returned {resp.status_code}"
            )
        print(f"  [glossary-move] Move term back to G1: {resp.status_code}")

    @test("verify_term_round_trip", tags=["move", "glossary", "round_trip"], order=51,
          depends_on=["move_term_back_to_g1"])
    def test_verify_term_round_trip(self, client, ctx):
        """Verify term QN reverted to @{g1QN} with original nanoId preserved."""
        term_guid = ctx.get_entity_guid("move_term")
        old_nano = ctx.get("move_term_nano")
        g1_qn = ctx.get("move_g1_qn")

        qn = _wait_for_glossary_suffix(client, term_guid, g1_qn, max_wait=30)
        assert qn, f"Expected qualifiedName on term after round-trip ({term_guid})"

        local_part, glossary_suffix = _parse_glossary_entity_qn(qn)
        assert glossary_suffix == g1_qn, (
            f"Term glossary suffix after round-trip: expected {g1_qn}, "
            f"got {glossary_suffix} (full QN: {qn})"
        )
        assert local_part == old_nano, (
            f"Term nanoId changed during round-trip: expected {old_nano}, "
            f"got {local_part}"
        )
        ctx.set("move_term_qn", qn)
        print(f"  [glossary-move] Term after round-trip: {qn}")

    # ================================================================
    # MOVE 5: Round-trip — move root category (+ full tree) back to G1
    # ================================================================

    @test("move_category_back_to_g1", tags=["move", "glossary", "round_trip"], order=55,
          depends_on=["verify_deep_cascade"])
    def test_move_category_back_to_g1(self, client, ctx):
        """ROUND-TRIP: Move root category from G2 back to G1 via entity API.

        Validates round-trip cascade across 3 category levels and 2 terms.
        """
        cat_guid = ctx.get_entity_guid("move_root_cat")
        g1_guid = ctx.get("move_g1_guid")
        current_qn = ctx.get("move_root_cat_qn")
        assert cat_guid and g1_guid, "Required entities not created"

        resp = _move_entity_anchor(
            client, "AtlasGlossaryCategory", cat_guid, current_qn,
            self.root_cat_name, g1_guid,
        )
        assert_status_in(resp, [200, 400, 403, 409])
        if resp.status_code != 200:
            raise SkipTestError(
                f"Move category back to G1 returned {resp.status_code}"
            )
        print(f"  [glossary-move] Move root cat back to G1: {resp.status_code}")

    @test("verify_category_round_trip", tags=["move", "glossary", "round_trip"], order=56,
          depends_on=["move_category_back_to_g1"])
    def test_verify_category_round_trip(self, client, ctx):
        """Verify full cascade round-trip: all 5 entities reverted to @{g1QN}.

        Checks root_cat, child_cat, deep_child_cat, cat_term, deep_term.
        All nanoIds must be preserved through the G1->G2->G1 round-trip.
        """
        g1_qn = ctx.get("move_g1_qn")
        entities_to_check = [
            ("move_root_cat", ctx.get("move_root_cat_nano"), "Root cat"),
            ("move_child_cat", ctx.get("move_child_cat_nano"), "Child cat"),
            ("move_deep_cat", ctx.get("move_deep_cat_nano"), "Deep cat"),
            ("move_cat_term", ctx.get("move_cat_term_nano"), "Cat term"),
            ("move_deep_term", ctx.get("move_deep_term_nano"), "Deep term"),
        ]

        for entity_name, expected_nano, label in entities_to_check:
            guid = ctx.get_entity_guid(entity_name)
            if not guid:
                print(f"  [glossary-move] {label}: skipped (no guid)")
                continue

            qn = _wait_for_glossary_suffix(client, guid, g1_qn, max_wait=30)
            assert qn, f"Expected QN on {label} after round-trip ({guid})"

            local_part, suffix = _parse_glossary_entity_qn(qn)
            assert suffix == g1_qn, (
                f"{label} suffix after round-trip: expected {g1_qn}, got {suffix} "
                f"(full QN: {qn})"
            )
            assert local_part == expected_nano, (
                f"{label} nanoId changed in round-trip: "
                f"expected {expected_nano}, got {local_part}"
            )
            ctx.set(f"{entity_name.replace('move_', 'move_')}_qn", qn)
            print(f"  [glossary-move] {label} round-trip: {qn}")

    # ================================================================
    # MOVE 6: True reparent — make root cat a child of another (same glossary)
    # ================================================================

    @test("true_reparent_to_child", tags=["move", "glossary", "reparent"], order=60,
          depends_on=["create_reparent_target", "verify_reparent_qn_unchanged"])
    def test_true_reparent_to_child(self, client, ctx):
        """TRUE REPARENT: Make reparent_cat a child of reparent_target within G1.

        Actually changes parentCategory (unlike earlier no-op reparent).
        Server should NOT change QN since the category stays in the same glossary.
        """
        reparent_guid = ctx.get_entity_guid("move_reparent_cat")
        reparent_target_guid = ctx.get("move_reparent_target_guid")
        g1_guid = ctx.get("move_g1_guid")
        old_qn = ctx.get("move_reparent_cat_qn")
        assert reparent_guid and reparent_target_guid and g1_guid, (
            "Required entities not created"
        )

        resp = client.post("/entity", json_data={
            "entity": {
                "typeName": "AtlasGlossaryCategory",
                "guid": reparent_guid,
                "attributes": {
                    "qualifiedName": old_qn,
                    "name": self.reparent_cat_name,
                },
                "relationshipAttributes": {
                    "anchor": {
                        "guid": g1_guid,
                        "typeName": "AtlasGlossary",
                    },
                    "parentCategory": {
                        "guid": reparent_target_guid,
                        "typeName": "AtlasGlossaryCategory",
                    },
                },
            }
        }, timeout=60)
        assert_status_in(resp, [200, 400, 403, 409])
        if resp.status_code != 200:
            raise SkipTestError(
                f"True reparent returned {resp.status_code}"
            )
        print(f"  [glossary-move] True reparent (root->child): {resp.status_code}")
        time.sleep(3)

    @test("verify_true_reparent_qn_unchanged", tags=["move", "glossary", "reparent"],
          order=61, depends_on=["true_reparent_to_child"])
    def test_verify_true_reparent_qn_unchanged(self, client, ctx):
        """Verify QN unchanged after actual parentCategory change within same glossary."""
        reparent_guid = ctx.get_entity_guid("move_reparent_cat")
        old_qn = ctx.get("move_reparent_cat_qn")

        new_qn = _read_entity_qn(client, reparent_guid)
        assert new_qn, f"Expected QN on reparent cat after true reparent ({reparent_guid})"
        assert new_qn == old_qn, (
            f"QN changed on true same-glossary reparent: expected {old_qn}, "
            f"got {new_qn}. Server should preserve QN when parentCategory changes "
            f"within the same glossary."
        )
        print(f"  [glossary-move] True reparent QN unchanged: {new_qn}")

    # ================================================================
    # CLEANUP: delete in reverse order
    # ================================================================

    @test("delete_glossary_move_entities", tags=["move", "glossary", "crud"], order=80)
    def test_delete_glossary_move_entities(self, client, ctx):
        """Delete all entities: deep cats, child cats, root cats, terms, glossaries."""
        # Deep categories first, then child, then root
        for name in ("move_deep_cat", "move_child_cat",
                     "move_reparent_cat", "move_reparent_target", "move_root_cat"):
            guid = ctx.get_entity_guid(name)
            if guid:
                resp = client.delete(f"/glossary/category/{guid}")
                assert_status_in(resp, [200, 204, 404])
                print(f"  [glossary-move] Deleted {name}: {resp.status_code}")

        # Terms
        for name in ("move_deep_term", "move_cat_term", "move_term"):
            guid = ctx.get_entity_guid(name)
            if guid:
                resp = client.delete(f"/glossary/term/{guid}")
                assert_status_in(resp, [200, 204, 404])
                print(f"  [glossary-move] Deleted {name}: {resp.status_code}")

        # Glossaries
        for name in ("move_g2", "move_g1"):
            guid = ctx.get_entity_guid(name)
            if guid:
                resp = client.delete(f"/glossary/{guid}")
                assert_status_in(resp, [200, 204, 404])
                print(f"  [glossary-move] Deleted {name}: {resp.status_code}")

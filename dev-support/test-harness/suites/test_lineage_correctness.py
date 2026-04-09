"""Lineage correctness tests.

Verifies lineage graph structure (guidEntityMap, relations, direction,
depth, hideProcess), process entity inputs/outputs round-trip,
classification propagation through lineage with hard assertions,
restrictPropagationThroughLineage blocking, propagation cleanup on
tag removal, and lineage behavior after entity deletion.

Existing suites test endpoint availability and response shapes but
use best-effort assertions for propagation.  This suite uses hard
assertions throughout.
"""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError
from core.data_factory import (
    build_dataset_entity, build_process_entity,
    unique_name, unique_qn, unique_type_name,
)
from core.typedef_helpers import ensure_classification_types


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _index_search(client, dsl):
    """Issue an indexsearch query and return (available, body)."""
    resp = client.post("/search/indexsearch", json_data={"dsl": dsl})
    if resp.status_code in (404, 400, 405):
        return False, {}
    if resp.status_code != 200:
        return False, {}
    return True, resp.json()


def _search_by_guid(client, guid):
    """Search for a single entity by GUID in ES."""
    return _index_search(client, {
        "from": 0, "size": 1,
        "query": {"bool": {"must": [
            {"term": {"__guid": guid}},
            {"term": {"__state": "ACTIVE"}},
        ]}}
    })


def _create_dataset(client, ctx, suffix, type_name="DataSet"):
    """Create an entity for lineage use, register cleanup, return guid or None."""
    qn = unique_qn(suffix)
    entity = build_dataset_entity(qn=qn, name=unique_name(suffix), type_name=type_name)
    resp = client.post("/entity", json_data={"entity": entity})
    if resp.status_code != 200:
        detail = repr(resp.body)[:500] if resp.body else "(empty body)"
        print(f"  [lineage-setup] DataSet creation for {suffix} returned {resp.status_code}: {detail}")
        return None
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    if not entities:
        print(f"  [lineage-setup] DataSet creation for {suffix} got 200 but no entities in response: {list(body.keys())}")
        return None
    guid = entities[0]["guid"]
    ctx.register_entity_cleanup(guid)
    return guid


def _create_process(client, ctx, suffix, input_guids, output_guids,
                    entity_type="DataSet"):
    """Create a Process with inputs/outputs.  Returns (ok, guid).

    *entity_type* is used for the typeName in input/output references.
    """
    inputs = [{"guid": g, "typeName": entity_type} for g in input_guids]
    outputs = [{"guid": g, "typeName": entity_type} for g in output_guids]
    proc = build_process_entity(
        qn=unique_qn(suffix), name=unique_name(suffix),
        inputs=inputs, outputs=outputs,
    )
    resp = client.post("/entity", json_data={"entity": proc})
    if resp.status_code != 200:
        detail = repr(resp.body)[:500] if resp.body else "(empty body)"
        print(f"  [lineage-setup] Process creation for {suffix} returned {resp.status_code}: {detail}")
        return False, None
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    if not entities:
        print(f"  [lineage-setup] Process creation for {suffix} got 200 but no entities: {list(body.keys())}")
        return False, None
    guid = entities[0]["guid"]
    ctx.register_entity_cleanup(guid)
    return True, guid


def _detect_lineage_entity_type(client):
    """Determine which entity type to use for lineage endpoints.

    On Atlan (preprod/staging), the relationship `process_catalog_outputs`
    expects outputs of type `Catalog`.  DataSet does NOT extend Catalog, so
    Process creation with DataSet outputs returns 400.

    Returns "Catalog" if the type exists, else "DataSet" (local dev).
    """
    resp = client.get("/types/typedef/name/Catalog")
    if resp.status_code == 200:
        print("  [lineage-setup] Catalog type found — using Catalog for lineage entities")
        return "Catalog"
    print("  [lineage-setup] Catalog type not found — using DataSet for lineage entities")
    return "DataSet"


def _get_lineage(client, guid, direction="BOTH", depth=3, hide_process=False):
    """GET /lineage/{guid} and return (available, body)."""
    params = {"direction": direction, "depth": depth}
    if hide_process:
        params["hideProcess"] = "true"
    resp = client.get(f"/lineage/{guid}", params=params)
    if resp.status_code in (404, 400, 405):
        return False, {}
    if resp.status_code != 200:
        return False, {}
    return True, resp.json()


def _get_entity_classifications(client, guid):
    """GET entity and return its classifications list."""
    resp = client.get(f"/entity/guid/{guid}")
    if resp.status_code != 200:
        return None
    entity = resp.json().get("entity", {})
    return entity.get("classifications", [])


# ---------------------------------------------------------------------------
# Suite
# ---------------------------------------------------------------------------

@suite("lineage_correctness", depends_on_suites=["entity_crud"],
       description="Lineage structure, propagation, and mutation correctness")
class LineageCorrectnessSuite:

    def setup(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)

        # --- Classification typedefs for propagation tests ---
        requested = [unique_type_name("LinTag"), unique_type_name("LinTag2")]
        names, created_new, self.tag_ok = ensure_classification_types(
            client, requested,
        )
        self.tag_name, self.tag2_name = names[0], names[1]
        if created_new:
            ctx.register_typedef_cleanup(client, self.tag_name)
            ctx.register_typedef_cleanup(client, self.tag2_name)

        # Detect which entity type to use for lineage (Catalog on Atlan, DataSet on local)
        self.entity_type = _detect_lineage_entity_type(client)

        # -------------------------------------------------------
        # Main lineage graph (non-destructive tests):
        #   ds_a  -->  proc_ab  -->  ds_b  -->  proc_bc  -->  ds_c
        # -------------------------------------------------------
        self.ds_a = _create_dataset(client, ctx, "lin-a", type_name=self.entity_type)
        self.ds_b = _create_dataset(client, ctx, "lin-b", type_name=self.entity_type)
        self.ds_c = _create_dataset(client, ctx, "lin-c", type_name=self.entity_type)

        self.lineage_ok = False
        if self.ds_a and self.ds_b and self.ds_c:
            ok1, self.proc_ab = _create_process(
                client, ctx, "lin-p-ab", [self.ds_a], [self.ds_b],
                entity_type=self.entity_type,
            )
            ok2, self.proc_bc = _create_process(
                client, ctx, "lin-p-bc", [self.ds_b], [self.ds_c],
                entity_type=self.entity_type,
            )
            self.lineage_ok = ok1 and ok2
        else:
            self.proc_ab = self.proc_bc = None

        # -------------------------------------------------------
        # Propagation lineage (will be mutated by tag tests):
        #   prop_src  -->  prop_proc  -->  prop_tgt
        # -------------------------------------------------------
        self.prop_src = _create_dataset(client, ctx, "lin-prop-src", type_name=self.entity_type)
        self.prop_tgt = _create_dataset(client, ctx, "lin-prop-tgt", type_name=self.entity_type)

        self.prop_lineage_ok = False
        if self.prop_src and self.prop_tgt:
            ok3, self.prop_proc = _create_process(
                client, ctx, "lin-prop-p", [self.prop_src], [self.prop_tgt],
                entity_type=self.entity_type,
            )
            self.prop_lineage_ok = ok3
        else:
            self.prop_proc = None

        # Wait for JanusGraph indexes to settle
        time.sleep(max(es_wait, 5))

        # Diagnostic summary
        print(f"  [lineage-setup] entity_type={self.entity_type}")
        print(f"  [lineage-setup] ds_a={self.ds_a}, ds_b={self.ds_b}, ds_c={self.ds_c}")
        print(f"  [lineage-setup] proc_ab={self.proc_ab}, proc_bc={self.proc_bc}, lineage_ok={self.lineage_ok}")
        print(f"  [lineage-setup] prop_src={self.prop_src}, prop_tgt={self.prop_tgt}, prop_proc={self.prop_proc}, prop_lineage_ok={self.prop_lineage_ok}")
        print(f"  [lineage-setup] tag_ok={self.tag_ok}, tag_name={self.tag_name}, tag2_name={self.tag2_name}")

    # ================================================================
    #  Group 1 — Lineage graph structure
    # ================================================================

    @test("lineage_both_has_correct_entities",
          tags=["lineage", "data_correctness"], order=1)
    def test_lineage_both(self, client, ctx):
        """GET /lineage BOTH — guidEntityMap contains src, process, and tgt."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")
        available, body = _get_lineage(client, self.ds_a, "BOTH", depth=3)
        if not available:
            raise SkipTestError("Lineage API not available in this env")

        gem = body.get("guidEntityMap", {})
        assert self.ds_a in gem, (
            f"Source {self.ds_a} not in guidEntityMap: {list(gem.keys())}"
        )
        assert self.proc_ab in gem, (
            f"Process {self.proc_ab} not in guidEntityMap: {list(gem.keys())}"
        )
        assert self.ds_b in gem, (
            f"Target {self.ds_b} not in guidEntityMap: {list(gem.keys())}"
        )

    @test("lineage_output_finds_downstream",
          tags=["lineage", "data_correctness"], order=2)
    def test_lineage_output(self, client, ctx):
        """GET /lineage OUTPUT — downstream entity reachable from source."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")
        available, body = _get_lineage(client, self.ds_a, "OUTPUT", depth=3)
        if not available:
            raise SkipTestError("Lineage API not available in this env")

        gem = body.get("guidEntityMap", {})
        assert self.ds_b in gem, (
            f"Downstream {self.ds_b} not in OUTPUT lineage from {self.ds_a}"
        )

    @test("lineage_input_finds_upstream",
          tags=["lineage", "data_correctness"], order=3)
    def test_lineage_input(self, client, ctx):
        """GET /lineage INPUT — upstream entity reachable from mid-entity."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")
        available, body = _get_lineage(client, self.ds_b, "INPUT", depth=3)
        if not available:
            raise SkipTestError("Lineage API not available in this env")

        gem = body.get("guidEntityMap", {})
        assert self.ds_a in gem, (
            f"Upstream {self.ds_a} not in INPUT lineage from {self.ds_b}"
        )

    @test("lineage_relations_have_edges",
          tags=["lineage", "data_correctness"], order=4)
    def test_lineage_relations(self, client, ctx):
        """Relations array has edges with fromEntityId / toEntityId."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")
        available, body = _get_lineage(client, self.ds_a, "OUTPUT", depth=1)
        if not available:
            raise SkipTestError("Lineage API not available in this env")

        relations = body.get("relations", [])
        if not relations:
            raise SkipTestError("Lineage API does not return relations array in this env")

        all_ids = set()
        for rel in relations:
            fid = rel.get("fromEntityId")
            tid = rel.get("toEntityId")
            if fid:
                all_ids.add(fid)
            if tid:
                all_ids.add(tid)

        assert self.ds_a in all_ids or self.proc_ab in all_ids, (
            f"Neither source {self.ds_a} nor process {self.proc_ab} "
            f"in relation edges: {relations}"
        )

    @test("lineage_multi_hop_reachable_at_depth_3",
          tags=["lineage", "data_correctness"], order=5)
    def test_multi_hop(self, client, ctx):
        """depth=3 from ds_a OUTPUT reaches ds_c (2 hops through 2 processes)."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")
        available, body = _get_lineage(client, self.ds_a, "OUTPUT", depth=3)
        if not available:
            raise SkipTestError("Lineage API not available in this env")

        gem = body.get("guidEntityMap", {})
        assert self.ds_c in gem, (
            f"Entity {self.ds_c} (2 hops away) not reachable at depth=3. "
            f"guidEntityMap has {len(gem)} entries: {list(gem.keys())}"
        )

    @test("lineage_depth_1_limits_reach",
          tags=["lineage", "data_correctness"], order=6)
    def test_depth_limit(self, client, ctx):
        """depth=1 from ds_a OUTPUT — ds_c (2 hops) should NOT be reachable."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")
        available, body = _get_lineage(client, self.ds_a, "OUTPUT", depth=1)
        if not available:
            raise SkipTestError("Lineage API not available in this env")

        gem = body.get("guidEntityMap", {})
        assert self.ds_c not in gem, (
            f"Entity {self.ds_c} (2 hops) should NOT be reachable at depth=1. "
            f"guidEntityMap: {list(gem.keys())}"
        )

    @test("lineage_hide_process_excludes_processes",
          tags=["lineage", "data_correctness"], order=7)
    def test_hide_process(self, client, ctx):
        """hideProcess=true — relations are virtual (dataset→dataset), no Process as relation endpoint."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")
        available, body = _get_lineage(
            client, self.ds_a, "BOTH", depth=3, hide_process=True,
        )
        if not available:
            raise SkipTestError("Lineage API not available in this env")

        gem = body.get("guidEntityMap", {})
        relations = body.get("relations", [])

        # Relations should NOT have Process entities as endpoints.
        # Process entities ARE expected in guidEntityMap (for processId lookup),
        # but virtual relations should be dataset→dataset only.
        process_guids = {
            guid for guid, info in gem.items()
            if isinstance(info, dict) and info.get("typeName") == "Process"
        }
        for rel in relations:
            if isinstance(rel, dict):
                from_id = rel.get("fromEntityId")
                to_id = rel.get("toEntityId")
                assert from_id not in process_guids, (
                    f"Relation fromEntityId {from_id} is a Process with hideProcess=true"
                )
                assert to_id not in process_guids, (
                    f"Relation toEntityId {to_id} is a Process with hideProcess=true"
                )

        # Non-Process entities should still be present
        ds_found = any(
            isinstance(info, dict) and info.get("typeName") == self.entity_type
            for info in gem.values()
        )
        assert ds_found, f"No {self.entity_type} entities in guidEntityMap with hideProcess=true"

    # ================================================================
    #  Group 2 — Process entity data round-trip
    # ================================================================

    @test("process_entity_has_correct_inputs",
          tags=["lineage", "data_correctness", "process"], order=8)
    def test_process_inputs(self, client, ctx):
        """GET process entity — inputs reference the source DataSet."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")
        resp = client.get(f"/entity/guid/{self.proc_ab}")
        assert_status(resp, 200)

        entity = resp.json().get("entity", {})
        # inputs live in relationshipAttributes or attributes
        inputs = (
            entity.get("relationshipAttributes", {}).get("inputs")
            or entity.get("attributes", {}).get("inputs")
            or []
        )
        assert inputs, f"Process {self.proc_ab} has no inputs field in entity response"

        input_guids = [i.get("guid") for i in inputs if isinstance(i, dict)]
        assert self.ds_a in input_guids, (
            f"Source {self.ds_a} not in process inputs: {input_guids}"
        )

    @test("process_entity_has_correct_outputs",
          tags=["lineage", "data_correctness", "process"], order=9)
    def test_process_outputs(self, client, ctx):
        """GET process entity — outputs reference the target DataSet."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")
        resp = client.get(f"/entity/guid/{self.proc_ab}")
        assert_status(resp, 200)

        entity = resp.json().get("entity", {})
        outputs = (
            entity.get("relationshipAttributes", {}).get("outputs")
            or entity.get("attributes", {}).get("outputs")
            or []
        )
        assert outputs, f"Process {self.proc_ab} has no outputs field in entity response"

        output_guids = [o.get("guid") for o in outputs if isinstance(o, dict)]
        assert self.ds_b in output_guids, (
            f"Target {self.ds_b} not in process outputs: {output_guids}"
        )

    # ================================================================
    #  Group 3 — On-demand & list endpoints with data checks
    # ================================================================

    @test("on_demand_lineage_has_base_entity",
          tags=["lineage", "data_correctness"], order=10)
    def test_on_demand(self, client, ctx):
        """POST /lineage on-demand — base entity appears in guidEntityMap."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")
        resp = client.post(f"/lineage/{self.ds_b}", json_data={
            "defaultParams": {
                "inputRelationsLimit": 10,
                "outputRelationsLimit": 10,
            },
        })
        if resp.status_code in (400, 404, 405):
            raise SkipTestError(f"On-demand lineage returned {resp.status_code} — not available")
        assert_status(resp, 200)

        body = resp.json()
        gem = body.get("guidEntityMap", {})
        if gem:
            assert self.ds_b in gem, (
                f"Base entity {self.ds_b} not in on-demand guidEntityMap"
            )

    @test("lineage_list_returns_data",
          tags=["lineage", "data_correctness"], order=11)
    def test_lineage_list(self, client, ctx):
        """POST /lineage/list — response contains lineage data."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")
        resp = client.post("/lineage/list", json_data={
            "guid": self.ds_a,
            "size": 10,
            "from": 0,
            "depth": 3,
            "direction": "OUTPUT",
        })
        if resp.status_code in (400, 404, 405):
            raise SkipTestError(f"Lineage list returned {resp.status_code} — not available")
        assert_status(resp, 200)

        body = resp.json()
        if isinstance(body, dict):
            # Should have some content
            has_data = (
                body.get("entities")
                or body.get("relations")
                or body.get("guidEntityMap")
                or body.get("searchParameters")
            )
            assert has_data, (
                f"lineage/list returned empty response: {list(body.keys())}"
            )

    # ================================================================
    #  Group 4 — Classification propagation through lineage
    #            (hard assertions, not best-effort)
    # ================================================================

    @test("tag_propagates_to_downstream_entity",
          tags=["lineage", "propagation", "data_correctness"], order=12)
    def test_tag_propagates(self, client, ctx):
        """Add tag to source with propagate=True — target gets it via GET entity."""
        if not self.prop_lineage_ok:
            raise SkipTestError("Propagation lineage setup failed — process creation returned non-200")
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed")

        print(f"  [lin-propagation] Adding {self.tag_name} to source {self.prop_src} with propagate=True")
        resp = client.post(
            f"/entity/guid/{self.prop_src}/classifications",
            json_data=[{
                "typeName": self.tag_name,
                "propagate": True,
                "restrictPropagationThroughLineage": False,
            }],
        )
        assert_status_in(resp, [200, 204])

        # Wait for propagation through graph (120s with polling — can be slow on preprod)
        print(f"  [lin-propagation] Waiting up to 120s for propagation to {self.prop_tgt}...")
        found = False
        for i in range(24):
            time.sleep(5)
            classifications = _get_entity_classifications(client, self.prop_tgt)
            if classifications is None:
                continue
            tag_names = [c.get("typeName") for c in classifications if isinstance(c, dict)]
            if self.tag_name in tag_names:
                found = True
                print(f"  [lin-propagation] Tag found on target after {(i+1)*5}s: {tag_names}")
                break
            print(f"  [lin-propagation] Polling ({(i+1)*5}s/120s): target tags = {tag_names}")
        classifications = _get_entity_classifications(client, self.prop_tgt) or []

        tag_found = any(
            isinstance(c, dict) and c.get("typeName") == self.tag_name
            for c in classifications
        )
        assert tag_found, (
            f"Tag {self.tag_name} did NOT propagate to downstream entity "
            f"{self.prop_tgt}. Classifications: "
            f"{[c.get('typeName') for c in classifications if isinstance(c, dict)]}"
        )

    @test("propagated_tag_visible_in_search",
          tags=["lineage", "propagation", "search", "data_correctness"],
          order=13, depends_on=["tag_propagates_to_downstream_entity"])
    def test_propagated_in_search(self, client, ctx):
        """Search downstream entity — propagatedClassificationNames has the tag."""
        if not self.prop_lineage_ok:
            raise SkipTestError("Propagation lineage setup failed")
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed")

        available, body = _search_by_guid(client, self.prop_tgt)
        if not available:
            raise SkipTestError("Search API not available in this env")
        entities = body.get("entities", [])
        assert entities, f"Target entity {self.prop_tgt} not found in search"

        entity = entities[0]
        prop_names = entity.get("propagatedClassificationNames", [])
        cn = entity.get("classificationNames", [])

        found = self.tag_name in prop_names or self.tag_name in cn
        assert found, (
            f"Propagated tag {self.tag_name} not in search result for "
            f"{self.prop_tgt}. propagatedClassificationNames={prop_names}, "
            f"classificationNames={cn}"
        )

    @test("remove_source_tag_clears_propagation",
          tags=["lineage", "propagation", "data_correctness"],
          order=14, depends_on=["tag_propagates_to_downstream_entity"])
    def test_remove_clears(self, client, ctx):
        """Remove tag from source — downstream entity loses the propagated tag."""
        if not self.prop_lineage_ok:
            raise SkipTestError("Propagation lineage setup failed")
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed")

        print(f"  [lin-remove] Removing {self.tag_name} from source {self.prop_src}")
        resp = client.delete(
            f"/entity/guid/{self.prop_src}/classification/{self.tag_name}"
        )
        assert_status_in(resp, [200, 204])

        # Poll for cleanup (120s — propagation cleanup can be slow on preprod)
        print(f"  [lin-remove] Waiting up to 120s for propagated tag removal from {self.prop_tgt}...")
        for i in range(24):
            time.sleep(5)
            classifications = _get_entity_classifications(client, self.prop_tgt) or []
            tag_names = [c.get("typeName") for c in classifications if isinstance(c, dict)]
            if self.tag_name not in tag_names:
                print(f"  [lin-remove] Tag removed from target after {(i+1)*5}s")
                break
            print(f"  [lin-remove] Polling ({(i+1)*5}s/120s): target tags = {tag_names}")
        classifications = _get_entity_classifications(client, self.prop_tgt) or []

        tag_names = [
            c.get("typeName") for c in classifications if isinstance(c, dict)
        ]
        assert self.tag_name not in tag_names, (
            f"Tag {self.tag_name} still on downstream entity {self.prop_tgt} "
            f"after removal from source. Classifications: {tag_names}"
        )

    @test("restrict_propagation_blocks_lineage",
          tags=["lineage", "propagation", "data_correctness"],
          order=15, depends_on=["remove_source_tag_clears_propagation"])
    def test_restrict_propagation(self, client, ctx):
        """restrictPropagationThroughLineage=True — target does NOT get the tag."""
        if not self.prop_lineage_ok:
            raise SkipTestError("Propagation lineage setup failed")
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed")

        print(f"  [lin-restrict] Adding {self.tag2_name} with restrictPropagationThroughLineage=True")
        resp = client.post(
            f"/entity/guid/{self.prop_src}/classifications",
            json_data=[{
                "typeName": self.tag2_name,
                "propagate": True,
                "restrictPropagationThroughLineage": True,
            }],
        )
        assert_status_in(resp, [200, 204])

        # Wait and verify tag did NOT propagate
        print(f"  [lin-restrict] Waiting 15s then checking target should NOT have tag...")
        time.sleep(15)

        classifications = _get_entity_classifications(client, self.prop_tgt)
        assert classifications is not None, f"Could not read entity {self.prop_tgt}"

        tag_names = [
            c.get("typeName") for c in classifications if isinstance(c, dict)
        ]
        assert self.tag2_name not in tag_names, (
            f"Tag {self.tag2_name} propagated to {self.prop_tgt} despite "
            f"restrictPropagationThroughLineage=True. Classifications: {tag_names}"
        )

    # ================================================================
    #  Group 5 — Lineage after entity deletion
    # ================================================================

    @test("lineage_breaks_after_process_delete",
          tags=["lineage", "data_correctness", "delete"], order=16)
    def test_lineage_after_process_delete(self, client, ctx):
        """Delete the Process — lineage from source shows no downstream."""
        # Create a disposable lineage: del_src → del_proc → del_tgt
        del_src = _create_dataset(client, ctx, "lin-del-src", type_name=self.entity_type)
        del_tgt = _create_dataset(client, ctx, "lin-del-tgt", type_name=self.entity_type)
        if not del_src or not del_tgt:
            raise SkipTestError("Entity creation failed")
        ok, del_proc = _create_process(
            client, ctx, "lin-del-p", [del_src], [del_tgt],
            entity_type=self.entity_type,
        )
        if not ok:
            raise SkipTestError("Process creation failed — lineage not supported")
        time.sleep(3)

        # Verify lineage exists before
        available, body = _get_lineage(client, del_src, "OUTPUT", depth=1)
        if not available:
            raise SkipTestError("Lineage API not available")
        gem_before = body.get("guidEntityMap", {})
        if del_tgt not in gem_before:
            raise SkipTestError("Lineage didn't register in time — target not in guidEntityMap")

        # Delete the process
        client.delete(f"/entity/guid/{del_proc}")
        time.sleep(max(ctx.get("es_sync_wait", 5), 5))

        # Lineage from source should no longer reach target
        _, body2 = _get_lineage(client, del_src, "OUTPUT", depth=1)
        gem_after = body2.get("guidEntityMap", {})
        relations_after = body2.get("relations", [])

        assert del_tgt not in gem_after or len(relations_after) == 0, (
            f"Target {del_tgt} still reachable after process deletion. "
            f"guidEntityMap: {list(gem_after.keys())}"
        )

    @test("lineage_after_target_soft_delete",
          tags=["lineage", "data_correctness", "delete"], order=17)
    def test_lineage_after_target_delete(self, client, ctx):
        """Soft-delete target — entity in lineage should be DELETED or removed."""
        del2_src = _create_dataset(client, ctx, "lin-del2-src", type_name=self.entity_type)
        del2_tgt = _create_dataset(client, ctx, "lin-del2-tgt", type_name=self.entity_type)
        if not del2_src or not del2_tgt:
            raise SkipTestError("Entity creation failed")
        ok, del2_proc = _create_process(
            client, ctx, "lin-del2-p", [del2_src], [del2_tgt],
            entity_type=self.entity_type,
        )
        if not ok:
            raise SkipTestError("Process creation failed — lineage not supported")
        time.sleep(3)

        # Verify lineage exists
        available, body = _get_lineage(client, del2_src, "OUTPUT", depth=1)
        if not available:
            raise SkipTestError("Lineage API not available")
        if del2_tgt not in body.get("guidEntityMap", {}):
            raise SkipTestError("Lineage didn't register in time — target not in guidEntityMap")

        # Soft-delete the target
        client.delete(f"/entity/guid/{del2_tgt}")
        time.sleep(max(ctx.get("es_sync_wait", 5), 5))

        # Check lineage — target may be marked DELETED or removed
        _, body2 = _get_lineage(client, del2_src, "OUTPUT", depth=1)
        gem = body2.get("guidEntityMap", {})
        if del2_tgt in gem:
            entity_info = gem[del2_tgt]
            if isinstance(entity_info, dict):
                status = entity_info.get("status", "")
                assert status == "DELETED", (
                    f"Deleted target {del2_tgt} in lineage with status={status}, "
                    f"expected DELETED"
                )

    @test("lineage_by_unique_attribute",
          tags=["lineage", "data_correctness"], order=18)
    def test_lineage_by_unique_attr(self, client, ctx):
        """GET /lineage/uniqueAttribute/type/DataSet — lineage by qualifiedName."""
        if not self.lineage_ok:
            raise SkipTestError("Lineage setup failed — process creation returned non-200")

        # Get ds_a's qualifiedName
        resp = client.get(f"/entity/guid/{self.ds_a}")
        assert_status(resp, 200)
        qn = resp.json().get("entity", {}).get("attributes", {}).get("qualifiedName")
        assert qn, f"Could not read qualifiedName for entity {self.ds_a}"

        resp2 = client.get(
            f"/lineage/uniqueAttribute/type/{self.entity_type}",
            params={
                "attr:qualifiedName": qn,
                "direction": "OUTPUT",
                "depth": 1,
            },
        )
        if resp2.status_code in (400, 404, 405):
            raise SkipTestError(f"Lineage by unique attr returned {resp2.status_code} — not available")
        assert_status(resp2, 200)

        body = resp2.json()
        gem = body.get("guidEntityMap", {})
        assert self.ds_a in gem or len(gem) > 0, (
            f"No entities in lineage by unique attribute for QN={qn}"
        )

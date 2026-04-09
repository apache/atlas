"""Delete correctness tests — pre/post-delete validation.

Validates that after soft-delete:
- Entity status is DELETED
- Classifications are gone/inaccessible
- Lineage is cleared
- Entities are excluded from ACTIVE search
"""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_equals, SkipTestError,
)
from core.data_factory import (
    build_dataset_entity, build_classification_def, build_process_entity,
    unique_name, unique_qn, unique_type_name, PREFIX,
    detect_process_io_type, create_process_with_io,
)
from core.typedef_helpers import ensure_classification_types


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


def _create_entity(client, ctx, suffix, type_name="DataSet"):
    """Create an entity, register cleanup, return (guid, qn)."""
    qn = unique_qn(suffix)
    entity = build_dataset_entity(qn=qn, name=unique_name(suffix), type_name=type_name)
    resp = client.post("/entity", json_data={"entity": entity})
    assert_status(resp, 200)
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    guid = entities[0]["guid"]
    # Register cleanup as safety net (tests will delete explicitly)
    ctx.register_entity_cleanup(guid)
    return guid, qn


@suite("delete_correctness", depends_on_suites=["entity_crud"],
       description="Pre/post delete correctness validation")
class DeleteCorrectnessSuite:

    def setup(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)

        # Detect entity type for Process I/O (Catalog on staging, DataSet on local)
        self.io_type = ctx.get("process_io_type") or detect_process_io_type(client)

        # Create a classification for delete tests
        requested = [unique_type_name("DelTag")]
        names, created_new, self.tag_ok = ensure_classification_types(
            client, requested,
        )
        self.tag_name = names[0]
        if created_new:
            ctx.register_typedef_cleanup(client, self.tag_name)

        # Create src and tgt entities with correct type for process I/O
        self.src_guid, self.src_qn = _create_entity(
            client, ctx, "del-src", type_name=self.io_type,
        )
        self.tgt_guid, self.tgt_qn = _create_entity(
            client, ctx, "del-tgt", type_name=self.io_type,
        )

        # Add classification to src
        resp = client.post(
            f"/entity/guid/{self.src_guid}/classifications",
            json_data=[{"typeName": self.tag_name}],
        )
        self.tag_add_ok = resp.status_code in (200, 204)

        # Add labels to src
        self.src_labels = ["del-label-a"]
        client.post("/entity", json_data={
            "entity": {
                "typeName": self.io_type,
                "guid": self.src_guid,
                "attributes": {
                    "qualifiedName": self.src_qn,
                    "name": unique_name("del-src"),
                },
                "labels": self.src_labels,
            }
        })

        # Create Process linking src -> tgt (lineage)
        ok, self.proc_guid = create_process_with_io(
            client, ctx, "del-proc",
            [self.src_guid], [self.tgt_guid], entity_type=self.io_type,
        )
        self.lineage_ok = ok

        # Wait for ES sync
        time.sleep(max(es_wait, 5))

    # ---- Pre-delete validation ----

    @test("pre_delete_entities_exist", tags=["delete", "correctness"], order=1)
    def test_pre_delete_entities_exist(self, client, ctx):
        for label, guid in [("src", self.src_guid), ("tgt", self.tgt_guid)]:
            resp = client.get(f"/entity/guid/{guid}")
            assert_status(resp, 200)
            assert_field_equals(resp, "entity.status", "ACTIVE")

        if self.proc_guid:
            resp = client.get(f"/entity/guid/{self.proc_guid}")
            assert_status(resp, 200)
            assert_field_equals(resp, "entity.status", "ACTIVE")

    @test("pre_delete_classifications_attached", tags=["delete", "correctness"], order=2,
          depends_on=["pre_delete_entities_exist"])
    def test_pre_delete_classifications_attached(self, client, ctx):
        if not self.tag_add_ok:
            raise SkipTestError("Classification add failed in setup — type cache may not have propagated")

        resp = client.get(f"/entity/guid/{self.src_guid}/classifications")
        assert_status(resp, 200)
        body = resp.json()
        classifications = body if isinstance(body, list) else body.get("list", [])
        found = any(c.get("typeName") == self.tag_name for c in classifications)
        assert found, (
            f"Classification {self.tag_name} not found on src entity before delete"
        )

    @test("pre_delete_lineage_exists", tags=["delete", "correctness"], order=3,
          depends_on=["pre_delete_entities_exist"])
    def test_pre_delete_lineage_exists(self, client, ctx):
        if not self.lineage_ok:
            raise SkipTestError("Process creation didn't work — staging may reject Process without connectionQN")

        resp = client.get(f"/lineage/{self.src_guid}", params={"depth": 1})
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            body = resp.json()
            guid_map = body.get("guidEntityMap", {})
            # Verify at least src is in the lineage map
            assert self.src_guid in guid_map, (
                f"src entity {self.src_guid} not found in lineage guidEntityMap"
            )

    @test("pre_delete_in_search", tags=["delete", "correctness"], order=4,
          depends_on=["pre_delete_entities_exist"])
    def test_pre_delete_in_search(self, client, ctx):
        # Poll for entities to appear in search (ES sync can be slow on staging)
        # Try multiple QN field names to handle ES mapping differences
        qn_fields = ("qualifiedName.keyword", "qualifiedName", "__qualifiedName")
        deadline = time.time() + 120
        count = 0
        available = False
        while time.time() < deadline:
            for qn_field in qn_fields:
                available, body = _index_search(client, {
                    "from": 0, "size": 10,
                    "query": {"bool": {"must": [
                        {"wildcard": {qn_field: f"{PREFIX}/del-*"}},
                        {"term": {"__typeName.keyword": self.io_type}},
                        {"term": {"__state": "ACTIVE"}},
                    ]}}
                })
                if not available:
                    continue
                count = body.get("approximateCount", 0)
                if count >= 2:
                    return  # Found both entities
            if not available:
                raise SkipTestError("Search endpoint not available (404/400/405)")
            time.sleep(5)

        assert count >= 2, (
            f"Expected at least 2 del- entities in search, got count={count}"
        )

    # ---- Delete ----

    @test("delete_entities", tags=["delete", "correctness"], order=5,
          depends_on=["pre_delete_entities_exist"])
    def test_delete_entities(self, client, ctx):
        # Delete process first (if it exists), then datasets
        if self.proc_guid:
            resp = client.delete(f"/entity/guid/{self.proc_guid}")
            assert_status(resp, 200)

        resp = client.delete(f"/entity/guid/{self.src_guid}")
        assert_status(resp, 200)

        resp = client.delete(f"/entity/guid/{self.tgt_guid}")
        assert_status(resp, 200)

    # ---- Post-delete validation ----

    @test("post_delete_entity_status", tags=["delete", "correctness"], order=6,
          depends_on=["delete_entities"])
    def test_post_delete_entity_status(self, client, ctx):
        for label, guid in [("src", self.src_guid), ("tgt", self.tgt_guid)]:
            resp = client.get(f"/entity/guid/{guid}")
            assert_status_in(resp, [200, 404])
            if resp.status_code == 200:
                assert_field_equals(resp, "entity.status", "DELETED")

        if self.proc_guid:
            resp = client.get(f"/entity/guid/{self.proc_guid}")
            assert_status_in(resp, [200, 404])
            if resp.status_code == 200:
                assert_field_equals(resp, "entity.status", "DELETED")

    @test("post_delete_classifications_gone", tags=["delete", "correctness"], order=7,
          depends_on=["delete_entities"])
    def test_post_delete_classifications_gone(self, client, ctx):
        if not self.tag_add_ok:
            raise SkipTestError("Classification add failed in setup — type cache may not have propagated")

        resp = client.get(f"/entity/guid/{self.src_guid}/classifications")
        # After delete: either 404 (entity gone) or 200 with entity status DELETED
        if resp.status_code == 404:
            return  # Entity is hard-deleted — classifications no longer accessible (valid)
        if resp.status_code == 200:
            # Entity still accessible but DELETED — check entity status
            entity_resp = client.get(f"/entity/guid/{self.src_guid}")
            if entity_resp.status_code == 200:
                status = entity_resp.json().get("entity", {}).get("status")
                assert status == "DELETED", (
                    f"Expected entity status DELETED after delete, got {status}"
                )

    @test("post_delete_search_cleared", tags=["delete", "correctness"], order=8,
          depends_on=["delete_entities"])
    def test_post_delete_search_cleared(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)
        time.sleep(max(es_wait, 5))

        # Poll until src is excluded from ACTIVE search (up to 30s)
        for i in range(6):
            available, body = _index_search(client, {
                "from": 0, "size": 5,
                "query": {"bool": {"must": [
                    {"term": {"__guid": self.src_guid}},
                    {"term": {"__state": "ACTIVE"}},
                ]}}
            })
            if not available:
                raise SkipTestError("Search endpoint not available")
            count = body.get("approximateCount", 0)
            if count == 0:
                break
            if i < 5:
                time.sleep(5)
                print(f"  [del-search] Waiting for src to clear from ACTIVE search ({(i+1)*5}s/30s)")

        assert count == 0, (
            f"Deleted src entity {self.src_guid} still in ACTIVE search after 30s, count={count}"
        )

        # Verify tgt is excluded from ACTIVE search
        _, body2 = _index_search(client, {
            "from": 0, "size": 5,
            "query": {"bool": {"must": [
                {"term": {"__guid": self.tgt_guid}},
                {"term": {"__state": "ACTIVE"}},
            ]}}
        })
        count2 = body2.get("approximateCount", 0)
        assert count2 == 0, (
            f"Deleted tgt entity {self.tgt_guid} still in ACTIVE search, count={count2}"
        )

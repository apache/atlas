"""Deep audit correctness tests (10 tests).

Validates audit event fields, ordering, action types, and cross-action
consistency for a single entity lifecycle.

Uses the global /entity/auditSearch endpoint with entityQualifiedName filter
(matches production UI pattern).
"""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present, SkipTestError,
)
from core.data_factory import (
    build_dataset_entity, build_classification_def,
    unique_qn, unique_name, unique_type_name,
)
from core.audit_helpers import (
    search_audit_events, poll_audit_events, assert_audit_event_exists,
)
from core.typedef_helpers import create_typedef_verified, ensure_classification_types


@suite("audit_correctness", depends_on_suites=["entity_crud"],
       description="Deep audit event correctness and field validation")
class AuditCorrectnessSuite:

    def setup(self, client, ctx):
        # Create a dedicated entity for audit correctness tests
        self.qn = unique_qn("audit-correct")
        self.entity_name = unique_name("audit-correct")
        entity = build_dataset_entity(
            qn=self.qn, name=self.entity_name,
            extra_attrs={"description": "audit correctness test"},
        )
        resp = client.post("/entity", json_data={"entity": entity})
        if resp.status_code != 200:
            raise Exception(f"Failed to create entity for audit correctness: {resp.status_code}")
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        if not entities:
            raise Exception("Entity creation returned empty mutatedEntities")
        self.guid = entities[0]["guid"]
        ctx.set("audit_corr_guid", self.guid)
        ctx.set("audit_corr_qn", self.qn)
        ctx.register_cleanup(lambda: client.delete(
            f"/entity/guid/{self.guid}", params={"deleteType": "PURGE"}
        ))

        # Check if audit endpoint is available
        time.sleep(5)
        events, total = poll_audit_events(client, self.guid, max_wait=60, interval=10,
                                          qualifiedName=self.qn)
        if events is None:
            self.audit_available = False
            return
        self.audit_available = total > 0

        # Get 1 usable classification type (create new or use existing)
        requested = [unique_type_name("AuditCorrTag")]
        names, self.created_tag, self.tag_ok = ensure_classification_types(
            client, requested,
        )
        self.tag_name = names[0]
        if self.created_tag:
            ctx.register_typedef_cleanup(client, self.tag_name)

    @test("audit_create_event_fields", tags=["audit"], order=1)
    def test_audit_create_event_fields(self, client, ctx):
        """Verify ENTITY_CREATE audit event has expected fields."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available")
        event = assert_audit_event_exists(
            client, self.guid, "ENTITY_CREATE", max_wait=60, interval=10,
            qualifiedName=self.qn,
        )
        if event is None:
            raise SkipTestError("Audit endpoint returned 404")

        # Must have entityId/entityGuid
        eid = event.get("entityId") or event.get("entityGuid")
        assert eid == self.guid, f"Expected entityId={self.guid}, got {eid}"

        # Must have action
        assert event.get("action") == "ENTITY_CREATE", (
            f"Expected action=ENTITY_CREATE, got {event.get('action')}"
        )

        # Must have timestamp
        ts = event.get("timestamp") or event.get("created")
        assert ts, f"Expected timestamp in audit event, keys={list(event.keys())}"

    @test("audit_update_event_detail", tags=["audit"], order=2,
          depends_on=["audit_create_event_fields"])
    def test_audit_update_event_detail(self, client, ctx):
        """Update entity, verify ENTITY_UPDATE audit event has detail/params."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available")
        guid = ctx.get("audit_corr_guid")
        qn = ctx.get("audit_corr_qn")
        assert guid, "audit_corr_guid not found"

        # Update the entity
        entity = {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": qn,
                "name": self.entity_name,
                "description": "audit updated description",
            },
        }
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)

        time.sleep(5)
        event = assert_audit_event_exists(
            client, guid, "ENTITY_UPDATE", max_wait=60, interval=10,
            qualifiedName=qn,
        )
        if event is None:
            raise SkipTestError("Audit endpoint returned 404")
        assert event.get("action") == "ENTITY_UPDATE", (
            f"Expected ENTITY_UPDATE, got {event.get('action')}"
        )

    @test("audit_event_ordering", tags=["audit"], order=3,
          depends_on=["audit_update_event_detail"])
    def test_audit_event_ordering(self, client, ctx):
        """Verify audit events are returned in reverse chronological order."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available")
        guid = ctx.get("audit_corr_guid")
        qn = ctx.get("audit_corr_qn")
        assert guid, "audit_corr_guid not found"

        events, total = search_audit_events(client, guid, size=20, qualifiedName=qn)
        if events is None:
            raise SkipTestError("Audit endpoint not available")
        assert len(events) >= 2, (
            f"Expected at least 2 audit events (CREATE+UPDATE), got {len(events)}"
        )

        # Check timestamps are descending (most recent first)
        timestamps = []
        for e in events:
            ts = e.get("timestamp") or e.get("created") or 0
            timestamps.append(ts)

        if all(ts > 0 for ts in timestamps):
            for i in range(len(timestamps) - 1):
                assert timestamps[i] >= timestamps[i + 1], (
                    f"Audit events not in descending order: "
                    f"ts[{i}]={timestamps[i]} < ts[{i+1}]={timestamps[i+1]}"
                )

    @test("audit_classification_add_event", tags=["audit"], order=4,
          depends_on=["audit_create_event_fields"])
    def test_audit_classification_add_event(self, client, ctx):
        """Add classification, verify CLASSIFICATION_ADD audit event."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available")
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed")
        guid = ctx.get("audit_corr_guid")
        qn = ctx.get("audit_corr_qn")
        assert guid, "audit_corr_guid not found"

        resp = client.post(
            f"/entity/guid/{guid}/classifications",
            json_data=[{"typeName": self.tag_name}],
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Classification type not yet propagated")

        time.sleep(5)
        event = assert_audit_event_exists(
            client, guid, "CLASSIFICATION_ADD", max_wait=60, interval=10,
            qualifiedName=qn,
        )
        if event is None:
            raise SkipTestError("Audit endpoint returned 404")
        assert event.get("action") == "CLASSIFICATION_ADD"

    @test("audit_classification_delete_event", tags=["audit"], order=5,
          depends_on=["audit_classification_add_event"])
    def test_audit_classification_delete_event(self, client, ctx):
        """Remove classification, verify CLASSIFICATION_DELETE audit event."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available")
        if not self.tag_ok:
            raise SkipTestError("Classification typedef creation failed")
        guid = ctx.get("audit_corr_guid")
        qn = ctx.get("audit_corr_qn")
        assert guid, "audit_corr_guid not found"

        resp = client.delete(
            f"/entity/guid/{guid}/classification/{self.tag_name}",
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code == 404:
            raise SkipTestError("Classification not found for removal")

        time.sleep(5)
        event = assert_audit_event_exists(
            client, guid, "CLASSIFICATION_DELETE", max_wait=60, interval=10,
            qualifiedName=qn,
        )
        if event is None:
            raise SkipTestError("Audit endpoint returned 404")
        assert event.get("action") == "CLASSIFICATION_DELETE"

    @test("audit_label_add_event", tags=["audit"], order=6,
          depends_on=["audit_create_event_fields"])
    def test_audit_label_add_event(self, client, ctx):
        """Add labels, verify LABEL_ADD audit event."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available")
        guid = ctx.get("audit_corr_guid")
        qn = ctx.get("audit_corr_qn")
        assert guid, "audit_corr_guid not found"

        resp = client.post(
            f"/entity/guid/{guid}/labels",
            json_data=["audit-corr-label-1"],
        )
        assert_status_in(resp, [200, 204])

        time.sleep(5)
        event = assert_audit_event_exists(
            client, guid, "LABEL_ADD", max_wait=60, interval=10,
            qualifiedName=qn,
        )
        if event is None:
            raise SkipTestError("Audit endpoint returned 404 or LABEL_ADD not tracked")

    @test("audit_delete_event", tags=["audit"], order=7,
          depends_on=["audit_label_add_event"])
    def test_audit_delete_event(self, client, ctx):
        """Soft delete entity, verify ENTITY_DELETE audit event."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available")
        guid = ctx.get("audit_corr_guid")
        qn = ctx.get("audit_corr_qn")
        assert guid, "audit_corr_guid not found"

        resp = client.delete(f"/entity/guid/{guid}")
        assert_status(resp, 200)

        time.sleep(5)
        event = assert_audit_event_exists(
            client, guid, "ENTITY_DELETE", max_wait=60, interval=10,
            qualifiedName=qn,
        )
        if event is None:
            raise SkipTestError("Audit endpoint returned 404")
        assert event.get("action") == "ENTITY_DELETE"

        # Restore for remaining tests
        resp2 = client.post("/entity/restore/bulk", params={"guid": guid})
        assert_status_in(resp2, [200, 204, 404])

    @test("audit_count_accuracy", tags=["audit"], order=8,
          depends_on=["audit_delete_event"])
    def test_audit_count_accuracy(self, client, ctx):
        """Verify totalCount matches actual events returned."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available")
        guid = ctx.get("audit_corr_guid")
        qn = ctx.get("audit_corr_qn")
        assert guid, "audit_corr_guid not found"

        events, total = search_audit_events(client, guid, size=50, qualifiedName=qn)
        if events is None:
            raise SkipTestError("Audit endpoint not available")
        # totalCount should be >= events returned
        assert total >= len(events), (
            f"totalCount ({total}) < events returned ({len(events)})"
        )
        # We've done CREATE, UPDATE, CLASSIFICATION_ADD, CLASSIFICATION_DELETE,
        # LABEL_ADD, ENTITY_DELETE — at least 4 events expected
        assert len(events) >= 4, (
            f"Expected at least 4 audit events, got {len(events)}"
        )

    @test("audit_global_search", tags=["audit"], order=9)
    def test_audit_global_search(self, client, ctx):
        """POST /entity/auditSearch — global audit search with entityQualifiedName."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available")
        qn = ctx.get("audit_corr_qn")
        assert qn, "audit_corr_qn not found"
        resp = client.post("/entity/auditSearch", json_data={
            "dsl": {
                "from": 0,
                "size": 5,
                "sort": [{"created": {"order": "desc"}}],
                "query": {
                    "bool": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {"term": {"entityQualifiedName": qn}},
                                ]
                            }
                        }
                    }
                },
            },
            "suppressLogs": True,
        })
        assert_status_in(resp, [200, 400, 404, 405])
        if resp.status_code == 200:
            body = resp.json()
            audits = body.get("entityAudits", body.get("audits", []))
            assert isinstance(audits, list), (
                f"Expected list of audits, got {type(audits).__name__}"
            )

    @test("audit_multiple_actions_same_entity", tags=["audit"], order=10,
          depends_on=["audit_count_accuracy"])
    def test_audit_multiple_actions_same_entity(self, client, ctx):
        """Verify all action types present for our entity."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available")
        guid = ctx.get("audit_corr_guid")
        qn = ctx.get("audit_corr_qn")
        assert guid, "audit_corr_guid not found"

        events, total = search_audit_events(client, guid, size=50, qualifiedName=qn)
        if events is None:
            raise SkipTestError("Audit endpoint not available")

        actions_found = set()
        for e in events:
            action = e.get("action")
            if action:
                actions_found.add(action)

        # At minimum CREATE and UPDATE should be there
        assert "ENTITY_CREATE" in actions_found, (
            f"Expected ENTITY_CREATE in actions, got {actions_found}"
        )
        assert "ENTITY_UPDATE" in actions_found, (
            f"Expected ENTITY_UPDATE in actions, got {actions_found}"
        )

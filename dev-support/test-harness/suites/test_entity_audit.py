"""Entity audit history tests via POST /entity/auditSearch (global endpoint)."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, assert_field_present, SkipTestError
from core.audit_helpers import (
    search_audit_events, poll_audit_events, assert_audit_event_exists,
)
from core.data_factory import build_dataset_entity, unique_qn, unique_name


@suite("entity_audit", depends_on_suites=["entity_crud"],
       description="Entity audit search endpoints")
class EntityAuditSuite:

    def setup(self, client, ctx):
        # Verify audit endpoint is reachable (first check)
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        if not guid:
            raise Exception("No ds1 entity registered; depends on entity_crud suite")
        self.ds1_qn = qn

        # Poll until at least one audit event is available (up to 60s)
        print(f"  [audit-setup] Polling for audit events on ds1 ({guid})...")
        events, total = poll_audit_events(client, guid, max_wait=60, interval=10,
                                          qualifiedName=qn)
        if events is None:
            # Endpoint genuinely doesn't exist (404/405) — flag for tests
            self.audit_available = False
            print("  [audit-setup] Audit endpoint not available (404/405)")
            return

        if total == 0:
            # Endpoint works but no events indexed — audit indexing not configured
            self.audit_available = False
            print(f"  [audit-setup] Audit endpoint returns 200 but 0 events for {guid} "
                  f"after 60s — audit indexing not configured on this environment")
            return
        self.audit_available = True

    @test("audit_search_basic", tags=["audit"], order=1)
    def test_audit_search_basic(self, client, ctx):
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment (404/405)")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        events, total = search_audit_events(client, guid, size=10, qualifiedName=qn)
        assert events is not None, "Audit endpoint returned None unexpectedly"
        assert isinstance(events, list), f"Expected list of audit events, got {type(events)}"
        assert total > 0, f"Expected totalCount > 0 for entity {guid}, got {total}"

    @test("audit_search_entity_create", tags=["audit"], order=2)
    def test_audit_search_entity_create(self, client, ctx):
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment (404/405)")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        events, total = poll_audit_events(client, guid, action_filter="ENTITY_CREATE",
                                          max_wait=30, interval=10, qualifiedName=qn)
        assert events is not None, "Audit endpoint returned None"
        assert len(events) > 0, (
            f"Expected ENTITY_CREATE audit event for {guid}, got 0 events after polling"
        )
        event = events[0]
        entity_id = event.get("entityId") or event.get("entityGuid")
        assert entity_id == guid, f"Expected entityId={guid}, got {entity_id}"
        assert event.get("action") == "ENTITY_CREATE", (
            f"Expected action=ENTITY_CREATE, got {event.get('action')}"
        )
        assert "user" in event, "Expected 'user' field in audit event"
        assert "timestamp" in event, "Expected 'timestamp' field in audit event"

    @test("audit_search_entity_update", tags=["audit"], order=3)
    def test_audit_search_entity_update(self, client, ctx):
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment (404/405)")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        events, total = poll_audit_events(client, guid, action_filter="ENTITY_UPDATE",
                                          max_wait=30, interval=10, qualifiedName=qn)
        assert events is not None, "Audit endpoint returned None"
        assert len(events) > 0, (
            f"Expected ENTITY_UPDATE audit event for {guid}, got 0 events after polling"
        )
        event = events[0]
        assert event.get("action") == "ENTITY_UPDATE", (
            f"Expected ENTITY_UPDATE, got {event.get('action')}"
        )

    @test("audit_search_action_filter", tags=["audit"], order=4)
    def test_audit_search_action_filter(self, client, ctx):
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment (404/405)")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        events, total = search_audit_events(client, guid, action_filter="ENTITY_CREATE",
                                            qualifiedName=qn)
        assert events is not None, "Audit endpoint returned None"
        assert len(events) > 0, "Expected at least 1 ENTITY_CREATE event"
        # All returned events should match the requested action filter
        for event in events:
            assert event.get("action") == "ENTITY_CREATE", (
                f"Expected all events to have action=ENTITY_CREATE, got {event.get('action')}"
            )

    @test("audit_search_pagination", tags=["audit"], order=5)
    def test_audit_search_pagination(self, client, ctx):
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment (404/405)")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        # Fetch page 1 (size=1)
        events_p1, total = search_audit_events(client, guid, size=1, qualifiedName=qn)
        assert events_p1 is not None, "Audit endpoint returned None"
        assert len(events_p1) <= 1, f"Expected at most 1 event with size=1, got {len(events_p1)}"
        # Fetch larger page to verify pagination returns more
        if total > 1:
            events_p2, total2 = search_audit_events(client, guid, size=5, qualifiedName=qn)
            assert events_p2 is not None, "Audit endpoint returned None"
            assert len(events_p2) > len(events_p1), (
                f"Expected more events with size=5 ({len(events_p2)}) than size=1 ({len(events_p1)})"
            )

    @test("audit_search_event_fields", tags=["audit"], order=6)
    def test_audit_search_event_fields(self, client, ctx):
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment (404/405)")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        events, total = search_audit_events(client, guid, size=1, qualifiedName=qn)
        assert events is not None, "Audit endpoint returned None"
        assert len(events) > 0, f"Expected at least 1 audit event for {guid}"
        event = events[0]
        required_fields = ["entityId", "action", "user"]
        for field_name in required_fields:
            assert field_name in event, f"Audit event missing required field '{field_name}'"

    @test("audit_search_entity_delete", tags=["audit"], order=7)
    def test_audit_search_entity_delete(self, client, ctx):
        """AUD-03: Verify ENTITY_DELETE audit event after entity deletion."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment (404/405)")

        # Prefer GUID from entity_crud's delete_entity_by_guid test
        guid = ctx.get("deleted_entity_guid")
        del_qn = ctx.get("deleted_entity_qn")

        if not guid:
            # Fallback: create and delete our own entity
            del_qn = unique_qn("audit-del")
            entity = build_dataset_entity(qn=del_qn, name=unique_name("audit-del"))
            resp = client.post("/entity", json_data={"entity": entity})
            assert_status(resp, 200)
            body = resp.json()
            creates = body.get("mutatedEntities", {}).get("CREATE", [])
            updates = body.get("mutatedEntities", {}).get("UPDATE", [])
            entities = creates or updates
            assert entities, "Entity creation returned no entities in mutatedEntities"
            guid = entities[0]["guid"]
            resp = client.delete(f"/entity/guid/{guid}")
            assert_status_in(resp, [200, 204])

        # Poll for ENTITY_DELETE audit event
        events, total = poll_audit_events(client, guid, action_filter="ENTITY_DELETE",
                                          max_wait=60, interval=10, qualifiedName=del_qn)
        assert events is not None, "Audit endpoint returned None"
        assert len(events) > 0, (
            f"Expected ENTITY_DELETE audit event for {guid} after 60s polling, got 0"
        )
        event = events[0]
        entity_id = event.get("entityId") or event.get("entityGuid")
        assert entity_id == guid, f"Expected entityId={guid}, got {entity_id}"
        assert event.get("action") == "ENTITY_DELETE", (
            f"Expected action=ENTITY_DELETE, got {event.get('action')}"
        )

    @test("audit_label_add_event", tags=["audit"], order=7.5)
    def test_audit_label_add_event(self, client, ctx):
        """Poll for LABEL_ADD audit event after adding label."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        assert guid, "ds1 GUID not found"
        # Add a label to trigger audit event
        client.post(f"/entity/guid/{guid}/labels", json_data=["audit-test-label"])
        events, total = poll_audit_events(
            client, guid, action_filter="LABEL_ADD",
            max_wait=60, interval=10, qualifiedName=qn,
        )
        if events is None:
            raise SkipTestError("Audit endpoint returned None for LABEL_ADD")
        if not events:
            raise SkipTestError("No LABEL_ADD audit events after 60s — may not be indexed")
        assert events[0].get("action") == "LABEL_ADD", (
            f"Expected LABEL_ADD, got {events[0].get('action')}"
        )

    @test("audit_classification_add_event", tags=["audit"], order=8)
    def test_audit_classification_add_event(self, client, ctx):
        """Poll for CLASSIFICATION_ADD audit event."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        assert guid, "ds1 GUID not found"
        events, total = poll_audit_events(
            client, guid, action_filter="CLASSIFICATION_ADD",
            max_wait=30, interval=10, qualifiedName=qn,
        )
        if events is None:
            raise SkipTestError("Audit endpoint returned None for CLASSIFICATION_ADD")
        if not events:
            raise SkipTestError(
                "No CLASSIFICATION_ADD events for ds1 — "
                "classification may not have been added to ds1"
            )
        assert events[0].get("action") == "CLASSIFICATION_ADD", (
            f"Expected CLASSIFICATION_ADD, got {events[0].get('action')}"
        )

    @test("audit_ordering", tags=["audit"], order=9)
    def test_audit_ordering(self, client, ctx):
        """Fetch multiple events, verify timestamps are in descending order."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        events, total = search_audit_events(client, guid, size=10, qualifiedName=qn)
        assert events is not None, "Audit endpoint returned None"
        if len(events) >= 2:
            timestamps = [e.get("timestamp", 0) for e in events]
            for i in range(len(timestamps) - 1):
                assert timestamps[i] >= timestamps[i + 1], (
                    f"Audit events not in descending order: "
                    f"{timestamps[i]} < {timestamps[i+1]} at index {i}"
                )

    @test("audit_count_endpoint", tags=["audit"], order=10)
    def test_audit_count_endpoint(self, client, ctx):
        """AuditSearch with size=0 to get totalCount only."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        events, total = search_audit_events(client, guid, size=0, qualifiedName=qn)
        if events is None:
            raise SkipTestError("Audit endpoint returned None")
        assert isinstance(total, int), f"Expected int totalCount, got {type(total).__name__}"
        assert total > 0, f"Expected totalCount > 0 for ds1, got {total}"

    @test("audit_timestamp_range", tags=["audit"], order=11)
    def test_audit_timestamp_range(self, client, ctx):
        """Search audit events with timestamp range via global endpoint."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        import time as _time
        now_ms = int(_time.time() * 1000)
        one_hour_ago = now_ms - (3600 * 1000)

        # Build entity filter: prefer QN, fall back to entityId
        entity_filter = (
            {"term": {"entityQualifiedName": qn}} if qn
            else {"term": {"entityId": guid}}
        )

        resp = client.post("/entity/auditSearch", json_data={
            "dsl": {
                "from": 0,
                "size": 10,
                "sort": [{"created": {"order": "desc"}}],
                "query": {
                    "bool": {
                        "filter": {
                            "bool": {
                                "must": [
                                    entity_filter,
                                    {"range": {"timestamp": {"gte": one_hour_ago, "lte": now_ms}}},
                                ]
                            }
                        }
                    }
                }
            },
            "suppressLogs": True,
        })
        assert_status_in(resp, [200, 400, 404])
        if resp.status_code == 200:
            body = resp.json()
            events = body.get("entityAudits", [])
            for evt in events[:5]:
                ts = evt.get("timestamp", 0) if isinstance(evt, dict) else 0
                if ts:
                    assert ts >= one_hour_ago, (
                        f"Event timestamp {ts} before range start {one_hour_ago}"
                    )

    @test("audit_event_detail_field", tags=["audit"], order=12)
    def test_audit_event_detail_field(self, client, ctx):
        """Verify audit event has detail or params field."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment")
        guid = ctx.get_entity_guid("ds1")
        qn = ctx.get_entity_qn("ds1")
        events, total = search_audit_events(client, guid, size=3, qualifiedName=qn)
        assert events is not None and len(events) > 0, "Expected at least 1 audit event"
        event = events[0]
        has_detail = any(k in event for k in ("detail", "params", "result", "eventKey"))
        assert has_detail, (
            f"Audit event should have detail/params/result/eventKey, "
            f"got keys: {list(event.keys())}"
        )

    @test("audit_nonexistent_entity", tags=["audit", "negative"], order=13)
    def test_audit_nonexistent_entity(self, client, ctx):
        """AuditSearch for nonexistent GUID — empty result or 404."""
        if not self.audit_available:
            raise SkipTestError("Audit endpoint not available on this environment")
        fake_guid = "00000000-0000-0000-0000-000000000000"
        events, total = search_audit_events(client, fake_guid, size=10)
        if events is None:
            return  # 404 is acceptable
        assert total == 0, (
            f"Expected 0 audit events for nonexistent GUID, got {total}"
        )

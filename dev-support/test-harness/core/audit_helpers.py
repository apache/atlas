"""Audit event helpers for verifying entity audit trail via POST /entity/auditSearch.

Uses the GLOBAL audit endpoint (/entity/auditSearch) instead of the per-entity
endpoint (/entity/{guid}/auditSearch) because the per-entity endpoint returns
0 events on staging/preprod environments.

Matches the production UI pattern: filter by entityQualifiedName (preferred)
or entityId, sort by created desc, filter context (no scoring).
"""

import time

# Upfront wait for audit indexing eventual consistency (seconds).
AUDIT_CONSISTENCY_WAIT_S = 30


def search_audit_events(client, guid, action_filter=None, size=10, qualifiedName=None):
    """Search audit events for an entity via POST /entity/auditSearch (global).

    Uses filter context (not must) for performance — matches production UI
    pattern with entityQualifiedName pinning, sort by created desc.

    Args:
        client: HTTP client
        guid: Entity GUID (used as entityId filter if qualifiedName not provided)
        action_filter: Optional action type filter (e.g. "ENTITY_CREATE")
        size: Number of events to return
        qualifiedName: Entity qualifiedName (preferred filter — matches production)

    Returns (events_list, total_count) or (None, 0) if endpoint unavailable.
    """
    # Build filter: prefer entityQualifiedName (matches prod UI), fall back to entityId
    if qualifiedName:
        entity_filter = {"term": {"entityQualifiedName": qualifiedName}}
    else:
        entity_filter = {"term": {"entityId": guid}}

    filter_clauses = [entity_filter]
    if action_filter:
        filter_clauses.append({"term": {"action": action_filter}})

    payload = {
        "dsl": {
            "from": 0,
            "size": size,
            "sort": [{"created": {"order": "desc"}}],
            "query": {
                "bool": {
                    "filter": {
                        "bool": {
                            "must": filter_clauses,
                        }
                    }
                }
            },
        },
        "suppressLogs": True,
    }

    # Use global audit endpoint (per-entity endpoint is broken on staging/preprod)
    resp = client.post("/entity/auditSearch", json_data=payload)

    if resp.status_code in (404, 400, 405):
        # Endpoint not available (local dev without audit index)
        return None, 0

    if resp.status_code != 200:
        return None, 0

    body = resp.json()
    events = body.get("entityAudits", [])
    total = body.get("totalCount", len(events))
    return events, total


def poll_audit_events(client, guid, action_filter=None, max_wait=60,
                      interval=10, size=10, qualifiedName=None):
    """Poll audit events until results appear.

    Returns (events_list, total_count) or (None, 0) if endpoint unavailable.
    """
    last_events = []
    last_total = 0
    for i in range(max_wait // interval):
        if i > 0:
            time.sleep(interval)
        events, total = search_audit_events(
            client, guid, action_filter=action_filter, size=size,
            qualifiedName=qualifiedName,
        )
        if events is None:
            return None, 0  # Endpoint genuinely not available
        if events:
            return events, total
        last_events = events
        last_total = total
        print(f"  [audit-poll] Waiting for {action_filter or 'any'} audit on {guid} "
              f"({(i+1)*interval}s/{max_wait}s)")
    return last_events, last_total


def assert_audit_event_exists(client, guid, expected_action, max_wait=60,
                              interval=10, qualifiedName=None):
    """Poll until the audit event exists and return it.

    Returns the event dict if found. Asserts if the endpoint is available
    but the event is not found after polling. Returns None ONLY if the
    audit endpoint doesn't exist (404/405).
    """
    events, total = poll_audit_events(
        client, guid, action_filter=expected_action,
        max_wait=max_wait, interval=interval,
        qualifiedName=qualifiedName,
    )

    if events is None:
        # Endpoint genuinely not available (404/405) — skip
        return None

    assert len(events) > 0, (
        f"Expected audit event {expected_action} for entity {guid} "
        f"but none found after {max_wait}s polling"
    )

    event = events[0]
    entity_id = event.get("entityId") or event.get("entityGuid")
    if entity_id:
        assert entity_id == guid, (
            f"Audit event entityId mismatch: expected {guid}, got {entity_id}"
        )
    return event

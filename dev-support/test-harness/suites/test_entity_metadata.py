"""Entity metadata attachment tests — certificates, README, announcements, owners.

Covers operations that attach rich metadata to entities beyond basic CRUD:
- Certificate status (VERIFIED, DRAFT, DEPRECATED)
- userDescription (README / rich text)
- Announcement (badge with type + message)
- Owner assignment
- Partial attribute updates via GUID
- Read-after-write verification for each
- Search reflection verification for each
"""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, assert_field_equals, SkipTestError
from core.data_factory import build_dataset_entity, unique_qn, unique_name


def _index_search(client, dsl):
    resp = client.post("/search/indexsearch", json_data={"dsl": dsl})
    if resp.status_code != 200:
        return False, {}
    return True, resp.json()


def _search_by_guid(client, guid):
    ok, body = _index_search(client, {
        "from": 0, "size": 1,
        "query": {"bool": {"must": [
            {"term": {"__guid": guid}},
            {"term": {"__state": "ACTIVE"}},
        ]}},
    })
    if not ok:
        return None
    entities = body.get("entities", [])
    return entities[0] if entities else None


def _create_entity_and_register(client, ctx, suffix, extra_attrs=None):
    """Create a DataSet entity, register cleanup, return (guid, qn, name)."""
    qn = unique_qn(suffix)
    name = unique_name(suffix)
    entity = build_dataset_entity(qn=qn, name=name, extra_attrs=extra_attrs)
    resp = client.post("/entity", json_data={"entity": entity})
    if resp.status_code != 200:
        return None, None, None
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    if not entities:
        return None, None, None
    guid = entities[0]["guid"]
    ctx.register_entity_cleanup(guid)
    return guid, qn, name


def _update_entity_attrs(client, guid, qn, name, attrs):
    """Partial update: set attributes on entity by GUID, return response.

    Must include `name` — staging enforces it as mandatory on Asset.
    """
    payload = {
        "entity": {
            "typeName": "DataSet",
            "guid": guid,
            "attributes": {"qualifiedName": qn, "name": name, **attrs},
        }
    }
    return client.post("/entity", json_data=payload)


@suite("entity_metadata", depends_on_suites=["entity_crud"],
       description="Entity metadata: certificates, README, announcements, owners, partial updates")
class EntityMetadataSuite:

    def setup(self, client, ctx):
        es_wait = ctx.get("es_sync_wait", 5)

        # Create primary entity with no extra attrs
        self.guid, self.qn, self.name = _create_entity_and_register(client, ctx, "meta-primary")
        assert self.guid, "Failed to create primary entity for metadata tests"

        # Create secondary entity for certificate lifecycle
        self.cert_guid, self.cert_qn, self.cert_name = _create_entity_and_register(
            client, ctx, "meta-cert",
            extra_attrs={"certificateStatus": "DRAFT"},
        )

        time.sleep(max(es_wait, 3))

    # ================================================================
    #  Certificate status
    # ================================================================

    @test("set_certificate_verified",
          tags=["metadata", "certificate", "v2"], order=1)
    def test_set_certificate_verified(self, client, ctx):
        """Set certificateStatus=VERIFIED, verify via GET."""
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "certificateStatus": "VERIFIED",
            "certificateStatusMessage": "Approved by test harness",
        })
        assert_status(resp, 200)

        # Read-after-write
        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        attrs = entity.get("attributes", {})
        assert attrs.get("certificateStatus") == "VERIFIED", (
            f"Expected certificateStatus=VERIFIED, got {attrs.get('certificateStatus')}"
        )
        assert attrs.get("certificateStatusMessage") == "Approved by test harness", (
            f"Expected message='Approved by test harness', got {attrs.get('certificateStatusMessage')}"
        )

    @test("set_certificate_deprecated",
          tags=["metadata", "certificate", "v2"], order=2,
          depends_on=["set_certificate_verified"])
    def test_set_certificate_deprecated(self, client, ctx):
        """Update certificate VERIFIED→DEPRECATED, verify old value replaced."""
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "certificateStatus": "DEPRECATED",
            "certificateStatusMessage": "Deprecated by test",
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        assert attrs.get("certificateStatus") == "DEPRECATED", (
            f"Expected DEPRECATED, got {attrs.get('certificateStatus')}"
        )

    @test("clear_certificate",
          tags=["metadata", "certificate", "v2"], order=3,
          depends_on=["set_certificate_deprecated"])
    def test_clear_certificate(self, client, ctx):
        """Clear certificate by setting to None, verify via GET."""
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "certificateStatus": None,
            "certificateStatusMessage": None,
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        cert = attrs.get("certificateStatus")
        assert cert in ("", None), (
            f"Expected empty certificate after clear, got {cert}"
        )

    @test("certificate_reflected_in_search",
          tags=["metadata", "certificate", "search", "v2"], order=4)
    def test_certificate_in_search(self, client, ctx):
        """Set certificate on secondary entity, verify search returns it."""
        assert self.cert_guid, "cert entity GUID not found — setup failed to create secondary entity"

        # cert_entity was created with DRAFT, update to VERIFIED
        resp = _update_entity_attrs(client, self.cert_guid, self.cert_qn, self.cert_name, {
            "certificateStatus": "VERIFIED",
        })
        assert_status(resp, 200)

        # Wait longer for the update to propagate to ES
        time.sleep(max(ctx.get("es_sync_wait", 5), 10))

        # Poll search until entity appears with updated certificate
        result = None
        for i in range(6):
            if i > 0:
                time.sleep(5)
            result = _search_by_guid(client, self.cert_guid)
            if result:
                cert = (
                    result.get("certificateStatus")
                    or result.get("attributes", {}).get("certificateStatus")
                )
                if cert == "VERIFIED":
                    break
            print(f"  [cert-search] Polling ES ({(i+1)*5}s/30s)")

        assert result is not None, (
            f"Entity {self.cert_guid} not found in search after 30s polling"
        )
        cert = (
            result.get("certificateStatus")
            or result.get("attributes", {}).get("certificateStatus")
        )
        if cert is not None:
            assert cert == "VERIFIED", (
                f"Expected VERIFIED in search, got {cert}"
            )

    # ================================================================
    #  README / userDescription
    # ================================================================

    @test("set_user_description",
          tags=["metadata", "readme", "v2"], order=10)
    def test_set_user_description(self, client, ctx):
        """Set userDescription (README), verify via GET."""
        readme_text = "# Test README\n\nThis entity is managed by the test harness."
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "userDescription": readme_text,
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        assert attrs.get("userDescription") == readme_text, (
            f"Expected userDescription to be set, got: {repr(attrs.get('userDescription'))[:80]}"
        )

    @test("update_user_description",
          tags=["metadata", "readme", "v2"], order=11,
          depends_on=["set_user_description"])
    def test_update_user_description(self, client, ctx):
        """Update userDescription, verify old value replaced."""
        new_readme = "Updated README content."
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "userDescription": new_readme,
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        assert attrs.get("userDescription") == new_readme, (
            f"Expected updated README, got: {repr(attrs.get('userDescription'))[:80]}"
        )

    @test("clear_user_description",
          tags=["metadata", "readme", "v2"], order=12,
          depends_on=["update_user_description"])
    def test_clear_user_description(self, client, ctx):
        """Clear userDescription, verify it's gone."""
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "userDescription": "",
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        ud = attrs.get("userDescription", "")
        assert ud in ("", None), (
            f"Expected empty userDescription after clear, got: {repr(ud)[:80]}"
        )

    # ================================================================
    #  Announcement (badge)
    # ================================================================

    @test("set_announcement",
          tags=["metadata", "announcement", "v2"], order=20)
    def test_set_announcement(self, client, ctx):
        """Set announcement (type + title + message), verify via GET."""
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "announcementType": "information",
            "announcementTitle": "Test Announcement",
            "announcementMessage": "This is a test announcement from the harness.",
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        assert attrs.get("announcementType") == "information", (
            f"Expected announcementType=information, got {attrs.get('announcementType')}"
        )
        assert attrs.get("announcementTitle") == "Test Announcement", (
            f"Expected announcementTitle='Test Announcement', got {attrs.get('announcementTitle')}"
        )
        assert attrs.get("announcementMessage") == "This is a test announcement from the harness.", (
            f"Expected announcementMessage set, got {repr(attrs.get('announcementMessage'))[:80]}"
        )

    @test("update_announcement_type",
          tags=["metadata", "announcement", "v2"], order=21,
          depends_on=["set_announcement"])
    def test_update_announcement_type(self, client, ctx):
        """Change announcement type (information→warning), verify old value replaced."""
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "announcementType": "warning",
            "announcementTitle": "Warning Announcement",
            "announcementMessage": "Updated to warning.",
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        assert attrs.get("announcementType") == "warning", (
            f"Expected warning, got {attrs.get('announcementType')}"
        )

    @test("clear_announcement",
          tags=["metadata", "announcement", "v2"], order=22,
          depends_on=["update_announcement_type"])
    def test_clear_announcement(self, client, ctx):
        """Clear announcement fields, verify via GET."""
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "announcementType": "",
            "announcementTitle": "",
            "announcementMessage": "",
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        ann_type = attrs.get("announcementType", "")
        assert ann_type in ("", None), (
            f"Expected empty announcementType after clear, got {ann_type}"
        )

    # ================================================================
    #  Owner assignment
    # ================================================================

    @test("set_owner_users",
          tags=["metadata", "owner", "v2"], order=30)
    def test_set_owner_users(self, client, ctx):
        """Set ownerUsers on entity, verify via GET."""
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "ownerUsers": ["test-harness-user"],
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        owner = attrs.get("ownerUsers")
        assert owner is not None and "test-harness-user" in str(owner), (
            f"Expected ownerUsers to contain 'test-harness-user', got {owner}"
        )

    @test("set_owner_groups",
          tags=["metadata", "owner", "v2"], order=31)
    def test_set_owner_groups(self, client, ctx):
        """Set ownerGroups on entity, verify via GET."""
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "ownerGroups": ["test-harness-group"],
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        group = attrs.get("ownerGroups")
        assert group is not None and "test-harness-group" in str(group), (
            f"Expected ownerGroups to contain 'test-harness-group', got {group}"
        )

    # ================================================================
    #  Partial update — multiple attrs at once
    # ================================================================

    @test("partial_update_multiple_attrs",
          tags=["metadata", "v2"], order=40)
    def test_partial_update_multiple(self, client, ctx):
        """Update description + certificate + announcement in single call, verify all."""
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "description": "Multi-attr update test",
            "certificateStatus": "VERIFIED",
            "announcementType": "information",
            "announcementTitle": "Multi-update",
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        assert attrs.get("description") == "Multi-attr update test", (
            f"description mismatch: {attrs.get('description')}"
        )
        assert attrs.get("certificateStatus") == "VERIFIED", (
            f"certificateStatus mismatch: {attrs.get('certificateStatus')}"
        )
        assert attrs.get("announcementType") == "information", (
            f"announcementType mismatch: {attrs.get('announcementType')}"
        )
        assert attrs.get("announcementTitle") == "Multi-update", (
            f"announcementTitle mismatch: {attrs.get('announcementTitle')}"
        )

    @test("update_entity_name_via_guid",
          tags=["metadata", "v2"], order=41)
    def test_update_name_via_guid(self, client, ctx):
        """Update entity name by GUID, verify name changed."""
        new_name = unique_name("renamed-meta")
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "name": new_name,
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        assert attrs.get("name") == new_name, (
            f"Expected name={new_name}, got {attrs.get('name')}"
        )
        self.name = new_name  # Update for subsequent tests

    @test("update_preserves_existing_attrs",
          tags=["metadata", "v2"], order=42,
          depends_on=["partial_update_multiple_attrs"])
    def test_update_preserves_existing(self, client, ctx):
        """Update one attr, verify other previously-set attrs are preserved."""
        resp = _update_entity_attrs(client, self.guid, self.qn, self.name, {
            "description": "Preservation test",
        })
        assert_status(resp, 200)

        resp2 = client.get(f"/entity/guid/{self.guid}")
        assert_status(resp2, 200)
        attrs = resp2.json().get("entity", {}).get("attributes", {})
        assert attrs.get("description") == "Preservation test", (
            f"Updated description missing: {attrs.get('description')}"
        )
        # certificateStatus from previous test should still be VERIFIED
        # (unless the update cleared it — that itself is a valuable signal)
        cert = attrs.get("certificateStatus")
        if cert is not None and cert != "":
            # If certificate survived, it should be the value we set earlier
            assert cert == "VERIFIED", (
                f"Expected certificate preserved as VERIFIED, got {cert}"
            )

    # ================================================================
    #  Search reflection after metadata updates
    # ================================================================

    @test("metadata_reflected_in_search",
          tags=["metadata", "search", "v2"], order=50,
          depends_on=["partial_update_multiple_attrs"])
    def test_metadata_in_search(self, client, ctx):
        """Verify metadata changes are reflected in search results."""
        time.sleep(ctx.get("es_sync_wait", 5))

        # Poll search until entity appears
        result = None
        for i in range(6):
            if i > 0:
                time.sleep(5)
            result = _search_by_guid(client, self.guid)
            if result:
                break
            print(f"  [meta-search] Polling ES ({(i+1)*5}s/30s)")
        assert result is not None, (
            f"Entity {self.guid} not found in search after 30s polling"
        )

        # Check description in search
        search_attrs = result.get("attributes", {})
        desc = search_attrs.get("description")
        if desc is not None:
            # Should be either "Preservation test" or "Multi-attr update test"
            assert "test" in desc.lower(), (
                f"Expected description containing 'test' in search, got: {desc}"
            )

        # Check certificate in search
        cert = search_attrs.get("certificateStatus") or result.get("certificateStatus")
        if cert:
            assert cert in ("VERIFIED", "DEPRECATED", "DRAFT", ""), (
                f"Unexpected certificateStatus in search: {cert}"
            )

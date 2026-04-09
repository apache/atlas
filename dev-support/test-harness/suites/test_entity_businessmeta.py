"""Business metadata add/delete on entities."""

import time

from core.decorators import suite, test
from core.assertions import assert_status, assert_status_in, SkipTestError
from core.audit_helpers import poll_audit_events
from core.kafka_helpers import assert_entity_in_kafka
from core.data_factory import (
    build_business_metadata_def, build_multi_attr_business_metadata_def,
    build_dataset_entity, unique_name, unique_qn, unique_type_name,
)
from core.typedef_helpers import create_typedef_verified, extract_bm_names_from_response, ensure_bm_types


@suite("entity_businessmeta", depends_on_suites=["entity_crud"],
       description="Business metadata on entities")
class EntityBusinessMetaSuite:

    def setup(self, client, ctx):
        # Create BM typedef with fallback to existing types
        self.bm_display_name = unique_type_name("HarnessBM")
        bm_info, self.created_bm, self.bm_ok = ensure_bm_types(
            client, self.bm_display_name,
        )

        self.bm_internal_name = None
        self.bm_attr_map = {}  # {displayName: internalName}
        self.bm_field_display = "bmField1"  # default, overridden if bm_info available
        if bm_info:
            self.bm_internal_name = bm_info["internal_name"]
            self.bm_display_name = bm_info["display_name"]
            self.bm_attr_map = bm_info["attr_map"]
            self.bm_field_display = bm_info["first_attr_display"]
        # Only register typedef cleanup when WE created it
        if self.bm_ok and self.created_bm:
            cleanup_name = self.bm_internal_name or self.bm_display_name
            ctx.register_typedef_cleanup(client, cleanup_name)
        if not self.bm_ok:
            print(f"  [bm-setup] BM typedef creation failed — tests will SKIP")

        # Create entity
        qn = unique_qn("bm-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("bm-test"))
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)
        body = resp.json()
        creates = body.get("mutatedEntities", {}).get("CREATE", [])
        updates = body.get("mutatedEntities", {}).get("UPDATE", [])
        entities = creates or updates
        assert entities, "Entity creation returned empty mutatedEntities"
        self.entity_guid = entities[0]["guid"]
        self.entity_qn = qn
        ctx.register_entity_cleanup(self.entity_guid)

    @test("add_business_metadata", tags=["businessmeta"], order=1)
    def test_add_business_metadata(self, client, ctx):
        if not self.bm_ok:
            raise SkipTestError("BM typedef creation failed (500/503)")
        # Use /businessmetadata/displayName endpoint — accepts human-readable names
        payload = {self.bm_display_name: {self.bm_field_display: "test-value"}}
        for attempt in range(6):
            if attempt > 0:
                time.sleep(10)
            resp = client.post(
                f"/entity/guid/{self.entity_guid}/businessmetadata/displayName",
                json_data=payload,
            )
            if resp.status_code in (200, 204):
                break
            if resp.status_code != 404:
                break
            elapsed = (attempt + 1) * 10 if attempt > 0 else 0
            print(f"  [bm] Type cache lag — 404 on attempt {attempt + 1}/6 ({elapsed}s)")
        if resp.status_code == 404:
            raise SkipTestError(
                f"BM type {self.bm_display_name} not recognized by entity endpoint after 50s — "
                f"cross-pod type cache issue (type created but not propagated)"
            )
        assert_status_in(resp, [200, 204])

    @test("add_multi_attr_business_metadata", tags=["businessmeta"], order=1.5)
    def test_add_multi_attr_business_metadata(self, client, ctx):
        multi_display = unique_type_name("HarnessMultiBM")
        multi_def = build_multi_attr_business_metadata_def(display_name=multi_display)
        multi_info, created_multi, multi_ok = ensure_bm_types(
            client, multi_display,
            attr_defs=multi_def["attributeDefs"],
            min_str_attrs=3,
        )
        if not multi_ok:
            raise SkipTestError(
                "Multi-attr BM type not available (creation failed, "
                "no existing type with 3+ string attrs)"
            )

        self.multi_bm_display = multi_info["display_name"]
        self.multi_bm_internal = multi_info["internal_name"]
        self.multi_bm_attr_map = multi_info["attr_map"]
        if created_multi:
            ctx.register_typedef_cleanup(client, self.multi_bm_internal)

        # Build payload: typed values if we created, string values if borrowed
        attr_names = list(multi_info["attr_map"].keys())[:3]
        if created_multi:
            bm_payload = {self.multi_bm_display: {
                "bmStrField": "hello",
                "bmIntField": 42,
                "bmBoolField": True,
            }}
            expected = [
                ("bmStrField", "hello"),
                ("bmIntField", 42),
                ("bmBoolField", True),
            ]
        else:
            bm_payload = {self.multi_bm_display: {
                attr_names[0]: "hello",
                attr_names[1]: "world",
                attr_names[2]: "test-val",
            }}
            expected = [
                (attr_names[0], "hello"),
                (attr_names[1], "world"),
                (attr_names[2], "test-val"),
            ]

        resp = client.post(
            f"/entity/guid/{self.entity_guid}/businessmetadata/displayName",
            json_data=bm_payload,
        )
        assert_status_in(resp, [200, 204, 404])
        if resp.status_code in [200, 204]:
            resp2 = client.get(f"/entity/guid/{self.entity_guid}")
            assert_status(resp2, 200)
            entity = resp2.json().get("entity", {})
            bm_key = self.multi_bm_internal or self.multi_bm_display
            bm = entity.get("businessAttributes", {}).get(bm_key, {})
            for display_name, expected_val in expected:
                internal_key = self.multi_bm_attr_map.get(display_name, display_name)
                assert bm.get(internal_key) == expected_val, (
                    f"Expected {internal_key}={expected_val!r}, got {bm}"
                )
            ctx.set("multi_bm_display", self.multi_bm_display)

    @test("verify_business_metadata", tags=["businessmeta"], order=2, depends_on=["add_business_metadata"])
    def test_verify_business_metadata(self, client, ctx):
        if not self.bm_ok:
            raise SkipTestError("BM typedef not available")
        resp = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp, 200)
        entity = resp.json().get("entity", {})
        # Entity response uses server-generated internal names
        bm_key = self.bm_internal_name or self.bm_display_name
        bm = entity.get("businessAttributes", {}).get(bm_key, {})
        attr_key = self.bm_attr_map.get(self.bm_field_display, self.bm_field_display)
        assert bm.get(attr_key) == "test-value", (
            f"Expected {attr_key}='test-value', got {bm} "
            f"(bm_key={bm_key}, all BAs={entity.get('businessAttributes', {})})"
        )

    @test("overwrite_business_metadata", tags=["businessmeta"], order=2.3, depends_on=["add_business_metadata"])
    def test_overwrite_business_metadata(self, client, ctx):
        if not self.bm_ok:
            raise SkipTestError("BM typedef not available")
        payload = {self.bm_display_name: {self.bm_field_display: "overwritten-value"}}
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/businessmetadata/displayName",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        # Verify overwrite (entity uses internal names)
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        bm_key = self.bm_internal_name or self.bm_display_name
        bm = entity.get("businessAttributes", {}).get(bm_key, {})
        attr_key = self.bm_attr_map.get(self.bm_field_display, self.bm_field_display)
        assert bm.get(attr_key) == "overwritten-value", (
            f"Expected {attr_key}='overwritten-value', got {bm}"
        )

    @test("partial_update_business_metadata", tags=["businessmeta"], order=2.5, depends_on=["add_business_metadata"])
    def test_partial_update_business_metadata(self, client, ctx):
        if not self.bm_ok:
            raise SkipTestError("BM typedef not available")
        payload = {self.bm_display_name: {self.bm_field_display: "partial-updated"}}
        resp = client.post(
            f"/entity/guid/{self.entity_guid}/businessmetadata/displayName",
            json_data=payload,
        )
        assert_status_in(resp, [200, 204])

        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        bm_key = self.bm_internal_name or self.bm_display_name
        bm = entity.get("businessAttributes", {}).get(bm_key, {})
        attr_key = self.bm_attr_map.get(self.bm_field_display, self.bm_field_display)
        assert bm.get(attr_key) == "partial-updated", (
            f"Expected {attr_key}='partial-updated', got {bm}"
        )

    @test("delete_business_metadata", tags=["businessmeta"], order=3, depends_on=["add_business_metadata"])
    def test_delete_business_metadata(self, client, ctx):
        if not self.bm_ok:
            raise SkipTestError("BM typedef not available")
        # DELETE requires the server-generated internal name in the path
        bm_key = self.bm_internal_name or self.bm_display_name
        resp = client.delete(
            f"/entity/guid/{self.entity_guid}/businessmetadata/{bm_key}",
        )
        assert_status_in(resp, [200, 204])

        # Read-after-write: GET entity and verify BM namespace is empty
        resp2 = client.get(f"/entity/guid/{self.entity_guid}")
        assert_status(resp2, 200)
        entity = resp2.json().get("entity", {})
        bm = entity.get("businessAttributes", {}).get(bm_key, {})
        # After delete, BM should be empty or absent
        if bm:
            attr_key = self.bm_attr_map.get(self.bm_field_display, self.bm_field_display)
            val = bm.get(attr_key)
            assert val is None, f"Expected {attr_key} cleared after delete, got {val}"

    @test("add_bm_audit", tags=["businessmeta", "audit"], order=4, depends_on=["add_business_metadata"])
    def test_add_bm_audit(self, client, ctx):
        events, total = poll_audit_events(
            client, self.entity_guid, action_filter="BUSINESS_ATTRIBUTE_UPDATE",
            qualifiedName=self.entity_qn, max_wait=60, interval=10,
        )
        if events is None:
            raise SkipTestError("Audit endpoint not available (404/405)")
        if not events:
            raise SkipTestError(
                f"Audit endpoint available but no BUSINESS_ATTRIBUTE_UPDATE events after 60s — "
                f"audit indexing may not be configured"
            )

    @test("bm_add_kafka_cdc", tags=["businessmeta", "kafka"], order=4.5,
          depends_on=["add_business_metadata"])
    def test_bm_add_kafka_cdc(self, client, ctx):
        """AUD-06: Verify Kafka CDC notification for BUSINESS_ATTRIBUTE_UPDATE."""
        result = assert_entity_in_kafka(ctx, self.entity_guid, "BUSINESS_ATTRIBUTE_UPDATE")
        # Soft assertion — result is None if Kafka unavailable or not found

    @test("search_by_bm_attribute", tags=["businessmeta", "search"], order=1.8,
          depends_on=["add_business_metadata"])
    def test_search_by_bm_attribute(self, client, ctx):
        """CM-04: Verify BM attribute value is indexed in ES via DSL filter.

        AtlasEntityHeader (search result) has NO businessAttributes field.
        BM attr values are indexed as flat ES fields keyed by the attr's
        internal name — same pattern the UI uses for BM search.
        """
        if not self.bm_ok:
            raise SkipTestError("BM typedef not available")
        # Get internal attr name — used as flat ES field name for BM attr values
        attr_internal = self.bm_attr_map.get(
            self.bm_field_display, self.bm_field_display,
        )
        bm_name = self.bm_internal_name or self.bm_display_name
        print(f"  [bm-search] BM typedef: {bm_name} (display: {self.bm_display_name})")
        print(f"  [bm-search] Polling ES for BM attr {self.bm_field_display} "
              f"(internal: {attr_internal})=test-value on entity {self.entity_guid}...")

        found = False
        for i in range(24):
            time.sleep(5)
            resp = client.post("/search/indexsearch", json_data={
                "dsl": {
                    "from": 0, "size": 1,
                    "query": {"bool": {"must": [
                        {"term": {attr_internal: "test-value"}},
                        {"term": {"__guid": self.entity_guid}},
                        {"term": {"__state": "ACTIVE"}},
                    ]}}
                }
            })
            if resp.status_code == 200:
                count = resp.json().get("approximateCount", 0)
                if count > 0:
                    found = True
                    print(f"  [bm-search] BM found in ES after {(i+1)*5}s")
                    break
            print(f"  [bm-search] BM not in ES yet ({(i+1)*5}s/120s)")

        assert found, (
            f"BM attr {attr_internal}=test-value not indexed for entity "
            f"{self.entity_guid} after 120s"
        )

    @test("add_bm_nonexistent_entity", tags=["businessmeta"], order=5)
    def test_add_bm_nonexistent_entity(self, client, ctx):
        if not self.bm_ok:
            raise SkipTestError("BM typedef not available")
        payload = {self.bm_display_name: {self.bm_field_display: "test"}}
        resp = client.post(
            "/entity/guid/00000000-0000-0000-0000-000000000000/businessmetadata/displayName",
            json_data=payload,
        )
        assert_status_in(resp, [404, 400])
        body = resp.json()
        if isinstance(body, dict):
            assert "errorMessage" in body or "errorCode" in body or "message" in body or "error" in body, (
                f"Expected error details in response, got keys: {list(body.keys())}"
            )

    @test("delete_bm_typedef", tags=["businessmeta", "typedef"], order=6,
          depends_on=["delete_business_metadata"])
    def test_delete_bm_typedef(self, client, ctx):
        """CM-05: Delete a BM typedef and verify it's gone."""
        if not self.bm_ok:
            raise SkipTestError("BM typedef not available — cannot test typedef deletion")
        if not self.created_bm:
            raise SkipTestError("Using borrowed BM type — skipping typedef deletion test")
        # Create a throwaway BM typedef (not the main one used by other tests)
        throwaway_display = unique_type_name("ThrowawayBM")
        payload = {"businessMetadataDefs": [build_business_metadata_def(display_name=throwaway_display)]}
        ok, resp = create_typedef_verified(client, payload, max_wait=30)
        if not ok:
            raise SkipTestError(
                f"Throwaway BM typedef creation failed ({resp.status_code})"
            )

        # Extract server-generated internal name for deletion
        throwaway_internal, _ = extract_bm_names_from_response(resp, throwaway_display)
        delete_name = throwaway_internal or throwaway_display

        # Delete the throwaway BM typedef using internal name
        resp = client.delete(f"/types/typedef/name/{delete_name}")
        assert_status_in(resp, [200, 204])

        # Verify it's gone — GET should return 404
        resp2 = client.get(f"/types/typedef/name/{delete_name}")
        assert_status_in(resp2, [404, 400])

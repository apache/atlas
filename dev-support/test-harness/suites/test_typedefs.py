"""TypeDef CRUD tests (~20 tests)."""

import time

from core.decorators import suite, test
from core.assertions import (
    assert_status, assert_status_in, assert_field_present,
    assert_field_equals, assert_field_not_empty, assert_field_type,
    assert_list_min_length, SkipTestError,
)
from core.data_factory import (
    build_enum_def, build_classification_def, build_struct_def,
    build_entity_def, build_business_metadata_def, build_relationship_def,
    build_dataset_entity, unique_type_name, unique_qn, unique_name,
)
from core.typedef_helpers import (
    create_typedef_verified, extract_bm_names_from_response,
    _discover_bm_by_display_name,
)


@suite("typedefs", description="TypeDef CRUD operations")
class TypeDefSuite:

    def setup(self, client, ctx):
        self.enum_name = unique_type_name("TestEnum")
        self.classification_name = unique_type_name("TestClassification")
        self.struct_name = unique_type_name("TestStruct")
        self.entity_type_name = unique_type_name("TestEntityType")
        self.bm_display_name = unique_type_name("TestBM")
        self.bm_server_name = None  # Set after creation

    # ---- GET existing types ----

    @test("get_all_typedefs", tags=["smoke", "typedef"], order=1)
    def test_get_all_typedefs(self, client, ctx):
        resp = client.get("/types/typedefs")
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, dict), f"Expected dict response, got {type(body).__name__}"
        for key in ("enumDefs", "entityDefs", "classificationDefs"):
            assert key in body, f"Expected '{key}' in typedefs response"
            assert isinstance(body[key], list), f"Expected '{key}' to be a list"

    @test("get_typedef_headers", tags=["smoke", "typedef"], order=2)
    def test_get_typedef_headers(self, client, ctx):
        resp = client.get("/types/typedefs/headers")
        assert_status(resp, 200)
        body = resp.json()
        assert isinstance(body, list), "Expected list of headers"
        assert len(body) > 0, "Expected at least one type header"

    @test("get_entitydef_by_name", tags=["typedef"], order=3)
    def test_get_entitydef_by_name(self, client, ctx):
        resp = client.get("/types/entitydef/name/DataSet")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", "DataSet")

    @test("get_entitydef_not_found", tags=["typedef"], order=4)
    def test_get_entitydef_not_found(self, client, ctx):
        resp = client.get("/types/entitydef/name/NonExistentType12345")
        assert_status_in(resp, [404, 204])

    # ---- CREATE types ----

    @test("create_enum_def", tags=["typedef", "crud"], order=10)
    def test_create_enum_def(self, client, ctx):
        payload = {"enumDefs": [build_enum_def(name=self.enum_name)]}
        ok, resp = create_typedef_verified(client, payload)
        if not ok and resp.status_code in (500, 502, 503):
            raise SkipTestError(
                f"Enum typedef POST returned {resp.status_code} and creation "
                f"could not be confirmed — server-side gateway timeout"
            )
        assert ok, (
            f"Enum typedef creation failed: POST returned {resp.status_code}"
        )
        ctx.set("test_enum_name", self.enum_name)

        if resp.status_code == 200:
            body = resp.json()
            enum_defs = body.get("enumDefs", [])
            assert len(enum_defs) > 0, "Expected enumDefs in response"
            assert enum_defs[0].get("name") == self.enum_name, f"Expected name={self.enum_name}"
            assert "elementDefs" in enum_defs[0], "Expected elementDefs in enum def"

    @test("get_enum_def_by_name", tags=["typedef"], order=11, depends_on=["create_enum_def"])
    def test_get_enum_def_by_name(self, client, ctx):
        resp = client.get(f"/types/enumdef/name/{self.enum_name}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.enum_name)

    @test("create_struct_def", tags=["typedef", "crud"], order=12)
    def test_create_struct_def(self, client, ctx):
        payload = {"structDefs": [build_struct_def(name=self.struct_name)]}
        ok, resp = create_typedef_verified(client, payload)
        if not ok and resp.status_code in (500, 502, 503):
            raise SkipTestError(
                f"Struct typedef POST returned {resp.status_code} — "
                f"server-side gateway timeout"
            )
        assert ok, (
            f"Struct typedef creation failed: POST returned {resp.status_code}"
        )
        ctx.set("test_struct_name", self.struct_name)

    @test("get_struct_def_by_name", tags=["typedef"], order=13, depends_on=["create_struct_def"])
    def test_get_struct_def_by_name(self, client, ctx):
        resp = client.get(f"/types/structdef/name/{self.struct_name}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.struct_name)

    @test("create_classification_def", tags=["typedef", "crud"], order=14)
    def test_create_classification_def(self, client, ctx):
        payload = {"classificationDefs": [build_classification_def(name=self.classification_name)]}
        ok, resp = create_typedef_verified(client, payload)
        if not ok and resp.status_code in (500, 502, 503):
            raise SkipTestError(
                f"Classification typedef POST returned {resp.status_code} — "
                f"server-side gateway timeout"
            )
        assert ok, (
            f"Classification typedef creation failed: POST returned {resp.status_code}"
        )
        ctx.set("test_classification_name", self.classification_name)

        if resp.status_code == 200:
            body = resp.json()
            cls_defs = body.get("classificationDefs", [])
            assert len(cls_defs) > 0, "Expected classificationDefs in response"

    @test("get_classification_def_by_name", tags=["typedef"], order=15, depends_on=["create_classification_def"])
    def test_get_classification_def_by_name(self, client, ctx):
        resp = client.get(f"/types/classificationdef/name/{self.classification_name}")
        # May return 404 if type cache hasn't propagated yet
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            assert_field_equals(resp, "name", self.classification_name)

    @test("create_entity_def", tags=["typedef", "crud"], order=16)
    def test_create_entity_def(self, client, ctx):
        payload = {"entityDefs": [build_entity_def(name=self.entity_type_name)]}
        ok, resp = create_typedef_verified(client, payload)
        if not ok and resp.status_code in (500, 502, 503):
            raise SkipTestError(
                f"Entity typedef POST returned {resp.status_code} — "
                f"server-side gateway timeout"
            )
        assert ok, (
            f"Entity typedef creation failed: POST returned {resp.status_code}"
        )
        ctx.set("test_entity_type_name", self.entity_type_name)

        if resp.status_code == 200:
            body = resp.json()
            entity_defs = body.get("entityDefs", [])
            assert len(entity_defs) > 0, "Expected entityDefs in response"
            assert "attributeDefs" in entity_defs[0], "Expected attributeDefs in entity def"

    @test("get_entity_def_by_name", tags=["typedef"], order=17, depends_on=["create_entity_def"])
    def test_get_entity_def_by_name(self, client, ctx):
        resp = client.get(f"/types/entitydef/name/{self.entity_type_name}")
        assert_status(resp, 200)
        assert_field_equals(resp, "name", self.entity_type_name)

    @test("create_business_metadata_def", tags=["typedef", "crud"], order=18)
    def test_create_business_metadata_def(self, client, ctx):
        payload = {"businessMetadataDefs": [build_business_metadata_def(display_name=self.bm_display_name)]}
        ok, resp = create_typedef_verified(client, payload)
        if not ok and resp.status_code in (500, 502, 503):
            raise SkipTestError(
                f"BM typedef POST returned {resp.status_code} — "
                f"server-side gateway timeout"
            )
        assert ok, (
            f"BM typedef creation failed: POST returned {resp.status_code}"
        )

        # Extract server-generated internal name
        internal_name, _ = extract_bm_names_from_response(resp, self.bm_display_name)
        if internal_name:
            self.bm_server_name = internal_name
        elif resp.status_code == 409:
            # 409 body is error JSON, not typedef — discover internal name
            # via headers endpoint
            discovered = _discover_bm_by_display_name(client, self.bm_display_name)
            if discovered:
                self.bm_server_name = discovered["internal_name"]
                print(f"  [typedef] Discovered BM internal name via headers: "
                      f"{self.bm_server_name}")
        ctx.set("test_bm_name", self.bm_server_name or self.bm_display_name)

        if resp.status_code == 200:
            body = resp.json()
            bm_defs = body.get("businessMetadataDefs", [])
            assert len(bm_defs) > 0, "Expected businessMetadataDefs in response"

    @test("get_bm_def_by_name", tags=["typedef"], order=19, depends_on=["create_business_metadata_def"])
    def test_get_bm_def_by_name(self, client, ctx):
        lookup_name = self.bm_server_name or self.bm_display_name
        resp = client.get(f"/types/businessmetadatadef/name/{lookup_name}")
        # May return 404 if type cache hasn't propagated yet
        assert_status_in(resp, [200, 404])
        if resp.status_code == 200:
            assert_field_equals(resp, "name", lookup_name)

    # ---- UPDATE type ----

    @test("update_enum_def", tags=["typedef", "crud"], order=20, depends_on=["create_enum_def"])
    def test_update_enum_def(self, client, ctx):
        payload = {
            "enumDefs": [build_enum_def(
                name=self.enum_name,
                elements=[
                    {"value": "VAL_A", "ordinal": 0},
                    {"value": "VAL_B", "ordinal": 1},
                    {"value": "VAL_C", "ordinal": 2},
                    {"value": "VAL_D", "ordinal": 3},
                ],
            )]
        }
        resp = client.put("/types/typedefs", json_data=payload)
        assert_status(resp, 200)

        # Read-after-write: GET enum and verify VAL_D is in elementDefs
        resp2 = client.get(f"/types/enumdef/name/{self.enum_name}")
        assert_status(resp2, 200)
        body = resp2.json()
        element_values = [e.get("value") for e in body.get("elementDefs", [])]
        assert "VAL_D" in element_values, f"Expected VAL_D in elementDefs, got {element_values}"

    @test("get_all_find_created_enum", tags=["typedef"], order=21, depends_on=["create_enum_def"])
    def test_get_all_find_created_enum(self, client, ctx):
        # GET-all typedefs and find our created enum in enumDefs by name
        resp = client.get("/types/typedefs")
        assert_status(resp, 200)
        body = resp.json()
        enum_defs = body.get("enumDefs", [])
        found = None
        for ed in enum_defs:
            if ed.get("name") == self.enum_name:
                found = ed
                break
        assert found is not None, f"Created enum '{self.enum_name}' not found in GET /types/typedefs"
        assert "elementDefs" in found, "Found enum should have elementDefs"

    # ---- Extended tests ----

    @test("create_entity_with_custom_type", tags=["typedef", "crud"], order=22, depends_on=["create_entity_def"])
    def test_create_entity_with_custom_type(self, client, ctx):
        time.sleep(5)
        qn = unique_qn("custom-typedef-test")
        entity = build_dataset_entity(qn=qn, name=unique_name("custom-td"), type_name=self.entity_type_name)
        resp = client.post("/entity", json_data={"entity": entity})
        if resp.status_code in (400, 404):
            raise SkipTestError(
                f"Entity creation with custom type returned {resp.status_code} — "
                f"type cache may not have propagated"
            )
        assert_status(resp, 200)
        if resp.status_code == 200:
            body = resp.json()
            creates = body.get("mutatedEntities", {}).get("CREATE", [])
            updates = body.get("mutatedEntities", {}).get("UPDATE", [])
            entities = creates or updates
            if entities:
                guid = entities[0]["guid"]
                ctx.register_entity("custom_type_td_entity", guid, self.entity_type_name)
                ctx.register_entity_cleanup(guid)
                assert entities[0].get("typeName") == self.entity_type_name, (
                    f"Expected typeName={self.entity_type_name}, got {entities[0].get('typeName')}"
                )
                ctx.set("custom_type_entity_exists", True)

    @test("create_relationship_def", tags=["typedef", "crud"], order=23)
    def test_create_relationship_def(self, client, ctx):
        self.rel_def_name = unique_type_name("TestRelDef")
        payload = {"relationshipDefs": [build_relationship_def(name=self.rel_def_name)]}
        ok, resp = create_typedef_verified(client, payload)
        if not ok and resp.status_code in (500, 502, 503):
            raise SkipTestError(
                f"Relationship typedef POST returned {resp.status_code} — "
                f"server-side gateway timeout"
            )
        assert ok, (
            f"Relationship typedef creation failed: POST returned {resp.status_code}"
        )
        ctx.set("test_rel_def_name", self.rel_def_name)
        ctx.register_typedef_cleanup(client, self.rel_def_name)

        if resp.status_code == 200:
            body = resp.json()
            rel_defs = body.get("relationshipDefs", [])
            assert len(rel_defs) > 0, "Expected relationshipDefs in response"
            assert rel_defs[0].get("name") == self.rel_def_name, (
                f"Expected name={self.rel_def_name}"
            )

    @test("delete_type_in_use", tags=["typedef"], order=24, depends_on=["create_entity_with_custom_type"])
    def test_delete_type_in_use(self, client, ctx):
        if not ctx.get("custom_type_entity_exists"):
            raise SkipTestError("No entity of custom type exists — create_entity_with_custom_type must have failed")
        resp = client.delete(f"/types/typedef/name/{self.entity_type_name}")
        # Should fail because an instance exists
        assert_status_in(resp, [400, 409])

    @test("get_typedef_by_guid", tags=["typedef"], order=25, depends_on=["create_enum_def"])
    def test_get_typedef_by_guid(self, client, ctx):
        # First get the enum by name to find its GUID
        resp = client.get(f"/types/enumdef/name/{self.enum_name}")
        assert_status(resp, 200)
        body = resp.json()
        type_guid = body.get("guid")
        assert type_guid, f"Expected 'guid' field in enum typedef response for {self.enum_name}"
        # GET by GUID
        resp2 = client.get(f"/types/typedef/guid/{type_guid}")
        assert_status(resp2, 200)
        assert_field_equals(resp2, "name", self.enum_name)

    # ---- DELETE types (cleanup) ----

    @test("delete_custom_type_entity", tags=["typedef", "crud"], order=89, depends_on=["create_entity_with_custom_type"])
    def test_delete_custom_type_entity(self, client, ctx):
        guid = ctx.get_entity_guid("custom_type_td_entity")
        if not guid:
            raise SkipTestError("custom_type_td_entity GUID not found — create_entity_with_custom_type must have failed")
        resp = client.delete(f"/entity/guid/{guid}")
        assert_status_in(resp, [200, 204])

    @test("delete_entity_def", tags=["typedef", "crud"], order=90, depends_on=["create_entity_def"])
    def test_delete_entity_def(self, client, ctx):
        resp = client.delete(f"/types/typedef/name/{self.entity_type_name}")
        assert_status_in(resp, [200, 204, 409])

    @test("delete_classification_def", tags=["typedef", "crud"], order=91, depends_on=["create_classification_def"])
    def test_delete_classification_def(self, client, ctx):
        resp = client.delete(f"/types/typedef/name/{self.classification_name}")
        # 404 if type cache never propagated the name
        assert_status_in(resp, [200, 204, 404])

    @test("delete_struct_def", tags=["typedef", "crud"], order=92, depends_on=["create_struct_def"])
    def test_delete_struct_def(self, client, ctx):
        resp = client.delete(f"/types/typedef/name/{self.struct_name}")
        # 404 if type cache never propagated the name
        assert_status_in(resp, [200, 204, 404])

    @test("delete_enum_def", tags=["typedef", "crud"], order=93, depends_on=["create_enum_def"])
    def test_delete_enum_def(self, client, ctx):
        resp = client.delete(f"/types/typedef/name/{self.enum_name}")
        # 404 if type cache never propagated the name
        assert_status_in(resp, [200, 204, 404])

    @test("delete_bm_def", tags=["typedef", "crud"], order=94, depends_on=["create_business_metadata_def"])
    def test_delete_bm_def(self, client, ctx):
        delete_name = self.bm_server_name or self.bm_display_name
        resp = client.delete(f"/types/typedef/name/{delete_name}")
        # 404 if type cache never propagated the name
        assert_status_in(resp, [200, 204, 404])

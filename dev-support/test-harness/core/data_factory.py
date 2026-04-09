"""Entity and typedef payload builders for test data."""

import time
import uuid

TIMESTAMP = int(time.time())
PREFIX = f"test-harness-{TIMESTAMP}"


def unique_qn(prefix="th"):
    return f"{PREFIX}/{prefix}-{uuid.uuid4().hex[:8]}"


def unique_name(prefix="th"):
    return f"{prefix}-{TIMESTAMP}-{uuid.uuid4().hex[:6]}"


def unique_type_name(prefix="Th"):
    """Generate a unique name safe for typedef names (alphanumeric + underscore only)."""
    return f"{prefix}_{TIMESTAMP}_{uuid.uuid4().hex[:6]}"


# ---- TypeDef builders ----

def build_enum_def(name=None, elements=None):
    name = name or unique_type_name("TestEnum")
    elements = elements or [
        {"value": "VAL_A", "ordinal": 0},
        {"value": "VAL_B", "ordinal": 1},
        {"value": "VAL_C", "ordinal": 2},
    ]
    return {
        "name": name,
        "displayName": name,
        "description": f"Test enum {name}",
        "typeVersion": "1.0",
        "elementDefs": elements,
    }


def build_classification_def(name=None, super_types=None, attribute_defs=None):
    name = name or unique_type_name("TestClassification")
    return {
        "name": name,
        "displayName": name,
        "description": f"Test classification {name}",
        "typeVersion": "1.0",
        "superTypes": super_types or [],
        "attributeDefs": attribute_defs or [],
    }


def build_struct_def(name=None, attribute_defs=None):
    name = name or unique_type_name("TestStruct")
    return {
        "name": name,
        "displayName": name,
        "description": f"Test struct {name}",
        "typeVersion": "1.0",
        "attributeDefs": attribute_defs or [
            {
                "name": "field1",
                "typeName": "string",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": False,
            }
        ],
    }


def build_entity_def(name=None, super_types=None, attribute_defs=None):
    name = name or unique_type_name("TestEntityType")
    return {
        "name": name,
        "displayName": name,
        "description": f"Test entity type {name}",
        "typeVersion": "1.0",
        "superTypes": super_types or ["DataSet"],
        "attributeDefs": attribute_defs or [
            {
                "name": "testAttr",
                "typeName": "string",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": True,
            }
        ],
    }


def build_business_metadata_def(display_name=None, attribute_defs=None, include_attrs=True):
    """Build a BM typedef matching the UI creation pattern.

    The server auto-generates random 22-char ``name`` values for BM typedefs
    and their attributes (``TypesREST.setRandomNameForEntityAndAttributeDefs``).
    We omit ``name`` and only provide ``displayName`` — the same pattern the UI
    uses.  After POST, read back the server-generated names from the response.

    Args:
        display_name:   Human-readable name for the BM typedef
        attribute_defs: Custom attribute definitions list
        include_attrs:  If False, omit attributeDefs entirely (UI Step 1 pattern)
    """
    display_name = display_name or unique_type_name("TestBM")
    bm_def = {
        # No "name" — server auto-generates a random 22-char internal name
        "displayName": display_name,
        "description": f"Test business metadata {display_name}",
        "options": {},
    }
    if include_attrs:
        bm_def["attributeDefs"] = attribute_defs or [
            {
                "name": "",          # Empty — server auto-generates
                "displayName": "bmField1",
                "typeName": "string",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": True,
                "options": {
                    "applicableEntityTypes": "[\"Asset\"]",
                    "maxStrLength": "50",
                },
            }
        ]
    return bm_def


def build_relationship_def(name=None, end1_type="DataSet", end2_type="DataSet"):
    name = name or unique_type_name("TestRelDef")
    return {
        "name": name,
        "description": f"Test relationship {name}",
        "typeVersion": "1.0",
        "relationshipCategory": "ASSOCIATION",
        "propagateTags": "NONE",
        "endDef1": {
            "type": end1_type,
            "name": f"{name}_end1",
            "isContainer": False,
            "cardinality": "SET",
        },
        "endDef2": {
            "type": end2_type,
            "name": f"{name}_end2",
            "isContainer": False,
            "cardinality": "SET",
        },
    }


# ---- Environment detection ----

def detect_process_io_type(client):
    """Determine which entity type to use for Process inputs/outputs.

    On Atlan (preprod/staging), the relationship ``process_catalog_outputs``
    expects outputs of type ``Catalog``.  DataSet does NOT extend Catalog, so
    Process creation with DataSet outputs returns 400.

    Returns "Catalog" if the type exists, else "DataSet" (local dev).
    Caches the result on the client instance to avoid repeated lookups.
    """
    cached = getattr(client, "_process_io_type", None)
    if cached:
        return cached
    resp = client.get("/types/typedef/name/Catalog")
    if resp.status_code == 200:
        client._process_io_type = "Catalog"
        print("  [detect] Catalog type found — using Catalog for process I/O entities")
        return "Catalog"
    client._process_io_type = "DataSet"
    print("  [detect] Catalog type not found — using DataSet for process I/O entities")
    return "DataSet"


def create_process_with_io(client, ctx, suffix, input_guids, output_guids,
                           entity_type=None, timeout=120):
    """Create a Process entity with proper entity type detection and timeout.

    Detects the correct entity type for inputs/outputs (Catalog on staging,
    DataSet on local dev) and uses a 120s timeout for process creation which
    can be slow on staging.

    Returns (ok: bool, proc_guid: str or None).
    """
    if entity_type is None:
        entity_type = detect_process_io_type(client)
    inputs = [{"guid": g, "typeName": entity_type} for g in input_guids]
    outputs = [{"guid": g, "typeName": entity_type} for g in output_guids]
    proc = build_process_entity(
        qn=unique_qn(suffix), name=unique_name(suffix),
        inputs=inputs, outputs=outputs,
    )
    resp = client.post("/entity", json_data={"entity": proc}, timeout=timeout)
    if resp.status_code != 200:
        detail = repr(resp.body)[:500] if resp.body else "(empty body)"
        print(f"  [process] Process creation for {suffix} returned "
              f"{resp.status_code}: {detail}")
        return False, None
    body = resp.json()
    creates = body.get("mutatedEntities", {}).get("CREATE", [])
    updates = body.get("mutatedEntities", {}).get("UPDATE", [])
    entities = creates or updates
    if not entities:
        print(f"  [process] Process creation for {suffix} got 200 but "
              f"no entities: {list(body.keys())}")
        return False, None
    guid = entities[0]["guid"]
    ctx.register_entity_cleanup(guid)
    return True, guid


# ---- Entity builders ----

def build_dataset_entity(qn=None, name=None, type_name="DataSet", extra_attrs=None):
    qn = qn or unique_qn("dataset")
    name = name or unique_name("dataset")
    attrs = {"qualifiedName": qn, "name": name}
    if extra_attrs:
        attrs.update(extra_attrs)
    return {
        "typeName": type_name,
        "attributes": attrs,
    }


def build_process_entity(qn=None, name=None, inputs=None, outputs=None):
    qn = qn or unique_qn("process")
    name = name or unique_name("process")
    entity = {
        "typeName": "Process",
        "attributes": {"qualifiedName": qn, "name": name},
    }
    # inputs/outputs are relationship attributes (process_dataset_inputs/outputs)
    rel_attrs = {}
    if inputs:
        rel_attrs["inputs"] = inputs
    if outputs:
        rel_attrs["outputs"] = outputs
    if rel_attrs:
        entity["relationshipAttributes"] = rel_attrs
    return entity


def build_glossary(name=None):
    name = name or unique_name("glossary")
    return {
        "typeName": "AtlasGlossary",
        "attributes": {
            "qualifiedName": unique_qn("glossary"),
            "name": name,
            "shortDescription": f"Test glossary {name}",
        },
    }


def build_glossary_term(glossary_guid, name=None):
    name = name or unique_name("term")
    return {
        "typeName": "AtlasGlossaryTerm",
        "attributes": {
            "qualifiedName": unique_qn("term"),
            "name": name,
            "shortDescription": f"Test term {name}",
        },
        "relationshipAttributes": {
            "anchor": {"guid": glossary_guid, "typeName": "AtlasGlossary"},
        },
    }


def build_glossary_category(glossary_guid, name=None, parent_category_guid=None):
    name = name or unique_name("category")
    rel_attrs = {
        "anchor": {"guid": glossary_guid, "typeName": "AtlasGlossary"},
    }
    if parent_category_guid:
        rel_attrs["parentCategory"] = {
            "guid": parent_category_guid,
            "typeName": "AtlasGlossaryCategory",
        }
    return {
        "typeName": "AtlasGlossaryCategory",
        "attributes": {
            "qualifiedName": unique_qn("category"),
            "name": name,
            "shortDescription": f"Test category {name}",
        },
        "relationshipAttributes": rel_attrs,
    }


# ---- Data Mesh entity builders ----

def build_domain_entity(name=None, parent_domain_guid=None):
    """DataDomain entity. QN is auto-generated by preprocessor.

    For sub-domains, pass ``parent_domain_guid`` — the preprocessor reads
    ``relationshipAttributes.parentDomain``, NOT ``attributes.parentDomainQualifiedName``
    (which is an OUTPUT the preprocessor writes).
    """
    name = name or unique_name("domain")
    attrs = {"qualifiedName": unique_qn("domain"), "name": name}
    entity = {
        "typeName": "DataDomain",
        "attributes": attrs,
    }
    if parent_domain_guid:
        entity["relationshipAttributes"] = {
            "parentDomain": {
                "guid": parent_domain_guid,
                "typeName": "DataDomain",
            },
        }
    return entity


def build_data_product_entity(name=None, domain_guid=None):
    """DataProduct entity linked to a domain.

    dataProductAssetsDSL is mandatory (DataProductPreProcessor validates it).
    We use a narrow DSL that matches nothing to avoid scanning all assets.
    """
    import json
    name = name or unique_name("product")
    # DSL that matches no real assets (fake GUID filter)
    assets_dsl = json.dumps({
        "query": {"bool": {"must": [
            {"term": {"__guid": "00000000-0000-0000-0000-000000000000"}},
        ]}}
    })
    attrs = {
        "qualifiedName": unique_qn("product"),
        "name": name,
        "dataProductAssetsDSL": assets_dsl,
    }
    entity = {
        "typeName": "DataProduct",
        "attributes": attrs,
    }
    if domain_guid:
        entity["relationshipAttributes"] = {
            "dataDomain": {"guid": domain_guid, "typeName": "DataDomain"},
        }
    return entity


# ---- Persona / Purpose / AuthPolicy builders ----

def build_persona_entity(name=None):
    """Persona entity. QN auto-generated by preprocessor.

    isAccessControlEnabled is required — AccessControlUtils.getIsAccessControlEnabled()
    unboxes Boolean to boolean and NPEs if the attribute is null.
    """
    name = name or unique_name("persona")
    return {
        "typeName": "Persona",
        "attributes": {
            "qualifiedName": unique_qn("persona"),
            "name": name,
            "description": f"Test persona {name}",
            "isAccessControlEnabled": True,
        },
    }


def build_purpose_entity(name=None, classifications=None):
    """Purpose entity.

    purposeClassifications is required by PurposePreProcessor — server returns
    400 ("Please provide purposeClassifications for Purpose") without it.
    Defaults to ["Confidential"] which is a built-in classification on Atlan.
    """
    name = name or unique_name("purpose")
    return {
        "typeName": "Purpose",
        "attributes": {
            "qualifiedName": unique_qn("purpose"),
            "name": name,
            "description": f"Test purpose {name}",
            "isAccessControlEnabled": True,
            "purposeClassifications": classifications or ["Confidential"],
        },
    }


def build_auth_policy_entity(persona_guid, name=None):
    """AuthPolicy linked to a Persona.

    AuthPolicyValidator requires: policyServiceName, policyType, policyActions,
    policySubCategory.  We use glossary sub-category to avoid needing a real
    Connection entity (metadata/data sub-categories require connectionQualifiedName
    pointing to an existing Connection).  QN is auto-generated by preprocessor.
    """
    name = name or unique_name("policy")
    return {
        "typeName": "AuthPolicy",
        "attributes": {
            "qualifiedName": unique_qn("policy"),
            "name": name,
            "policyType": "allow",
            "policyCategory": "persona",
            "policySubCategory": "glossary",
            "policyServiceName": "atlas",
            "policyResourceCategory": "CUSTOM",
            "policyActions": ["persona-glossary-read"],
            "policyResources": ["entity-type:AtlasGlossary"],
        },
        "relationshipAttributes": {
            "accessControl": {"guid": persona_guid, "typeName": "Persona"},
        },
    }


# ---- Multi-attribute BM typedef builder ----

def build_multi_attr_business_metadata_def(display_name=None):
    """BM def with string + int + boolean attributes (UI pattern — no ``name``)."""
    display_name = display_name or unique_type_name("TestMultiBM")
    return {
        "displayName": display_name,
        "description": f"Multi-attribute BM {display_name}",
        "options": {},
        "attributeDefs": [
            {
                "name": "",
                "displayName": "bmStrField",
                "typeName": "string",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": True,
                "options": {
                    "applicableEntityTypes": "[\"Asset\"]",
                    "maxStrLength": "50",
                },
            },
            {
                "name": "",
                "displayName": "bmIntField",
                "typeName": "int",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": True,
                "options": {
                    "applicableEntityTypes": "[\"Asset\"]",
                },
            },
            {
                "name": "",
                "displayName": "bmBoolField",
                "typeName": "boolean",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": True,
                "options": {
                    "applicableEntityTypes": "[\"Asset\"]",
                },
            },
        ],
    }

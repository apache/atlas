"""Typedef creation helper with verify-after-500 for server-side timeouts.

On preprod/staging, POST /types/typedefs often returns 500 because the
gateway timeout (~15s) is shorter than the server-side processing time.

Additionally, the type cache on staging/preprod NEVER refreshes for certain
typedef categories (classification, businessMetadata).  POST returns 200
with the full typedef body, subsequent POSTs return 409 ("already exists"),
but GET /types/{category}/name/{name} keeps returning 404 indefinitely.

Strategy:
  For non-BM types (skip_repost=False):
    1. POST with client-level retries enabled (default 3 attempts).
       The client retries on 500/502/503 with 5s, 10s waits.
    2. If the server created the type during attempt 1 (which returned 500
       due to gateway timeout), attempt 2 sees the type exists and returns
       409 quickly (<1s).  This is the common fast path on preprod.
    3. If all client retries return 500: single confirmatory re-POST after
       10s, then GET polling for max_wait seconds (safety net).

  For BM types (skip_repost=True):
    1. POST with retries=0 (single attempt) because each POST generates a
       new random internal name — retries would create duplicates.
    2. If 500: return failure immediately (caller discovers existing types).

  For both: 200 = created, 409 = already exists → success immediately.

GET polling is ONLY used to verify creation after 500 errors.
For 200 and 409, we trust the POST response and skip GET entirely
(the type cache may never refresh on staging for classification/BM defs).
"""

import json
import time

# Maps payload category key -> GET path segment for verification
_CATEGORY_TO_PATH = {
    "classificationDefs": "classificationdef",
    "businessMetadataDefs": "businessmetadatadef",
    "enumDefs": "enumdef",
    "structDefs": "structdef",
    "entityDefs": "entitydef",
    "relationshipDefs": "relationshipdef",
}


def create_typedef_verified(client, payload, max_wait=60, interval=10,
                            timeout=120, skip_repost=False):
    """POST /types/typedefs with verification for server-side timeouts.

    Strategy:
      For non-BM types (skip_repost=False):
        1. POST with client-level retries (default 3 attempts, 5s/10s waits).
           On preprod the gateway returns 500 after ~15s, but the server
           creates the type.  The client's retry sees 409 quickly.
        2. If client retries exhaust (all 500): single confirmatory re-POST
           after 10s, then GET polling for max_wait seconds.

      For BM types (skip_repost=True):
        1. POST with retries=0 (each POST generates a new random name).
        2. If 500: return failure immediately.

    Args:
        client:      AtlasClient instance
        payload:     dict with typedef category keys
        max_wait:    max seconds for GET polling after retries exhaust (default 60)
        interval:    seconds between GET polls (default 10)
        timeout:     HTTP timeout in seconds for POST attempts (default 120)
        skip_repost: if True, use retries=0 and don't re-POST after 500.
                     Use for BM payloads where each POST generates a new
                     random name (re-POST creates duplicates, not 409).

    Returns:
        (success: bool, resp: ApiResponse)
        success=True means the type was created (POST 200, 409,
        or confirmed via re-POST 409 / GET polling).
    """
    verify_targets = _extract_verify_targets(payload)
    names = [n for _, n in verify_targets] if verify_targets else []

    # For BM types: retries=0 (single attempt — each POST creates new random name)
    # For non-BM types: retries=None (client default: 3 attempts with 5s, 10s waits)
    # The client retries on 500/502/503. On preprod, retry 2 gets 409 quickly
    # because the server checks for existing types first (<1s) before heavy work.
    retries = 0 if skip_repost else None
    resp = client.post("/types/typedefs", json_data=payload, timeout=timeout,
                       retries=retries)

    # 409 = type already exists — definitive success
    if resp.status_code == 409:
        print(f"  [typedef] 409 (already exists) — type confirmed: {names}")
        return True, resp

    # 200 = type created — trust the server contract
    # (POST /types/typedefs always returns 200 with AtlasTypesDef body on success)
    if resp.status_code == 200:
        print(f"  [typedef] 200 — type created: {names}")
        return True, resp

    # 408/500/502/503 — server error or gateway timeout.
    # For non-BM types, client retries already exhausted (3 attempts).
    # For BM types (skip_repost=True), only 1 attempt was made.
    if resp.status_code in (408, 500, 502, 503):
        if skip_repost:
            print(f"  [typedef] POST returned {resp.status_code} — "
                  f"skip_repost=True, not re-posting (BM types get new "
                  f"random names per POST)")
            return False, resp

        # Non-BM: client retries exhausted. One confirmatory re-POST after
        # 10s wait (catches types created during the last client attempt),
        # then fall back to GET polling.
        print(f"  [typedef] POST returned {resp.status_code} after client "
              f"retries, one confirmatory re-POST in 10s...")
        time.sleep(10)
        resp2 = client.post("/types/typedefs", json_data=payload,
                            timeout=timeout, retries=0)
        if resp2.status_code == 409:
            print(f"  [typedef] Re-POST returned 409 — type was created: "
                  f"{names}")
            return True, resp2
        if resp2.status_code == 200:
            print(f"  [typedef] Re-POST returned 200 — type confirmed: "
                  f"{names}")
            return True, resp2
        print(f"  [typedef] Re-POST returned {resp2.status_code}")

        # Fall back to GET polling for types whose name we know.
        # BM types have server-generated names so we can't poll GET.
        if verify_targets:
            print(f"  [typedef] Polling GET for up to {max_wait}s...")
            ok = _poll_types_queryable(client, verify_targets, max_wait,
                                       interval, label="verify-after-POST")
            if ok:
                print(f"  [typedef] Types confirmed via GET: {names}")
                return True, resp

        print(f"  [typedef] POST {resp.status_code} — type not confirmed")
        return False, resp

    # Other status codes (400, 404, etc.) — genuine failure
    print(f"  [typedef] POST returned {resp.status_code} — not retryable")
    return False, resp


def _extract_verify_targets(payload):
    """Extract (get_path_segment, name) pairs from a typedef payload.

    BM typedefs created with the UI pattern have no ``name`` (server
    auto-generates it), so we skip them — GET verification won't work
    anyway on staging due to the type cache issue.
    """
    targets = []
    for category_key, defs_list in payload.items():
        path_segment = _CATEGORY_TO_PATH.get(category_key)
        if not path_segment or not isinstance(defs_list, list):
            continue
        for typedef in defs_list:
            name = typedef.get("name") if isinstance(typedef, dict) else None
            if name:
                targets.append((path_segment, name))
    return targets


def extract_bm_names_from_response(resp, display_name):
    """Extract server-generated internal name and attr names from POST response.

    After ``POST /types/typedefs``, the server replaces human-readable BM
    ``name`` values with random 22-char strings.  This helper finds the BM
    def matching *display_name* in the response body and returns the
    server-assigned internal names.

    Args:
        resp:          ApiResponse from ``POST /types/typedefs``
        display_name:  The ``displayName`` we used when creating the BM

    Returns:
        (internal_name, attr_map) where attr_map = {displayName: internalName}.
        Returns (None, {}) if extraction fails.
    """
    if not resp or resp.status_code not in (200, 409):
        return None, {}
    try:
        body = resp.json()
    except Exception:
        return None, {}

    for bm_def in body.get("businessMetadataDefs", []):
        if bm_def.get("displayName") == display_name:
            internal_name = bm_def.get("name")
            attr_map = {}
            for attr_def in bm_def.get("attributeDefs", []):
                attr_display = attr_def.get("displayName")
                attr_internal = attr_def.get("name")
                if attr_display and attr_internal:
                    attr_map[attr_display] = attr_internal
            if internal_name:
                print(f"  [bm] Server-generated name: {internal_name} "
                      f"(displayName={display_name})")
                print(f"  [bm] Attr map: {attr_map}")
            return internal_name, attr_map
    return None, {}


# Module-level flag: set True when BM typedef creation consistently fails (500).
# Subsequent ensure_bm_types() calls skip retries and go straight to existing
# type discovery, saving ~120s of wasted time per call.
_bm_creation_broken = False


def ensure_classification_types(client, names):
    """Create classification types, fall back to existing types if not usable.

    On staging/preprod, newly created classification types may not propagate
    across pods.  POST /types/typedefs returns 200/409, but GET returns 404
    and POST /entity/.../classifications returns 404 ("invalid classification").

    When this happens, discovers existing classification types that ARE
    queryable and returns those instead.

    Args:
        client:  AtlasClient instance
        names:   list of classification type names to create

    Returns:
        (usable_names: list[str], created_new: bool, ok: bool)
        - usable_names: type names usable for entity operations
        - created_new: True if new types were created (caller should cleanup)
        - ok: True if enough usable types available
    """
    from core.data_factory import build_classification_def

    count = len(names)
    defs = [build_classification_def(name=n) for n in names]

    # Step 1: Try to create new types.
    # Short max_wait (30s) because we have our own fallback to existing types.
    created, _resp = create_typedef_verified(
        client, {"classificationDefs": defs}, max_wait=30,
    )

    if created:
        # Step 2: Check if first type is queryable (usable across pods)
        check = client.get(f"/types/classificationdef/name/{names[0]}")
        if check.status_code == 200:
            return list(names), True, True

        # Created but not queryable — cross-pod type cache issue
        print(f"  [setup] Classification types created (POST 200/409) but "
              f"not queryable — staging cross-pod type cache issue")
    else:
        print(f"  [setup] Classification typedef creation failed "
              f"(POST returned errors)")

    # Step 3: Fall back to existing classification types
    print(f"  [setup] Discovering existing classification types...")
    existing = _find_existing_classification_types(client, count=count)
    if len(existing) >= count:
        print(f"  [setup] Using {count} existing types: {existing[:count]}")
        return existing[:count], False, True

    print(f"  [setup] Only found {len(existing)} existing classification "
          f"types, need {count}")
    return list(names), created, False


def _find_existing_classification_types(client, count=3):
    """Find existing classification type names that can be applied to DataSet.

    Uses /types/typedefs/headers to get candidates, then GET the full
    classificationdef to check:
      1. Type is queryable (GET returns 200)
      2. Type has no entityTypes restriction, or includes DataSet/Asset

    Types with entityTypes restrictions (e.g. only Table, Column) will
    silently fail when applied to DataSet — POST returns 200 but the
    classification never appears on the entity.
    """
    resp = client.get("/types/typedefs/headers")
    if resp.status_code != 200:
        print(f"  [setup] GET /types/typedefs/headers returned {resp.status_code}")
        return []
    body = resp.json()
    candidates = []
    if isinstance(body, list):
        for td in body:
            cat = td.get("category", "")
            if cat.upper() == "CLASSIFICATION":
                name = td.get("name", "")
                if name and not name.startswith("__"):
                    candidates.append(name)
    elif isinstance(body, dict):
        # Some Atlas versions nest under category keys
        for td in body.get("classificationDefs", []):
            name = td.get("name", "")
            if name and not name.startswith("__"):
                candidates.append(name)

    print(f"  [setup] Found {len(candidates)} classification type candidates "
          f"in headers: {candidates[:10]}")

    # Verify candidates are queryable AND applicable to DataSet
    results = []
    checked = 0
    for name in candidates:
        check = client.get(f"/types/classificationdef/name/{name}")
        if check.status_code != 200:
            continue
        checked += 1
        typedef_body = check.json()
        entity_types = typedef_body.get("entityTypes", []) or []
        # Accept if no restriction (empty list) or if DataSet/Asset is allowed
        if entity_types:
            allowed = {t.lower() for t in entity_types}
            if not (allowed & {"dataset", "asset", "referenceable"}):
                if checked <= 5:
                    print(f"  [setup] Skipping '{name}': restricted to "
                          f"entityTypes={entity_types}")
                continue
        results.append(name)
        if len(results) >= count:
            break
        # Don't spend too long checking candidates
        if checked > 30:
            break
    if results:
        print(f"  [setup] Found {len(results)} unrestricted classification "
              f"types: {results}")
    return results


def ensure_entity_types(client, names, super_types=None):
    """Create entity types, fall back to existing types if not queryable.

    Same pattern as ``ensure_classification_types()`` but for entity typedefs.
    On staging/preprod, newly created entity types may not propagate across
    pods — POST returns 200/409, but GET returns 404 and entity creation
    with that type returns 404 ("unknown type").

    Args:
        client:      AtlasClient instance
        names:       list of entity type names to create
        super_types: super types for the entity def (default ["DataSet"])

    Returns:
        (usable_names: list[str], created_new: bool, ok: bool)
        - usable_names: type names usable for entity creation
        - created_new: True if new types were created (caller should cleanup)
        - ok: True if enough usable types available
    """
    from core.data_factory import build_entity_def

    count = len(names)
    defs = [build_entity_def(name=n, super_types=super_types) for n in names]

    # Step 1: Try to create new types.
    # Short max_wait (30s) because we have our own fallback to existing types.
    created, _resp = create_typedef_verified(
        client, {"entityDefs": defs}, max_wait=30,
    )

    if created:
        # Step 2: Check if first type is queryable (usable across pods)
        check = client.get(f"/types/entitydef/name/{names[0]}")
        if check.status_code == 200:
            return list(names), True, True

        # Created but not queryable — cross-pod type cache issue
        print(f"  [setup] Entity types created (POST 200/409) but "
              f"not queryable — staging cross-pod type cache issue")
    else:
        print(f"  [setup] Entity typedef creation failed "
              f"(POST returned errors)")

    # Step 3: Fall back to existing entity types that extend DataSet
    print(f"  [setup] Discovering existing entity types extending DataSet...")
    existing = _find_existing_entity_types(client, count=count)
    if len(existing) >= count:
        print(f"  [setup] Using {count} existing entity type(s): {existing[:count]}")
        return existing[:count], False, True

    print(f"  [setup] Only found {len(existing)} existing entity "
          f"types extending DataSet, need {count}")
    return list(names), created, False


def _find_existing_entity_types(client, count=1):
    """Find existing entity type names that extend DataSet.

    Uses /types/typedefs/headers to get candidates, then GET the full
    entitydef to check:
      1. Type is queryable (GET returns 200)
      2. Type has DataSet in its superTypes hierarchy
    """
    resp = client.get("/types/typedefs/headers")
    if resp.status_code != 200:
        print(f"  [setup] GET /types/typedefs/headers returned {resp.status_code}")
        return []
    body = resp.json()
    candidates = []
    # Known built-in types to skip (not useful as "custom" types for testing)
    skip_types = {
        "DataSet", "Process", "Infrastructure", "Referenceable", "Asset",
        "DataDomain", "DataProduct", "AtlasGlossary", "AtlasGlossaryTerm",
        "AtlasGlossaryCategory", "AuthPolicy", "Persona", "Purpose",
        "Stakeholder", "StakeholderTitle", "Collection", "Folder", "Query",
        "__internal", "__AtlasUserProfile", "__AtlasUserSavedSearch",
        "__ExportImportAuditEntry",
    }
    if isinstance(body, list):
        for td in body:
            cat = td.get("category", "")
            if cat.upper() == "ENTITY":
                name = td.get("name", "")
                if name and not name.startswith("__") and name not in skip_types:
                    candidates.append(name)
    elif isinstance(body, dict):
        for td in body.get("entityDefs", []):
            name = td.get("name", "")
            if name and not name.startswith("__") and name not in skip_types:
                candidates.append(name)

    print(f"  [setup] Found {len(candidates)} entity type candidates "
          f"in headers: {candidates[:10]}")

    # Verify candidates are queryable AND extend DataSet
    results = []
    checked = 0
    for name in candidates:
        check = client.get(f"/types/entitydef/name/{name}")
        if check.status_code != 200:
            continue
        checked += 1
        typedef_body = check.json()
        super_types = typedef_body.get("superTypes", []) or []
        # Accept types that have DataSet (or Asset/Catalog) as a super type
        super_set = {s.lower() for s in super_types}
        if super_set & {"dataset", "asset", "catalog"}:
            results.append(name)
            if len(results) >= count:
                break
        elif checked <= 5:
            print(f"  [setup] Skipping '{name}': superTypes={super_types}")
        # Don't spend too long checking candidates
        if checked > 30:
            break
    if results:
        print(f"  [setup] Found {len(results)} entity type(s) extending "
              f"DataSet: {results}")
    return results


def ensure_bm_types(client, display_name, attr_defs=None, min_str_attrs=1):
    """Create BM typedef, fall back to existing BM types if not usable.

    Uses the UI's 2-step creation pattern which is lighter per-step:
      Step 1: POST BM without attributes (skips attribute validation)
      Step 2: PUT to add attributes (server generates internal attr names)

    On staging/preprod, BM typedef creation via POST /types/typedefs
    consistently returns 500 (server error or gateway timeout).  When this
    happens, falls back to discovering existing BM types that ARE queryable.

    Caches the failure so subsequent calls skip straight to existing type
    discovery instead of wasting ~60s on retries each time.

    Args:
        client:         AtlasClient instance
        display_name:   desired display name for the BM typedef
        attr_defs:      optional custom attributeDefs list (for multi-attr BM)
        min_str_attrs:  minimum number of usable string attrs required

    Returns:
        (bm_info: dict|None, created_new: bool, ok: bool)
        bm_info = {
            "internal_name": str,        # server-generated 22-char name
            "display_name": str,         # human-readable name
            "attr_map": {display: internal},
            "first_attr_display": str,   # display name of first usable attr
        }
        created_new: True if a new type was created (caller should cleanup)
        ok: True if a usable BM type is available
    """
    global _bm_creation_broken
    from core.data_factory import build_business_metadata_def

    BM_TIMEOUT = 120  # seconds — BM operations are slow on preprod

    # Fast path: if BM creation already failed on this server, skip straight
    # to existing type discovery (avoids ~120s of wasted retries per call)
    if _bm_creation_broken:
        print(f"  [bm-setup] BM creation known to fail on this server — "
              f"skipping to existing type discovery...")
        bm_info = _find_existing_bm_types(client, min_str_attrs=min_str_attrs)
        if bm_info:
            print(f"  [bm-setup] Using existing BM type: "
                  f"display={bm_info['display_name']}, "
                  f"internal={bm_info['internal_name']}, "
                  f"attrs={list(bm_info['attr_map'].keys())}")
            return bm_info, False, True
        print(f"  [bm-setup] No existing BM type found with {min_str_attrs}+ "
              f"string attributes applicable to DataSet/Asset")
        return None, False, False

    # Build the attribute defs we'll need for Step 2
    if attr_defs:
        full_bm_def = build_business_metadata_def(display_name=display_name)
        full_bm_def["attributeDefs"] = attr_defs
        target_attr_defs = attr_defs
    else:
        full_bm_def = build_business_metadata_def(display_name=display_name)
        target_attr_defs = full_bm_def["attributeDefs"]

    # Track whether Step 1 got a server error (500) — if so, skip Step 3
    # (same server will return the same 500).
    step1_server_error = False

    # ── Step 1: POST BM without attributes (UI pattern — lighter) ──
    print(f"  [bm-setup] Step 1: POST BM without attributes "
          f"(displayName={display_name})...")
    empty_bm_def = build_business_metadata_def(
        display_name=display_name, include_attrs=False,
    )
    empty_payload = {"businessMetadataDefs": [empty_bm_def]}
    step1_ok, step1_resp = create_typedef_verified(
        client, empty_payload, timeout=BM_TIMEOUT, max_wait=30,
        skip_repost=True,
    )

    bm_guid = None
    internal_name = None

    if step1_ok:
        internal_name, _ = extract_bm_names_from_response(
            step1_resp, display_name,
        )
        # Extract guid from response
        bm_guid = _extract_bm_guid_from_response(step1_resp, display_name)

        if not internal_name or not bm_guid:
            # POST returned 409 or empty body — discover via headers
            print(f"  [bm-setup] POST succeeded but no body — "
                  f"discovering via headers...")
            discovered = _discover_bm_by_display_name(client, display_name)
            if discovered:
                internal_name = discovered["internal_name"]
                bm_guid = discovered["guid"]
                # If it already has attrs, we might not need Step 2
                if len(discovered.get("attr_map", {})) >= min_str_attrs:
                    first_attr = next(iter(discovered["attr_map"]))
                    bm_info = {
                        "internal_name": internal_name,
                        "display_name": discovered["display_name"],
                        "attr_map": discovered["attr_map"],
                        "first_attr_display": first_attr,
                    }
                    print(f"  [bm-setup] Discovered existing BM with attrs: "
                          f"internal={internal_name}")
                    return bm_info, False, True

        if internal_name and bm_guid:
            print(f"  [bm-setup] Step 1 OK: guid={bm_guid}, "
                  f"internal={internal_name}")
        elif internal_name:
            print(f"  [bm-setup] Step 1 OK: internal={internal_name} "
                  f"(no guid extracted)")
        else:
            print(f"  [bm-setup] Step 1 returned {step1_resp.status_code} "
                  f"but could not extract names")
    else:
        print(f"  [bm-setup] Step 1 failed "
              f"(POST returned {step1_resp.status_code})")
        if step1_resp.status_code in (500, 502, 503):
            step1_server_error = True
            _bm_creation_broken = True
        # Try discovering in case a previous run created it
        discovered = _discover_bm_by_display_name(client, display_name)
        if discovered:
            internal_name = discovered["internal_name"]
            bm_guid = discovered["guid"]
            if len(discovered.get("attr_map", {})) >= min_str_attrs:
                first_attr = next(iter(discovered["attr_map"]))
                bm_info = {
                    "internal_name": internal_name,
                    "display_name": discovered["display_name"],
                    "attr_map": discovered["attr_map"],
                    "first_attr_display": first_attr,
                }
                print(f"  [bm-setup] Found pre-existing BM with attrs: "
                      f"internal={internal_name}")
                return bm_info, False, True
            print(f"  [bm-setup] Found BM shell (no attrs yet): "
                  f"internal={internal_name}")

    # ── Step 2: PUT to add attributes ──
    if internal_name:
        print(f"  [bm-setup] Step 2: PUT to add {len(target_attr_defs)} "
              f"attribute(s)...")
        put_bm_def = {
            "name": internal_name,
            "displayName": display_name,
            "description": f"Test business metadata {display_name}",
            "options": {},
            "attributeDefs": target_attr_defs,
        }
        if bm_guid:
            put_bm_def["guid"] = bm_guid
        put_payload = {"businessMetadataDefs": [put_bm_def]}
        # Let client handle retries on 500 (PUT is idempotent for typedefs)
        put_resp = client.put(
            "/types/typedefs", json_data=put_payload,
            timeout=BM_TIMEOUT,
        )
        if put_resp.status_code in (200, 204):
            _, attr_map = extract_bm_names_from_response(
                put_resp, display_name,
            )
            if attr_map and len(attr_map) >= min_str_attrs:
                first_attr = next(iter(attr_map))
                bm_info = {
                    "internal_name": internal_name,
                    "display_name": display_name,
                    "attr_map": attr_map,
                    "first_attr_display": first_attr,
                }
                print(f"  [bm-setup] 2-step creation succeeded: "
                      f"internal={internal_name}, "
                      f"attrs={list(attr_map.keys())}")
                return bm_info, True, True
            # PUT 200 but can't extract attrs — try GET to read them back
            print(f"  [bm-setup] PUT returned {put_resp.status_code} but "
                  f"attr extraction failed — trying GET...")
            discovered = _discover_bm_by_display_name(client, display_name)
            if discovered and len(discovered.get("attr_map", {})) >= min_str_attrs:
                first_attr = next(iter(discovered["attr_map"]))
                bm_info = {
                    "internal_name": internal_name,
                    "display_name": discovered["display_name"],
                    "attr_map": discovered["attr_map"],
                    "first_attr_display": first_attr,
                }
                print(f"  [bm-setup] 2-step creation succeeded (verified via GET)")
                return bm_info, True, True
        else:
            print(f"  [bm-setup] Step 2 PUT returned {put_resp.status_code}")

    # ── Step 3: Fallback — single-shot POST with attributes ──
    # Skip if Step 1 already got a server error (same server = same 500)
    # Skip if we already have an internal_name (shell exists, just can't add
    # attrs — don't create a duplicate BM type with a different random name)
    if step1_server_error:
        print(f"  [bm-setup] Skipping Step 3 (Step 1 already returned "
              f"{step1_resp.status_code} — same server will fail again)")
    elif internal_name:
        print(f"  [bm-setup] Skipping Step 3 (shell '{internal_name}' exists, "
              f"PUT failed — avoiding duplicate BM type)")
    else:
        print(f"  [bm-setup] Step 3: Trying single-shot POST with attributes "
              f"(timeout={BM_TIMEOUT}s)...")
        full_payload = {"businessMetadataDefs": [full_bm_def]}
        created, resp = create_typedef_verified(
            client, full_payload, timeout=BM_TIMEOUT, max_wait=30,
            skip_repost=True,
        )
        if created:
            resp_internal, attr_map = extract_bm_names_from_response(
                resp, display_name,
            )
            if resp_internal and len(attr_map) >= min_str_attrs:
                first_attr = next(iter(attr_map))
                bm_info = {
                    "internal_name": resp_internal,
                    "display_name": display_name,
                    "attr_map": attr_map,
                    "first_attr_display": first_attr,
                }
                print(f"  [bm-setup] Single-shot creation succeeded: "
                      f"internal={resp_internal}, "
                      f"attrs={list(attr_map.keys())}")
                return bm_info, True, True
            if resp_internal:
                print(f"  [bm-setup] Single-shot created with "
                      f"{len(attr_map)} attrs, need {min_str_attrs}")
            else:
                print(f"  [bm-setup] Single-shot created "
                      f"(POST {resp.status_code}) but could not extract names")
        else:
            print(f"  [bm-setup] Single-shot POST also failed "
                  f"({resp.status_code})")
            if resp.status_code in (500, 502, 503):
                _bm_creation_broken = True

    # ── Step 4: Fallback — discover existing BM types ──
    print(f"  [bm-setup] Discovering existing BM types...")
    bm_info = _find_existing_bm_types(client, min_str_attrs=min_str_attrs)
    if bm_info:
        print(f"  [bm-setup] Using existing BM type: "
              f"display={bm_info['display_name']}, "
              f"internal={bm_info['internal_name']}, "
              f"attrs={list(bm_info['attr_map'].keys())}")
        return bm_info, False, True

    print(f"  [bm-setup] No existing BM type found with {min_str_attrs}+ "
          f"string attributes applicable to DataSet/Asset")
    return None, False, False


def _extract_bm_guid_from_response(resp, display_name):
    """Extract the guid of a BM typedef from a POST/PUT response."""
    if not resp or resp.status_code not in (200, 409):
        return None
    try:
        body = resp.json()
    except Exception:
        return None
    for bm_def in body.get("businessMetadataDefs", []):
        if bm_def.get("displayName") == display_name:
            return bm_def.get("guid")
    return None


def _discover_bm_by_display_name(client, display_name):
    """Find a BM type by displayName via headers + GET.

    Used when POST returns 500/409 but we need the guid and internal name
    for the PUT step.

    Returns:
        dict with keys: internal_name, display_name, guid, attr_map
        or None if not found.
    """
    resp = client.get("/types/typedefs/headers")
    if resp.status_code != 200:
        return None

    body = resp.json()
    candidates = []
    if isinstance(body, list):
        for td in body:
            if td.get("category", "").upper() == "BUSINESS_METADATA":
                name = td.get("name", "")
                if name:
                    candidates.append(name)
    elif isinstance(body, dict):
        for td in body.get("businessMetadataDefs", []):
            name = td.get("name", "")
            if name:
                candidates.append(name)

    # Check each candidate's displayName
    for name in candidates:
        check = client.get(f"/types/businessmetadatadef/name/{name}")
        if check.status_code != 200:
            continue
        typedef_body = check.json()
        if typedef_body.get("displayName") == display_name:
            guid = typedef_body.get("guid")
            attr_map = {}
            for attr_def in typedef_body.get("attributeDefs", []):
                a_display = attr_def.get("displayName")
                a_internal = attr_def.get("name")
                if a_display and a_internal:
                    attr_map[a_display] = a_internal
            return {
                "internal_name": name,
                "display_name": display_name,
                "guid": guid,
                "attr_map": attr_map,
            }
    return None


def _find_existing_bm_types(client, min_str_attrs=1):
    """Find an existing BM type with string attrs applicable to DataSet/Asset.

    Uses /types/typedefs/headers to get BM candidates, then GET the full
    businessmetadatadef to check:
      1. Type is queryable (GET returns 200)
      2. Has at least ``min_str_attrs`` string attributes
      3. Attributes have applicableEntityTypes that include DataSet/Asset
         (or no restriction)

    Returns:
        bm_info dict or None.
    """
    resp = client.get("/types/typedefs/headers")
    if resp.status_code != 200:
        print(f"  [bm-setup] GET /types/typedefs/headers returned "
              f"{resp.status_code}")
        return None

    body = resp.json()
    candidates = []

    if isinstance(body, list):
        for td in body:
            cat = td.get("category", "")
            if cat.upper() == "BUSINESS_METADATA":
                name = td.get("name", "")
                if name and not name.startswith("__"):
                    candidates.append(name)
    elif isinstance(body, dict):
        for td in body.get("businessMetadataDefs", []):
            name = td.get("name", "")
            if name and not name.startswith("__"):
                candidates.append(name)

    print(f"  [bm-setup] Found {len(candidates)} BM type candidates "
          f"in headers: {candidates[:10]}")

    checked = 0
    for name in candidates:
        check = client.get(f"/types/businessmetadatadef/name/{name}")
        if check.status_code != 200:
            continue
        checked += 1

        typedef_body = check.json()
        bm_display = typedef_body.get("displayName", name)
        attr_defs_list = typedef_body.get("attributeDefs", [])

        # Find string attributes applicable to DataSet/Asset
        usable_attrs = {}
        for attr_def in attr_defs_list:
            attr_type = attr_def.get("typeName", "")
            attr_display = attr_def.get("displayName", "")
            attr_internal = attr_def.get("name", "")
            if not attr_display or not attr_internal:
                continue
            if attr_type != "string":
                continue

            # Check applicableEntityTypes
            options = attr_def.get("options", {}) or {}
            applicable_raw = options.get("applicableEntityTypes", "")
            if applicable_raw:
                try:
                    entity_types = json.loads(applicable_raw)
                    allowed = {t.lower() for t in entity_types}
                    if not (allowed & {"dataset", "asset", "referenceable"}):
                        continue
                except (json.JSONDecodeError, TypeError):
                    pass  # If can't parse, assume usable

            usable_attrs[attr_display] = attr_internal

        if len(usable_attrs) >= min_str_attrs:
            first_attr = next(iter(usable_attrs))
            return {
                "internal_name": name,
                "display_name": bm_display,
                "attr_map": usable_attrs,
                "first_attr_display": first_attr,
            }

        if checked <= 5:
            print(f"  [bm-setup] Skipping '{bm_display}' ({name}): "
                  f"only {len(usable_attrs)} usable string attrs, "
                  f"need {min_str_attrs}")
        if checked > 30:
            break

    return None


def _poll_types_queryable(client, verify_targets, max_wait, interval, label=""):
    """Poll GET until all types in verify_targets are queryable.

    Only used after 500 errors to check if types were silently created.
    Returns True if all types found within max_wait seconds.
    """
    prefix = f"  [{label}] " if label else "  "
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(interval)
        elapsed += interval
        all_found = True
        for path_segment, name in verify_targets:
            check = client.get(f"/types/{path_segment}/name/{name}")
            if check.status_code != 200:
                all_found = False
                break
        if all_found:
            print(f"{prefix}All {len(verify_targets)} type(s) queryable "
                  f"after {elapsed}s")
            return True
        names = [n for _, n in verify_targets]
        print(f"{prefix}Waiting for type cache ({elapsed}s/{max_wait}s): "
              f"{names}")
    return False

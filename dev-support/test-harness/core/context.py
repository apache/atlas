"""Shared test state and cleanup registry."""

import time
import threading
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional


def _purge_entity_with_retry(client, guid, max_attempts=3):
    """Hard-delete (purge) an entity with retry on timeout.

    DELETE with deleteType=PURGE is idempotent, so retrying on timeout is safe.
    Uses escalating timeouts: 60s, 90s, 120s.
    """
    for attempt in range(max_attempts):
        timeout = 60 + (30 * attempt)
        resp = client.delete(
            f"/entity/guid/{guid}",
            params={"deleteType": "PURGE"},
            timeout=timeout,
            retries=0,  # we handle retries here
        )
        if resp.status_code == 408:
            # Timeout — server likely still processing, retry
            if attempt < max_attempts - 1:
                wait = 10 * (attempt + 1)
                print(f"  [cleanup] Purge {guid} timed out ({timeout}s), "
                      f"waiting {wait}s then retrying...")
                time.sleep(wait)
                continue
            # Final attempt also timed out — return the 408
            print(f"  [cleanup] Purge {guid} timed out after {max_attempts} "
                  f"attempts (server may still complete it)")
        return resp
    return resp


class TestContext:
    """Thread-safe key-value store for sharing data between tests.

    Also maintains a LIFO cleanup stack so children are deleted before parents.

    Entity cleanup uses hard delete (DELETE /entity/guid/{guid}?deleteType=PURGE)
    to fully remove entities from graph + ES, ensuring typedef deletion
    succeeds afterward (classification/BM types check ES for references).
    Use register_entity_cleanup(guid) for entities and register_cleanup(fn)
    for everything else (glossaries, typedefs, etc.).
    """

    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._entities: OrderedDict = OrderedDict()   # name -> {guid, type}
        self._cleanup_stack: List[Callable] = []
        self._entity_cleanup_guids: List[str] = []     # GUIDs for batch delete
        self._lock = threading.Lock()

    # ---- generic k/v ----

    def set(self, key, value):
        with self._lock:
            self._data[key] = value

    def get(self, key, default=None):
        with self._lock:
            return self._data.get(key, default)

    # ---- entity registry ----

    def register_entity(self, name, guid, type_name=None, qualifiedName=None):
        with self._lock:
            self._entities[name] = {
                "guid": guid, "typeName": type_name,
                "qualifiedName": qualifiedName,
            }

    def get_entity_guid(self, name) -> Optional[str]:
        with self._lock:
            entry = self._entities.get(name)
            return entry["guid"] if entry else None

    def get_entity_qn(self, name) -> Optional[str]:
        with self._lock:
            entry = self._entities.get(name)
            return entry.get("qualifiedName") if entry else None

    def get_entity(self, name) -> Optional[Dict]:
        with self._lock:
            return self._entities.get(name)

    def list_entities(self) -> Dict[str, Dict]:
        with self._lock:
            return dict(self._entities)

    # ---- cleanup ----

    def register_cleanup(self, fn: Callable):
        """Register a no-arg callable to run during cleanup (LIFO order).

        Use this for glossary deletes and other non-entity cleanup.
        For entity deletes, prefer register_entity_cleanup(guid).
        For typedef deletes, prefer register_typedef_cleanup(client, name).
        """
        with self._lock:
            self._cleanup_stack.append(fn)

    def register_typedef_cleanup(self, client, name: str):
        """Register a typedef for deletion during cleanup (fast-fail).

        Uses retries=0 and timeout=60 because typedef DELETE returning 500
        means the server rejected it (e.g. type has references in ES) —
        retrying the same request won't help, it just wastes minutes.
        """
        with self._lock:
            self._cleanup_stack.append(
                lambda n=name, c=client: c.delete(
                    f"/types/typedef/name/{n}", timeout=120, retries=0,
                )
            )

    def register_entity_cleanup(self, guid: str):
        """Register an entity GUID for hard deletion during cleanup.

        Entities are hard-deleted (purged) individually via
        DELETE /entity/guid/{guid}?deleteType=PURGE to fully remove them from
        graph + ES, which is required before typedef deletion can succeed.
        """
        with self._lock:
            self._entity_cleanup_guids.append(guid)

    def run_cleanup(self, client=None, es_settle_wait=30):
        """Execute cleanup: hard-delete entities, wait for ES, then run callbacks.

        Order:
          1. Hard-delete (purge) all registered entities individually.
          2. Wait es_settle_wait seconds for ES to reflect deletions.
          3. Run cleanup callbacks (typedef deletes, glossary deletes, etc.).

        Hard delete is required because:
          - Soft delete leaves entities in ES with classification/BM references.
          - Typedef deletion checks ES for references (classificationHasReferences).
          - If ES still has entities with the classification, typedef delete fails
            with TYPE_HAS_REFERENCES (409) or CLASSIFICATION_TYPE_HAS_REFERENCES.

        Args:
            client: AtlasClient instance for entity deletes.
                    If None, entity cleanups are skipped (only callbacks run).
            es_settle_wait: Seconds to wait after entity deletion before
                            running typedef/callback cleanup (default 30).
        """
        errors = []

        with self._lock:
            entity_guids = list(reversed(self._entity_cleanup_guids))
            self._entity_cleanup_guids.clear()
            stack = list(reversed(self._cleanup_stack))
            self._cleanup_stack.clear()

        # 1. Hard-delete (purge) entities individually
        #    DELETE with purge is idempotent, so we retry on timeout (408).
        #    Use a longer timeout (60s) since purge can be slow on staging.
        if entity_guids and client:
            # Deduplicate while preserving order
            seen = set()
            unique_guids = []
            for g in entity_guids:
                if g not in seen:
                    seen.add(g)
                    unique_guids.append(g)

            total = len(unique_guids)
            purged = 0
            failed = 0
            for guid in unique_guids:
                try:
                    resp = _purge_entity_with_retry(client, guid)
                    if resp.status_code in (200, 204):
                        purged += 1
                    elif resp.status_code == 404:
                        # Already deleted — count as success
                        purged += 1
                    else:
                        failed += 1
                        errors.append(
                            Exception(f"Hard delete {guid} returned "
                                      f"{resp.status_code}")
                        )
                except Exception as e:
                    failed += 1
                    errors.append(e)
            if total > 0:
                print(f"  Hard-deleted (purged) {purged}/{total} entities"
                      + (f" ({failed} failed)" if failed else ""))

        # 2. Wait for ES to reflect entity deletions before typedef cleanup
        has_callbacks = len(stack) > 0
        if entity_guids and client and has_callbacks:
            print(f"  Waiting {es_settle_wait}s for ES to settle before "
                  f"typedef/callback cleanup...")
            time.sleep(es_settle_wait)

        # 3. Run remaining cleanup callbacks (glossary, typedef, etc.)
        for fn in stack:
            try:
                fn()
            except Exception as e:
                errors.append(e)

        return errors

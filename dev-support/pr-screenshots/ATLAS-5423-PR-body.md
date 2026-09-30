## What changes were proposed in this pull request?

When hook notifications reference an **unknown or invalid entity `typeName`**, processing used to fail inside the entity store (often after opening a graph transaction), then enter the consumer **retry loop**. That produced noisy rollback logs, wasted retries, and could delay later valid messages on the same consumer thread.

This change **validates all entity type names up front** in `SerialEntityProcessor` (used by `NotificationHookConsumer` for hook message handling), **before** the retry loop and before any entity-store work:

- Collects type names from the notification payload (root entities, referred entities, partial-update entity ids, delete ids) for **V1 and V2** hook message types.
- Resolves each name via `typeRegistry.getEntityTypeByName`; missing types throw `AtlasBaseException` with `AtlasErrorCode.UNKNOWN_TYPENAME`.
- On failure: log `Unrecoverable failure, skipping retries`, record the message in **`failed.log`** (`DROPPED_NOTIFICATION`), **commit the Kafka offset**, and return (no retries, no graph transaction for that message).

Related: [ATLAS-5423](https://issues.apache.org/jira/browse/ATLAS-5423) / CDPD-123799.

## How was this patch tested?

### Unit tests

`NotificationHookConsumerTest` on **Java 8** (Zulu 1.8.0_504):

```bash
export JAVA_HOME=<jdk8>
mvn -pl webapp test -Dtest=NotificationHookConsumerTest -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false
```

**96 tests, 0 failures**, including four new cases:

| Test | Behavior verified |
|------|-------------------|
| `testUnknownTypeNameInPartialUpdateIsNotRetried` | `ENTITY_PARTIAL_UPDATE_V2` with unknown type → no `updateEntity`, offset committed |
| `testUnknownReferredEntityTypeNameInCreateIsNotRetried` | `ENTITY_CREATE_V2` with unknown referred entity → no `createOrUpdate`, offset committed |
| `testUnknownTypeNameInV1PartialUpdateIsNotRetried` | V1 `ENTITY_PARTIAL_UPDATE` with unknown type → no conversion/store, offset committed |
| `testUnknownTypeNameInDeleteV2IsNotRetried` | `ENTITY_DELETE_V2` with unknown type in list → no delete, offset committed |

### Build

Full server build (Java 8, `-Pdist,embedded-hbase-solr`, tests skipped for packaging):

```bash
mvn clean install -DskipTests -Pdist,embedded-hbase-solr -Drat.skip=true
```

`atlas-webapp` **checkstyle** passed on this module.

### End-to-end (local embedded stack)

Built server tarball, started embedded HBase/Solr/Kafka on a non-conflicting port (`21001`), published raw `ATLAS_HOOK` messages, and verified:

- Four invalid notifications → **4** “Unrecoverable failure, skipping retries” lines, **no** consumer retry logs, **no** graph rollback on the hook consumer thread for those messages.
- **4** entries in `failed.log`.
- A **valid** `ENTITY_CREATE_V2` sent after the invalid batch is processed normally.

Script (optional):

```bash
ATLAS_URL=http://localhost:21001 \
ATLAS_HOME=/path/to/apache-atlas-3.0.0-SNAPSHOT \
PYTHON=/path/to/venv-with-kafka-python/bin/python \
./dev-support/scripts/e2e-unknown-typename-notification.sh
```

**Note:** Open-source Atlas ships a `trino_table` typedef in `models/6000-Trino`; the e2e script uses a **generated unregistered type name** so “unknown typename” is reproducible without omitting that model. In CDP, an unregistered type such as `trino_table` matches the production failure mode.

No UI changes.

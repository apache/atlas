# Atlas Semantic Search

Semantic search finds metadata entities by **meaning** rather than exact keyword match.
Embeddings are stored on the existing JanusGraph **vertex index** in OpenSearch
(`{index-name}_vertex_index`, default `janusgraph_vertex_index`).

Indexing is **not** performed inside the Atlas webapp. Run the Semantic Indexer as a
separate process (`services/atlas-semantic-indexer`).

## Architecture

| Component | Role |
|-----------|------|
| **Atlas server** | Read-only `POST /v2/search/semantic` and `GET /v2/search/similar`; auth/scrub via discovery |
| **Semantic Indexer** | Kafka consumer on `ATLAS_ENTITIES`; enriches GUIDs from graph; partial-updates OpenSearch |
| **atlas-semantic-repair** | Offline backfill of missing embeddings, or all of them with `--all` (`tools/atlas-semantic-repair`) |

### Index fields (Atlas-owned)

On the JanusGraph vertex OpenSearch index:

- `atlas_semantic_text` — pipeline input (removed after embed)
- `atlas_semantic_embedding` — `knn_vector`
- Ingest pipeline: `atlas-semantic-ingest`

### Text we embed

For each entity, `SemanticTextBuilder` reads the entity vertex and walks a few edges in JanusGraph, then
joins everything into one text (max 8,000 characters) that the ML model turns into the embedding.

Example graph around the column `customer.email` (boxes are vertices, arrows are edges and their labels):

```
┌──────────────────────────┐                                            ┌──────────────────────────┐
│ AtlasGlossaryTerm "PII"  │   synonyms, related terms, antonyms, ...   │ AtlasGlossaryTerm        │
│ name, abbreviation,      │ ────────────────────────────────────────── │ "Personal data"          │
│ description              │                not followed                │                          │
└──────────────────────────┘                                            └──────────────────────────┘
            │
            │ r:AtlasGlossarySemanticAssignment  (3) term to entity
            ▼
┌────────────────────────────────────────┐                          ┌────────────────────────────┐
│ hive_column "customer.email"  (entity) │       classifiedAs       │ classification "Sensitive" │
│ __typeName, __classificationsText,     │    ──────────────────    │ {level: "high"}            │
│ __labels, __customAttributes,          │      not followed:       │ own or propagated          │
│ hive_column.name, .comment, ...        │ in __classificationsText └────────────────────────────┘
└────────────────────────────────────────┘
```

Order of the text, starting at the entity vertex (the numbers match the diagram; 1 is the entity box itself):

1. Type name, then the entity's own string attributes (`hive_column.name`, `.comment`, ...). Internal `__*`
   properties are skipped. References to other entities are edges, not properties, so a column's text doesn't
   include its table.
2. `__classificationsText`, `__labels` and `__customAttributes` (entity vertex properties). Atlas keeps the type
   names and attribute values of all classifications, own and propagated, in `__classificationsText`, so the
   `classifiedAs` edges are not followed.
3. For each `r:AtlasGlossarySemanticAssignment` edge (term to entity, read from the entity side): the term's
   name, abbreviation and description. At most 100 assigned terms are read per entity (code constant
   `MAX_GLOSSARY_TERMS` in `SemanticTextBuilder`), fewer once the text is full; which 100 is up to JanusGraph.

Terms linked to an assigned term (synonyms, related terms, antonyms, preferred terms, replacements,
translations, ...) are **not** followed. Their text would be one level further from the entity, so a change to
them would need a second-level fan-out; antonyms would also add the opposite meaning. The deepest read is one
edge from the entity.

Glossary objects are not embedded and not followed. A glossary (`AtlasGlossary`, e.g. "Retail Glossary"), its
categories (`AtlasGlossaryCategory`) and its terms (`AtlasGlossaryTerm`) are entity vertices of their own, linked
by `AtlasGlossaryTermAnchor` (glossary to term), `AtlasGlossaryCategoryAnchor` (glossary to category),
`AtlasGlossaryTermCategorization` (category to term) and `AtlasGlossaryCategoryHierarchyLink` (category to
sub-category). None of these vertices gets an embedding, so they never appear in semantic results, and the
text builder doesn't read glossary or category names: a term contributes only its own name, abbreviation and
description to the entities it is assigned to. Changes to glossaries and categories are ignored.

The entity's own attributes come first so they are never cut by the 8,000-character limit. Once the text is
full, the builder stops reading more terms.

Which notifications re-embed what:

| Kafka notification | Re-embeds |
|---|---|
| `ENTITY_CREATE` / `ENTITY_UPDATE` of an entity | that entity |
| `CLASSIFICATION_ADD` / `_UPDATE` / `_DELETE` | that entity (Atlas sends one message per entity, also for propagated classifications) |
| `RELATIONSHIP_CREATE` / `_DELETE` of `AtlasGlossarySemanticAssignment` (term assigned to / removed from an entity) | the entity end only; needs `atlas.notification.relationships.enabled=true` (below) |
| `RELATIONSHIP_UPDATE` | nothing (it changes the relationship's own attributes, which are not embedded) |
| other `RELATIONSHIP_*` | nothing (relationships are edges the text doesn't include) |
| `ENTITY_UPDATE` of an `AtlasGlossaryTerm` (name, abbreviation, description) | the term's assigned entities, capped (below) |

`CLASSIFICATION_*` are needed on their own: adding, updating or removing a classification (directly or by
propagation) rewrites `__classificationsText` but sends no `ENTITY_UPDATE`.

#### `atlas.notification.relationships.enabled`

Atlas sends `RELATIONSHIP_*` notifications only when this is `true` on the **Atlas server** (default `false`).
Without it, assigning a term to an entity, or removing it, through the glossary API or UI
(`POST`/`DELETE /v2/glossary/terms/{guid}/assignedEntities`) sends **no** Kafka message at all, so the
entity keeps its old embedding until its next update or a repair run. Assigning terms inside an entity
create/update (the `meanings` relationship attribute) sends `ENTITY_UPDATE` and works either way.

Enable it (Atlas server `atlas-application.properties`, restart required) if term assignments should reach the
embeddings right away:

    atlas.notification.relationships.enabled=true

Impact: Atlas then publishes a `RELATIONSHIP_CREATE` for **every** relationship it creates, including those
created by hooks through entity relationship attributes (table to columns, process inputs/outputs, ...),
plus `RELATIONSHIP_UPDATE` / `_DELETE` from the relationship and glossary APIs. On a busy catalog this is
a large increase of messages on `ATLAS_ENTITIES`, read by every consumer of the topic (the indexer drops all of
them except term assignments, after deserializing them). Size the topic and check other consumers before
enabling it.

#### Overlaps and gaps

- An entity update that also adds a classification can send both `ENTITY_UPDATE` and `CLASSIFICATION_ADD`.
  In the same poll the guid is embedded once; across polls it is embedded twice (harmless).
- Assigning a term through an entity update makes Atlas record an update on the term as well, so it sends
  `ENTITY_UPDATE` for the entity **and** for the term. The term's message re-embeds up to
  `max.term.entities` of its other entities although their text didn't change. The message doesn't say which
  attributes changed, so the indexer can't tell this apart from a real term edit.
- Labels added or removed through the labels API (`/v2/entity/guid/{guid}/labels`, also used by the UI)
  send no Kafka message: Atlas updates `__labels` but its V2 listener ignores label events. The embedding
  keeps the old labels until the entity's next update or a repair run. Labels set inside an entity
  create/update arrive with `ENTITY_UPDATE`.

#### Glossary-term fan-out

Why a glossary-term update reaches many entities: the term's text is part of every assigned entity's text, but
Atlas sends one notification for the term only. The indexer follows the term's
`r:AtlasGlossarySemanticAssignment` edges (one level) and re-embeds each assigned entity; deleted entities are
skipped at indexing time.

This fan-out is capped by `atlas.semantic.indexer.max.term.entities` (default 100; `0` disables it). Each entity
costs one ML call, and a poll that runs longer than `max.poll.interval.ms` is redelivered by Kafka, so an
unbounded fan-out could restart forever. The cap is per term: a poll of 25 messages for 25 different terms can
still reach 25 × 100 = 2,500 entities (a few minutes with a healthy model). Above the cap, only the first 100
entities are re-embedded and the indexer logs a WARN with the term guid. Run `atlas_semantic_repair.sh --all`
to refresh the others (or all of them when the fan-out is disabled). Repair takes only the entity guids from
OpenSearch; for each one it builds the text from the graph like the indexer does, reading the entity's current
term assignments and the terms' current text, so no fan-out (and no cap) is involved. Repair is also the
catch-up for the gaps above.

### DELETE policy

`ENTITY_DELETE` Kafka events are **skipped** (no-op). Stale embeddings may remain until
JanusGraph removes the document or you run repair.

## Semantic Indexer service

Standalone Kafka consumer (`org.apache.atlas.semantic.indexer`) that listens on
`ATLAS_ENTITIES`, builds embeddable text from JanusGraph, and partial-updates the
JanusGraph OpenSearch vertex index via the `atlas-semantic-ingest` ML pipeline.

Run (non-Docker):

    atlas_semantic_indexer.sh

Health endpoint (enabled by default, no auth):

    GET http://localhost:8089/health

Docker runs the indexer in a slim JRE image (`Dockerfile.atlas-semantic-indexer`).

## Configuration

Overlay file merged at container start: `config/atlas-semantic-docker.properties`

```properties
atlas.semantic.enabled=true
atlas.semantic.model.id=<from init-opensearch-semantic-local.sh>
atlas.semantic.embedding.dimension=384
atlas.semantic.indexer.kafka.group.id=atlas_semantic_indexer
atlas.semantic.indexer.batch.size=25
atlas.semantic.indexer.kafka.poll.timeout.ms=5000
# messages per poll (Atlas default is 1); guids are deduplicated across the messages of one poll
atlas.semantic.indexer.kafka.max.poll.records=25
# max entities re-embedded when one glossary term changes (see "Text we embed")
atlas.semantic.indexer.max.term.entities=100
# max time to index one poll, longer and Kafka redelivers it; default 30 min
atlas.semantic.indexer.kafka.max.poll.interval.ms=1800000
atlas.semantic.indexer.health.enabled=true
atlas.semantic.indexer.health.port=8089
atlas.semantic.indexer.health.path=/health
```

There is no separate OpenSearch connection config: semantic search, the indexer and the repair
tool read the same JanusGraph index backend settings as Atlas
(`config/atlas/postgres/opensearch/atlas-application.properties`):

| Setting | Keys |
|---|---|
| Hosts | `atlas.graph.index.search.hostname` (comma-separated, `host` or `host:port`), `atlas.graph.index.search.port` |
| TLS | `atlas.graph.index.search.opensearch.ssl.enabled`, `.ssl.truststore.location/password`, `.ssl.keystore.location/storepassword/keypassword`, `.ssl.disable-hostname-verification`, `.ssl.allow-self-signed-certificates` |
| Basic auth | `atlas.graph.index.search.opensearch.http.auth.type=BASIC`, `.http.auth.basic.username/password` |
| Timeouts | `atlas.graph.index.search.opensearch.connect-timeout`, `.socket-timeout` |

Hard limits (code constants, not configurable): `topK` ≤ 100, semantic query ≤ 30,000 characters.
REST also applies `atlas.query.param.max.length` (default 4096) to the query first.

## Operations

### `index.knn` must be set when the vertex index is created

`index.knn` is a **final** OpenSearch setting: it can't be updated on an existing index, open or closed
(`Can't update non dynamic settings` / `final ... setting [index.knn], not updateable`). It can only be set
when an index is created (including as a clone). Without it, the `knn_vector` mapping is rejected.
New installs: set this before Atlas starts for the first time:

```properties
atlas.graph.index.search.opensearch.create.ext.knn=true
```

Atlas (when `atlas.semantic.enabled=true`), the Semantic Indexer and the repair tool refuse to start without it.
They check it before opening the graph, so a missing property can't create a vertex index without knn.
The property only affects index creation; on an existing index, the knn check at indexer/repair startup still
applies.

**Existing Atlas with data** (vertex index created without knn): `index.knn` can't be updated, but it can
be set when **cloning** the index. `_clone` hard-links the segment files, so it takes seconds and almost no
extra disk, keeps every document, and needs no reindex. OpenSearch has no rename, so clone twice: once to
a knn copy, then back to the original name.

Requirements (from the OpenSearch clone API): cluster health **green** (a single-node cluster with
replicas stays yellow: set `index.number_of_replicas` to 0 on the vertex index first), the
`indices:admin/resize` permission when the Security plugin is on. Each shard is cloned on the node that
holds it, so this works on multi-node clusters. The clone call returns **before** the copy is finished:
always wait for the target health before using it or deleting the source.

`bin/atlas_vertex_index_enable_knn.sh` runs this procedure with checks:
- it refuses to run while Atlas answers at `ATLAS_URL`;
- without `--confirm` it only checks: the index exists, knn is off, health is green, and no `_knn` copy is left;
- it verifies the copy (green, `index.knn`, same document count) before deleting the original;
- it keeps `${IDX}_knn` as a write-blocked backup.

```bash
OPENSEARCH_URL=https://os:9200 OPENSEARCH_USER=admin OPENSEARCH_PASSWORD=... bin/atlas_vertex_index_enable_knn.sh            # checks
OPENSEARCH_URL=https://os:9200 OPENSEARCH_USER=admin OPENSEARCH_PASSWORD=... bin/atlas_vertex_index_enable_knn.sh --confirm  # migrate
```

Other variables: `VERTEX_INDEX` (default `janusgraph_vertex_index`), `CURL_OPTS` (e.g. `--cacert ca.pem`),
`WAIT_TIMEOUT` (default `10m`). The manual steps it performs:

1. Stop Atlas and the Semantic Indexer (the index must not change while it is cloned).
2. Add `atlas.graph.index.search.opensearch.create.ext.knn=true` to `atlas-application.properties`
   (for any future index creation).
3. Clone to a knn copy, wait for it, and check the document counts match. `_clone` doesn't copy
   `number_of_replicas`: pass the source's value, or the copy gets the cluster default and may never be green.
   ```bash
   H='Content-Type: application/json'; IDX=janusgraph_vertex_index
   curl -X PUT  "$OS/$IDX/_settings" -H "$H" -d '{"index.blocks.write":true}'
   curl -X POST "$OS/$IDX/_clone/${IDX}_knn" -H "$H" -d '{"settings":{"index.knn":true,"index.number_of_replicas":0,"index.blocks.write":null}}'
   curl "$OS/_cluster/health/${IDX}_knn?wait_for_status=green&timeout=10m"
   curl "$OS/$IDX/_count"; curl "$OS/${IDX}_knn/_count"
   ```
4. Replace the original with a clone of the knn copy (`index.knn` is kept), and wait again:
   ```bash
   curl -X DELETE "$OS/$IDX"
   curl -X PUT  "$OS/${IDX}_knn/_settings" -H "$H" -d '{"index.blocks.write":true}'
   curl -X POST "$OS/${IDX}_knn/_clone/$IDX" -H "$H" -d '{"settings":{"index.number_of_replicas":0,"index.blocks.write":null}}'
   curl "$OS/_cluster/health/$IDX?wait_for_status=green&timeout=10m"
   curl "$OS/$IDX/_settings/index.knn"; curl "$OS/$IDX/_count"
   ```
5. Start Atlas, run `atlas_semantic_repair.sh` (default mode: entities without an embedding), then start
   the Semantic Indexer. Kafka kept the events published meanwhile; the indexer resumes from its last offset.
6. Once search is verified, delete the copy: `curl -X DELETE "$OS/${IDX}_knn"`.

If the cloned index is lost or inconsistent, fall back to a rebuild from HBase (the source of truth): delete
the vertex index, start Atlas (JanusGraph recreates it with knn), run the index repair tool in full mode
(`repair_index.py`, see `docs/src/documents/Tools/AtlasRepairIndex.md`), then semantic repair.

### Rollout

1. Deploy OpenSearch ML embedding model (`init-opensearch-semantic-local.sh` or remote variant).
2. Set model id and dimension in Atlas config.
3. Start **Semantic Indexer** (`atlas_semantic_indexer.sh` or Docker `atlas-semantic-indexer` container).
4. Run **repair** once for existing data (`atlas_semantic_repair.sh`).

Repair reads candidates from the vertex index in OpenSearch (no JanusGraph scan) and builds the text from the graph:

| Command | Re-embeds |
|---|---|
| `atlas_semantic_repair.sh` | active entities **without** an embedding (after reindex / first rollout) |
| `atlas_semantic_repair.sh --all` | **every** active entity (after a model change) |
| `atlas_semantic_repair.sh --guid <guid>` | one entity |

Options: `--batch-size N` (default 50), `--dry-run` (count only, no OpenSearch changes). Exit code is 1 if any
entity failed. Entities with no vertex-index document are not visible to repair: run Atlas index repair first.

OpenSearch only provides the list of entity guids: a vertex-index document holds the vertex's indexed
properties but no edges, so no term assignments. Each entity's text, including its terms, is read from
JanusGraph with the same `SemanticTextBuilder` as the indexer:

```
OpenSearch scan ──► page of entity GUIDs (e.g. "guid-of-customer.email")
                         │
                         ▼  for each GUID
JanusGraph: load entity vertex "customer.email"
    ├─ read its own properties (name, comment, __classificationsText, __labels, ...)
    └─ follow incoming r:AtlasGlossarySemanticAssignment edges (max 100)
           └─ term vertex "PII": read name, abbreviation, description
                         │
                         ▼
text = "hive_column customer.email ... Sensitive ... PII ..."
                         │
                         ▼
OpenSearch: update that entity's document through the ingest pipeline ──► ML model ──► atlas_semantic_embedding
```

Every entity reads its current terms this way, so repair needs no glossary-term fan-out (and no cap).
5. Enable `atlas.semantic.enabled=true` on Atlas for REST search.

### After JanusGraph / OpenSearch index rebuild (reindex)

JanusGraph index **restore/reindex** (`OpenSearchIndex.restore()`, Atlas index repair, or
`REINDEX` management operations) **replaces each OpenSearch document in full** from graph index
entries. The semantic fields (`atlas_semantic_embedding`) are **not** part of JanusGraph's index
store — they are patched later by the Semantic Indexer — so **reindex wipes all embeddings**.

**Required after any vertex-index rebuild:**

1. Confirm Atlas and OpenSearch are healthy.
2. Run a full semantic backfill:
   ```bash
   docker exec -u atlas atlas /opt/atlas/bin/atlas_semantic_repair.sh
   ```
   Or from the host: `atlas_semantic_repair.sh` (same classpath as Atlas).
3. Verify search: `dev-support/atlas-docker/seed/udf-retail/test-udf-retail-semantic.sh`

A reindex writes to OpenSearch directly from HBase; it does not go through the Atlas entity store, so it
publishes **no** Kafka events. The Semantic Indexer therefore never sees the rebuilt entities; it only
indexes entities that change afterwards. Repair is the supported backfill path.

## REST API

Search results are loaded from JanusGraph (full entity headers with auth scrubbing).

**Semantic search**

```http
POST /api/atlas/v2/search/semantic
Content-Type: application/json

{"query":"customer revenue table","topK":10,"typeName":"hive_table"}
```

**Similar entities**

```http
GET /api/atlas/v2/search/similar?guid=<entity-guid>&topK=10
```

---

## Local Docker guide

### Prerequisites

- Docker Desktop (enough disk for ~2 GB of images + dist tarballs)
- Maven 3.8+
- JDK 17

### 1. Build artifacts

```bash
# From repo root
mvn -pl repository,services/atlas-semantic-indexer,tools/atlas-semantic-repair,distro -am \
  package -DskipTests -DskipEnunciate=true

cp distro/target/apache-atlas-*-server.tar.gz dev-support/atlas-docker/dist/
cp services/atlas-semantic-indexer/target/apache-atlas-*-semantic-indexer.tar.gz dev-support/atlas-docker/dist/
```

### 2. Start the stack

One command (builds images, starts Postgres + OpenSearch + Atlas + Semantic Indexer, bootstraps ML model):

```bash
cd dev-support/atlas-docker
./scripts/run-atlas-semantic.sh
```

Or manually:

```bash
export ATLAS_BACKEND=postgres ATLAS_INDEX_BACKEND=opensearch COMPOSE_PROFILES=opensearch-index

docker compose -f docker-compose.atlas.yml -f docker-compose.atlas-postgres.yml \
  -f docker-compose.atlas-semantic.yml up -d --wait

OPENSEARCH_URL=http://localhost:9200 ./scripts/init-opensearch-semantic-local.sh
```

| Service | URL |
|---------|-----|
| Atlas UI | http://localhost:21000/n3/index.html |
| Atlas API | http://localhost:21000/api/atlas/v2 |
| OpenSearch | http://localhost:9200 |
| Indexer health | http://localhost:8089/health |

Credentials: `admin` / `atlasR0cks!`

### 3. Verify indexer health

The indexer exposes a lightweight HTTP health check (no auth required):

```bash
curl -sS http://localhost:8089/health | jq
```

Expected response (`HTTP 200`):

```json
{
  "status": "UP",
  "service": "atlas-semantic-indexer",
  "uptimeSeconds": 592
}
```

One-liner with status code:

```bash
curl -sS -w '\nHTTP %{http_code}\n' http://localhost:8089/health
```

### 4. (Optional) Apply seed data

UDF retail sample dataset with glossary terms, classifications, and business metadata:

```bash
./seed/udf-retail/apply-udf-retail-seed.sh
```

Wait for Kafka indexing, or backfill immediately:

```bash
docker exec -u atlas atlas /opt/atlas/bin/atlas_semantic_repair.sh
```

### 5. Test REST APIs

**Semantic search**

```bash
curl -u admin:atlasR0cks! -H 'Content-Type: application/json' \
  -d '{"query":"customer lifetime value","topK":5}' \
  http://localhost:21000/api/atlas/v2/search/semantic | jq
```

**Similar entities** (replace `<GUID>` with an entity guid from search results)

```bash
curl -u admin:atlasR0cks! \
  'http://localhost:21000/api/atlas/v2/search/similar?guid=<GUID>&topK=5' | jq
```

**Automated smoke tests** (requires seed data):

```bash
./seed/udf-retail/test-udf-retail-semantic.sh
```

Expect `queryType: "SEMANTIC"` and non-empty `entities` once embeddings are indexed.

### 6. Logs

```bash
# Indexer (INFO)
docker logs -f atlas-semantic-indexer 2>&1 | grep --line-buffered 'org.apache.atlas'

# Atlas semantic REST (DEBUG)
docker logs -f atlas 2>&1 | grep --line-buffered 'org.apache.atlas.semantic'
```

## OpenSearch bootstrap

`init-opensearch-semantic-local.sh` ensures a local MiniLM model and patches:

- checks `index.knn=true` on the vertex index (fails if missing: it can't be added later, see below)
- ingest pipeline `atlas-semantic-ingest`
- `atlas_semantic_embedding` knn_vector mapping

**Idempotent by default:** searches OpenSearch for a `DEPLOYED` model with the configured
`MODEL_NAME`, verifies with `GET /_plugins/_ml/models/{id}`, and reuses it. If none is
found, it registers a **new** model (OpenSearch assigns a new id each time — see below).
The bootstrap artifact is written for Atlas config reference only.

To deliberately register a **new** model:

```bash
./scripts/init-opensearch-semantic-local.sh --force
# or: ATLAS_SEMANTIC_FORCE_INIT=true ./scripts/run-atlas-semantic.sh
```

After a new model id is created, run `atlas_semantic_repair.sh --all` to re-embed existing entities (the default
mode only fills missing embeddings, so it would keep vectors from the old model).

For remote embedding models, use `init-opensearch-semantic-remote.sh` (see `config/connectors/README.md`).

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Empty semantic search results | Run repair or wait for indexer; check `curl localhost:9200/janusgraph_vertex_index/_count?q=exists:atlas_semantic_embedding` |
| Indexer restart loop | Check `docker logs atlas-semantic-indexer`; ensure slim image has `bin/atlas_semantic_indexer.sh` |
| Model id mismatch | Re-run `./scripts/init-opensearch-semantic-local.sh` (reuse) or `--force` if OpenSearch was reset; sync `atlas-semantic-docker.properties` |
| Clear embeddings only | `./seed/udf-retail/clear-opensearch-semantic-embeddings.sh` |
| Health endpoint unreachable | Recreate container after rebuild; confirm port `8089:8089` in `docker-compose.atlas-semantic.yml` |

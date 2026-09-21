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
| **atlas-semantic-repair** | Offline backfill for all existing vertices (`tools/atlas-semantic-repair`) |

### Index fields (Atlas-owned)

On the JanusGraph vertex OpenSearch index:

- `atlas_semantic_text` — pipeline input (removed after embed)
- `atlas_semantic_embedding` — `knn_vector`
- Ingest pipeline: `atlas-semantic-ingest`

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
atlas.search.semantic.enable=true
atlas.search.semantic.opensearch.model.id=<from init-opensearch-semantic-local.sh>
atlas.search.semantic.opensearch.embedding.dimension=384
atlas.semantic.indexer.kafka.group.id=atlas_semantic_indexer
atlas.semantic.indexer.batch.size=25
atlas.semantic.indexer.kafka.poll.timeout.ms=5000
atlas.semantic.indexer.health.enabled=true
atlas.semantic.indexer.health.port=8089
atlas.semantic.indexer.health.path=/health
```

OpenSearch hostname/port reuse `atlas.graph.index.search.hostname` and
`atlas.graph.index.search.port` from
`config/atlas/postgres/opensearch/atlas-application.properties`.

## Operations

1. Deploy OpenSearch ML embedding model (`init-opensearch-semantic-local.sh` or remote variant).
2. Set model id and dimension in Atlas config.
3. Start **Semantic Indexer** (`atlas_semantic_indexer.sh` or Docker `atlas-semantic-indexer` container).
4. Run **repair** once for existing data (`atlas_semantic_repair.sh`).
5. Enable `atlas.search.semantic.enable=true` on Atlas for REST search.

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
  "uptimeMs": 15853
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

`init-opensearch-semantic-local.sh` deploys a local MiniLM model and patches:

- `index.knn=true` on the vertex index
- ingest pipeline `atlas-semantic-ingest`
- `atlas_semantic_embedding` knn_vector mapping

For remote embedding models, use `init-opensearch-semantic-remote.sh` (see `config/connectors/README.md`).

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Empty semantic search results | Run repair or wait for indexer; check `curl localhost:9200/janusgraph_vertex_index/_count?q=exists:atlas_semantic_embedding` |
| Indexer restart loop | Check `docker logs atlas-semantic-indexer`; ensure slim image has `bin/atlas_semantic_indexer.sh` |
| Model id mismatch | Re-run `init-opensearch-semantic-local.sh`; update `atlas-semantic-docker.properties` |
| Clear embeddings only | `./seed/udf-retail/clear-opensearch-semantic-embeddings.sh` |
| Health endpoint unreachable | Recreate container after rebuild; confirm port `8089:8089` in `docker-compose.atlas-semantic.yml` |

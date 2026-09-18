---
name: Semantic Search
route: /SemanticSearch
menu: Documentation
submenu: Search
---

# Semantic Search

Semantic search finds metadata entities by **meaning** rather than exact keyword match.
Embeddings are stored on the existing JanusGraph **vertex index** in OpenSearch (`{index-name}_vertex_index`).

## Architecture

| Component | Role |
|-----------|------|
| **Atlas server** | Read-only `POST /v2/search/semantic` and `GET /v2/search/similar`; auth/scrub via discovery |
| **Semantic Indexer (SI)** | Kafka consumer on `ATLAS_ENTITIES`; enriches GUIDs from graph; partial-updates OpenSearch (`services/atlas-semantic-indexer`) |
| **atlas-semantic-repair** | Offline backfill for all existing vertices (`tools/atlas-semantic-repair`) |

Indexing is **not** performed inside the Atlas webapp. Run the Semantic Indexer as a separate process.

### Index fields (Atlas-owned)

On the JanusGraph vertex OpenSearch index:

- `atlas_semantic_text` — pipeline input (removed after embed)
- `atlas_semantic_embedding` — `knn_vector`
- Ingest pipeline: `atlas-semantic-ingest`

### DELETE policy

`ENTITY_DELETE` Kafka events are **skipped** (no-op). Stale embeddings may remain until JanusGraph removes the document or you run repair.

## Configuration

```properties
atlas.search.semantic.enable=false
atlas.search.semantic.opensearch.model.id=
atlas.search.semantic.opensearch.embedding.dimension=384
atlas.semantic.indexer.kafka.group.id=atlas_semantic_indexer
atlas.semantic.indexer.batch.size=25
```

OpenSearch hostname/port reuse `atlas.graph.index.search.hostname` and `atlas.graph.index.search.port`.

## Operations

1. Deploy OpenSearch ML embedding model (same ops flow as docker `init-opensearch-semantic-local.sh`).
2. Set model id and dimension in Atlas config.
3. Start **Semantic Indexer** (`atlas_semantic_indexer.sh`).
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

See `dev-support/atlas-docker/SemanticSearchCurlTest.md` for a docker curl walkthrough.

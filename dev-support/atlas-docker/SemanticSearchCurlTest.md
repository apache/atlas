# Semantic Search curl test (Docker)

Prerequisites: OpenSearch ML model deployed, Semantic Indexer running, repair completed, and
`atlas.search.semantic.enable=true` on Atlas.

## Semantic query

```bash
curl -u admin:admin -X POST "http://localhost:21000/api/atlas/v2/search/semantic" \
  -H "Content-Type: application/json" \
  -d '{"query":"sports car sedan","topK":5,"typeName":"demo_car"}'
```

## Similar entities

```bash
curl -u admin:admin "http://localhost:21000/api/atlas/v2/search/similar?guid=<GUID>&topK=5"
```

Expect `queryType: SEMANTIC` and optional `similarityScores` in the response.

# Atlas Metastore API Documentation

Developer reference for the Atlas Metastore REST API.

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| [`/api/atlas/v2/entity/bulk`](api/entity-bulk.md) | POST | Create or update entities (upsert). The primary write API for ingesting metadata. |
| [`/api/atlas/v2/search/indexsearch`](api/search-indexsearch.md) | POST | Search entities via Elasticsearch DSL. The primary read/search API. |

## Quick Start

### Search for Tables

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/search/indexsearch' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "dsl": {
    "size": 20,
    "query": {
      "bool": {
        "filter": [
          { "term": { "__typeName.keyword": "Table" } },
          { "term": { "__state": "ACTIVE" } }
        ]
      }
    }
  },
  "attributes": ["qualifiedName", "name", "description"],
  "excludeMeanings": true,
  "excludeClassifications": true
}'
```

### Create a Table

```bash
curl -X POST 'http://localhost:21000/api/atlas/v2/entity/bulk' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{
  "entities": [
    {
      "typeName": "Table",
      "attributes": {
        "qualifiedName": "mydb.public.users@snowflake",
        "name": "users",
        "description": "User accounts table"
      }
    }
  ]
}'
```

## Key Concepts

- **Upsert semantics:** The entity bulk API matches by `typeName` + `qualifiedName`. If a match exists, it updates; otherwise it creates.
- **Authorization:** All API calls require a Bearer token. Search results are automatically filtered by the user's permissions.
- **Elasticsearch DSL:** The search API accepts raw ES query DSL, giving full control over filtering, sorting, aggregations, and pagination.
- **Partial updates:** On update, only attributes present in the request are modified. Omitted attributes are left unchanged.

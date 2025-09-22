---
name: REST API
route: /RestApi
menu: Documentation
submenu: REST API
---

import {CustomLink} from "theme/components/shared/common/CustomLink";

# REST API

1. <CustomLink href="http://atlas.apache.org/api/v2/index.html">REST API Documentation</CustomLink>
2. <CustomLink href="http://atlas.apache.org/api/rest.html">Legacy API Documentation </CustomLink>

## Newly Added REST API Endpoints (ATLAS-4811)

This section documents REST endpoints that were missing from earlier versions of the Atlas documentation.  
Source of truth: the live Swagger UI (`/api/v2/ui/`) and Swagger JSON (`/api/v2/ui/swagger.json`) which, at time of writing, report version **2.4.0**.

---

### Entity REST – GET `/api/atlas/v2/entity/bulk`

**Summary:** Fetch multiple entities in a single request.

**Query parameters**
- `guid` (required, repeatable): One or more entity GUIDs to retrieve.

**Request example**
```bash
curl -s -X GET "http://localhost:21000/api/atlas/v2/entity/bulk?guid=<guid1>&guid=<guid2>" \
  -H "Accept: application/json"
```

**Response example**
```json
{
  "entities": [
    { "guid": "1234-5678-90", "typeName": "hive_table", "attributes": { "name": "sales" } }
  ]
}
```

---

### Entity REST – POST `/api/atlas/v2/entity/bulk`

**Summary:** Create or update multiple entities at once.

**Body schema:** JSON object with `entities` array.

**Request example**
```bash
curl -s -X POST "http://localhost:21000/api/atlas/v2/entity/bulk" \
  -H "Content-Type: application/json" \
  -d '{
        "entities": [
          { "typeName": "hive_table", "attributes": { "name": "sales" } }
        ]
      }'
```

**Response example**
```json
{
  "mutatedEntities": {
    "CREATE": [ { "guid": "abcd-efgh-1234" } ]
  }
}
```

---

### Discovery REST – GET `/api/atlas/v2/search/dsl`

**Summary:** Execute a DSL query against Atlas metadata.

**Query parameters**
- `query` (required): DSL expression.
- `limit` (optional): Max results (default 10).
- `offset` (optional): Paging offset.

**Request example**
```bash
curl -s -X GET "http://localhost:21000/api/atlas/v2/search/dsl?query=hive_table" \
  -H "Accept: application/json"
```

**Response example**
```json
{
  "queryType": "DSL",
  "queryText": "hive_table",
  "entities": [ { "guid": "1234", "typeName": "hive_table" } ]
}
```

---

### Discovery REST – GET `/api/atlas/v2/search/basic`

**Summary:** Full-text style search across entities.

**Query parameters**
- `query` (required): Search string.
- `limit` / `offset` (optional): Pagination.

**Request example**
```bash
curl -s -X GET "http://localhost:21000/api/atlas/v2/search/basic?query=sales" \
  -H "Accept: application/json"
```

**Response example**
```json
{
  "queryText": "sales",
  "entities": [ { "guid": "abcd", "typeName": "hive_table" } ]
}
```

---

#### Verifying these endpoints

Use the Swagger UI “Try it out” feature at `/api/v2/ui/` or `curl` against a running Atlas (default port `21000`).  
For authentication and server setup, see the main [Installation Guide](../InstallationSteps.md).

---

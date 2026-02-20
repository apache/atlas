# Purpose Discovery API - Test Curl Requests

## API Endpoint

**POST** `/api/meta/purposes/user`

Discovers all Purpose entities accessible to a user based on their username and group memberships.

## Request Schema

```json
{
  "username": "string (required)",
  "groups": ["string"] (required, can be empty),
  "attributes": ["string"] (optional),
  "limit": 100 (optional, default: 100, max: 500),
  "offset": 0 (optional, default: 0)
}
```

## Response Schema

```json
{
  "purposes": [AtlasEntityHeader],
  "count": 5,
  "totalCount": 10,
  "hasMore": true
}
```

---

## Test Curl Requests

### 1. Basic Request - Get Purposes for User (with access)

```bash
# User hritik.rai.testmember2 has access to 3 purposes on staging
```

### 1b. Basic Request - User without access (empty result expected)

```bash
curl 'https://staging.atlan.com/api/meta/purposes/user' \
  -H 'accept: application/json, text/plain, */*' \
  -H 'authorization: Bearer <YOUR_TOKEN>' \
  -H 'content-type: application/json' \
  -H 'x-atlan-client-origin: product_webapp' \
  --data-raw '{
    "username": "arnab.saha@atlan.com",
    "groups": ["temp0", "public"],
    "limit": 20,
    "offset": 0
  }'
```

### 2. Request with Specific Attributes

```bash
curl 'https://staging.atlan.com/api/meta/purposes/user' \
  -H 'accept: application/json, text/plain, */*' \
  -H 'authorization: Bearer <YOUR_TOKEN>' \
  -H 'content-type: application/json' \
  -H 'x-atlan-client-origin: product_webapp' \
  --data-raw '{
    "username": "arnab.saha@atlan.com",
    "groups": ["temp0", "public"],
    "attributes": [
      "name",
      "displayName",
      "description",
      "qualifiedName",
      "purposeClassifications",
      "isAccessControlEnabled",
      "__createdBy",
      "__modifiedBy",
      "__timestamp",
      "__modificationTimestamp"
    ],
    "limit": 100,
    "offset": 0
  }'
```

### 3. Pagination - Second Page

```bash
curl 'https://staging.atlan.com/api/meta/purposes/user' \
  -H 'accept: application/json, text/plain, */*' \
  -H 'authorization: Bearer <YOUR_TOKEN>' \
  -H 'content-type: application/json' \
  -H 'x-atlan-client-origin: product_webapp' \
  --data-raw '{
    "username": "arnab.saha@atlan.com",
    "groups": ["temp0", "public"],
    "limit": 20,
    "offset": 20
  }'
```

### 4. Public Purposes Only (Empty Groups)

```bash
curl 'https://staging.atlan.com/api/meta/purposes/user' \
  -H 'accept: application/json, text/plain, */*' \
  -H 'authorization: Bearer <YOUR_TOKEN>' \
  -H 'content-type: application/json' \
  -H 'x-atlan-client-origin: product_webapp' \
  --data-raw '{
    "username": "test-user@atlan.com",
    "groups": [],
    "limit": 100,
    "offset": 0
  }'
```

---

## Validation Test Cases

### 5. Error Case - Missing Username (Should Return 400)

```bash
curl 'https://staging.atlan.com/api/meta/purposes/user' \
  -H 'accept: application/json, text/plain, */*' \
  -H 'authorization: Bearer <YOUR_TOKEN>' \
  -H 'content-type: application/json' \
  -H 'x-atlan-client-origin: product_webapp' \
  --data-raw '{
    "groups": ["temp0"]
  }'
```

**Expected Response:**
```json
{
  "errorCode": "ATLAS-400-00-001",
  "errorMessage": "username is required"
}
```

### 6. Error Case - Missing Groups (Should Return 400)

```bash
curl 'https://staging.atlan.com/api/meta/purposes/user' \
  -H 'accept: application/json, text/plain, */*' \
  -H 'authorization: Bearer <YOUR_TOKEN>' \
  -H 'content-type: application/json' \
  -H 'x-atlan-client-origin: product_webapp' \
  --data-raw '{
    "username": "arnab.saha@atlan.com"
  }'
```

**Expected Response:**
```json
{
  "errorCode": "ATLAS-400-00-001",
  "errorMessage": "groups is required (can be empty list)"
}
```

### 7. Error Case - Invalid Limit (Should Return 400)

```bash
curl 'https://staging.atlan.com/api/meta/purposes/user' \
  -H 'accept: application/json, text/plain, */*' \
  -H 'authorization: Bearer <YOUR_TOKEN>' \
  -H 'content-type: application/json' \
  -H 'x-atlan-client-origin: product_webapp' \
  --data-raw '{
    "username": "arnab.saha@atlan.com",
    "groups": [],
    "limit": 1000
  }'
```

**Expected Response:**
```json
{
  "errorCode": "ATLAS-400-00-001",
  "errorMessage": "limit cannot exceed 500"
}
```

---

## Full Curl with All Headers (Copy-Paste Ready)

Replace `<YOUR_TOKEN>` with your Bearer token from browser dev tools.

```bash
curl 'https://staging.atlan.com/api/meta/purposes/user' \
  -H 'accept: application/json, text/plain, */*' \
  -H 'accept-language: en-CA,en-GB;q=0.9,en-US;q=0.8,en;q=0.7' \
  -H 'authorization: Bearer <YOUR_TOKEN>' \
  -H 'content-type: application/json' \
  -H 'origin: https://staging.atlan.com' \
  -H 'referer: https://staging.atlan.com/' \
  -H 'sec-ch-ua: "Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  -H 'sec-fetch-dest: empty' \
  -H 'sec-fetch-mode: cors' \
  -H 'sec-fetch-site: same-origin' \
  -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36' \
  -H 'x-atlan-client-origin: product_webapp' \
  -H 'x-atlan-via-ui: true' \
  --data-raw '{
    "username": "arnab.saha@atlan.com",
    "groups": ["temp0", "public"],
    "attributes": [
      "name",
      "displayName",
      "description",
      "qualifiedName",
      "purposeClassifications",
      "isAccessControlEnabled"
    ],
    "limit": 20,
    "offset": 0
  }'
```

---

## Expected Success Response Example

```json
{
  "purposes": [
    {
      "typeName": "Purpose",
      "guid": "abc123-def456-...",
      "attributes": {
        "name": "PII Data Access",
        "displayName": "PII Data Access",
        "description": "Purpose for accessing PII data",
        "qualifiedName": "default/purpose123",
        "purposeClassifications": ["PII", "Sensitive"],
        "isAccessControlEnabled": true
      },
      "status": "ACTIVE"
    }
  ],
  "count": 1,
  "totalCount": 5,
  "hasMore": true
}
```

---

## Notes

- The API queries AuthPolicy entities with `policyCategory="purpose"` that match the user's username or group memberships
- Results include purposes accessible via the "public" group automatically
- Pagination uses `offset` and `limit` parameters
- Default attributes are returned if `attributes` field is not specified

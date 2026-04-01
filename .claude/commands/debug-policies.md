---
description: Debug policy/authorization issues (lock icons, access denied) on an Atlas tenant
argument-hint: [connection-name-or-guid]
allowed-tools: [Bash, Read, Grep, Glob, Task, AskUserQuestion]
---

# Debug Atlas Policy Issues

You are debugging authorization/policy problems on an Atlas tenant. The user sees lock icons on assets, access denied errors, or scrubbed search results. This is a systematic diagnostic — run each phase, report findings, and only move to fixes after confirming root cause.

**Connection hint from user:** $ARGUMENTS

## Prerequisites

First, confirm kubectl access and get a working token:

```bash
# Verify pod is accessible
kubectl exec -n atlas atlas-0 -c atlas-main -- echo "Pod accessible"

# Get keycloak credentials from pod
KC_SECRET=$(kubectl exec -n atlas atlas-0 -c atlas-main -- printenv KEYCLOAK_CLIENT_SECRET 2>/dev/null)
KC_CLIENT=$(kubectl exec -n atlas atlas-0 -c atlas-main -- printenv KEYCLOAK_CLIENT_ID 2>/dev/null)

# Get token
TOKEN=$(kubectl exec -n atlas atlas-0 -c atlas-main -- curl -s -X POST \
  'http://keycloak-http.keycloak.svc.cluster.local/auth/realms/default/protocol/openid-connect/token' \
  -d "client_id=$KC_CLIENT" \
  -d "client_secret=$KC_SECRET" \
  -d 'grant_type=client_credentials' -d 'scope=openid' | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")
```

If token fails, read `/opt/apache-atlas/conf/keycloak.json` on the pod for the correct credentials.

---

## Phase 1: PolicyRefresher Health Check

Check if the policy engine loaded policies at all:

```bash
kubectl exec -n atlas atlas-0 -c atlas-main -- bash -c \
  'grep -E "policy engine|policy count|PolicyRefresher.*loading|PolicyRefresher.*found" /opt/apache-atlas/logs/atlas.2026*.out | tail -15'
```

**Report:**
- How many policy evaluators loaded (expected: 150+)
- Whether PolicyRefresher is running delta refreshes
- Any ERROR lines related to policies

Also check the authorizer config:
```bash
kubectl exec -n atlas atlas-0 -c atlas-main -- grep 'atlas.authorizer' /opt/apache-atlas/conf/atlas-application.properties
```

If `atlas.authorizer.impl=none`, policies are disabled — that's NOT the issue (everything should be allowed). If `atlas.authorizer.impl=atlas`, proceed with diagnosis.

---

## Phase 2: List All Connections and Their Policy Counts

```bash
kubectl exec -n atlas atlas-0 -c atlas-main -- curl -s \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -X POST 'http://localhost:21000/api/meta/search/indexsearch' \
  -d '{"dsl":{"from":0,"size":50,"query":{"bool":{"must":[{"term":{"__typeName.keyword":"Connection"}},{"term":{"__state":"ACTIVE"}}]}}}}' | \
  python3 -c "
import json, sys
for e in json.load(sys.stdin).get('entities', []):
    a = e.get('attributes', {})
    print(f'{a.get(\"name\",\"?\")} | guid={e.get(\"guid\")} | qn={a.get(\"qualifiedName\",\"?\")} | adminUsers={a.get(\"adminUsers\",[])} | policyCount={a.get(\"assetPoliciesCount\",0)}')
"
```

If the user specified a connection via $ARGUMENTS, identify the target connection from the results. Otherwise, identify connections with `policyCount=0` — those are the problem connections.

---

## Phase 3: Check Bootstrap Policies for Problem Connection(s)

For each connection with issues, check if its 8 bootstrap policies exist:

```bash
CONN_GUID="<guid from phase 2>"
kubectl exec -n atlas atlas-0 -c atlas-main -- curl -s \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -X POST 'http://localhost:21000/api/meta/search/indexsearch' \
  -d "{\"dsl\":{\"from\":0,\"size\":20,\"query\":{\"bool\":{\"must\":[{\"term\":{\"__typeName.keyword\":\"AuthPolicy\"}},{\"prefix\":{\"qualifiedName\":\"$CONN_GUID\"}}]}}}}" | \
  python3 -c "
import json, sys
ents = json.load(sys.stdin).get('entities', [])
print(f'Bootstrap policies found: {len(ents)}')
expected = ['connection-CRUD', 'connection-classification', 'connection-entity-business-metadata',
            'connection-entity-label', 'connection-link-assets', 'connection-link-assets-inverse',
            'connection-add-terms', 'dataPolicy-connection']
found = [e.get('attributes',{}).get('qualifiedName','').split('/')[-1] for e in ents]
missing = [p for p in expected if p not in found]
print(f'Found: {found}')
if missing:
    print(f'MISSING: {missing}')
else:
    print('All 8 bootstrap policies present')
"
```

**Expected**: 8 policies. If 0 or fewer than 8, note which are missing.

---

## Phase 4: Check Keycloak Role

```bash
CONN_GUID="<guid>"
ROLE_NAME="connection_admins_${CONN_GUID}"
kubectl exec -n atlas atlas-0 -c atlas-main -- curl -s -w '\nHTTP_STATUS:%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "http://keycloak-http.keycloak.svc.cluster.local/auth/admin/realms/default/roles/$ROLE_NAME" | tail -1
```

- **HTTP_STATUS:200** = role exists
- **HTTP_STATUS:404** = role missing

---

## Phase 5: Check User Role Assignment

Get the admin user(s) from the Connection entity (from Phase 2 output), then check each:

```bash
USERNAME="<admin-user-from-connection>"
USER_ID=$(kubectl exec -n atlas atlas-0 -c atlas-main -- curl -s \
  -H "Authorization: Bearer $TOKEN" \
  "http://keycloak-http.keycloak.svc.cluster.local/auth/admin/realms/default/users?username=$USERNAME&exact=true" | \
  python3 -c "import json,sys; users=json.load(sys.stdin); print(users[0]['id'] if users else 'NOT_FOUND')")

kubectl exec -n atlas atlas-0 -c atlas-main -- curl -s \
  -H "Authorization: Bearer $TOKEN" \
  "http://keycloak-http.keycloak.svc.cluster.local/auth/admin/realms/default/users/$USER_ID/role-mappings/realm" | \
  python3 -c "
import json, sys
roles = json.load(sys.stdin)
conn_roles = [r['name'] for r in roles if 'connection_admins' in r['name']]
admin_roles = [r['name'] for r in roles if r['name'].startswith('\$')]
print(f'Connection admin roles: {conn_roles}')
print(f'System roles: {admin_roles}')
"
```

---

## Phase 6: Diagnosis Summary

After running Phases 1-5, present a clear diagnosis table to the user:

```
| Check                          | Status | Detail                      |
|-------------------------------|--------|-----------------------------|
| PolicyRefresher running        | OK/FAIL | N policies loaded           |
| Authorizer enabled             | OK/N/A | atlas / none                |
| Connection identified          | OK/FAIL | name, guid, qn              |
| Bootstrap policies (8 expected)| OK/FAIL | N found, M missing          |
| Keycloak role exists           | OK/FAIL | connection_admins_{guid}    |
| User assigned to role          | OK/FAIL | username -> role             |
```

Identify what needs to be fixed. Then use AskUserQuestion to ask the user which fixes to apply.

---

## Phase 7: Apply Fixes (with user confirmation)

Use AskUserQuestion to confirm before each fix. The fixes are:

### Fix A: Create Missing Bootstrap Policies

Only if Phase 3 showed missing policies. Build the policy JSON using these values from Phase 2:
- `CONN_GUID` = connection GUID
- `CONN_NAME` = connection name
- `CONN_QN` = connection qualifiedName

The 8 required policies are defined in `addons/static/templates/connection_bootstrap_policies.json`. Read that template file, substitute `{guid}` -> CONN_GUID, `{name}` -> CONN_NAME, `{entity}` -> CONN_QN, set `isPolicyEnabled: true`, and POST to `/api/meta/entity/bulk`.

After creation, verify with the same search from Phase 3.

### Fix B: Create Missing Keycloak Role

Only if Phase 4 showed 404:

```bash
kubectl exec -n atlas atlas-0 -c atlas-main -- curl -s -w '\nHTTP:%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -X POST 'http://keycloak-http.keycloak.svc.cluster.local/auth/admin/realms/default/roles' \
  -d "{\"name\":\"connection_admins_${CONN_GUID}\",\"description\":\"Connection admins for ${CONN_NAME}\"}"
```

### Fix C: Assign User to Role

Only if Phase 5 showed the user is NOT assigned. Get role ID and user ID, then:

```bash
kubectl exec -n atlas atlas-0 -c atlas-main -- curl -s -w '\nHTTP:%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -X POST "http://keycloak-http.keycloak.svc.cluster.local/auth/admin/realms/default/users/$USER_ID/role-mappings/realm" \
  -d "[{\"id\":\"$ROLE_ID\",\"name\":\"$ROLE_NAME\"}]"
```

---

## Phase 8: Verification

After applying fixes:

1. Wait 30s for PolicyRefresher delta to pick up new policies
2. Check logs for delta update:
```bash
kubectl exec -n atlas atlas-0 -c atlas-main -- bash -c \
  'grep -E "PolicyDelta|policy engine|policy count" /opt/apache-atlas/logs/atlas.2026*.out | tail -10'
```
3. Verify new policy count is higher than before
4. Tell the user: **"Re-login to the UI (or refresh browser session) to get a new JWT token with the updated roles. Lock icons should disappear."**

---

## Important Notes

- **Never disable the authorizer** (`atlas.authorizer.impl=none`) as a fix — that removes all access control
- The `atlan-backend` service account token works for both Atlas API and Keycloak admin API
- PolicyRefresher delta runs every 30 seconds — new policies are picked up automatically
- Users must re-login after role changes because Keycloak roles are embedded in the JWT
- The bootstrap policy template is at `addons/static/templates/connection_bootstrap_policies.json`
- `ConnectionPreProcessor.processCreateConnection()` (line 108) is what normally creates these — check why it failed if this is a recurring issue
- Read the detailed debugging guide at `.claude/projects/-Users-sriram-aravamuthan-repositories-atlas-metastore/memory/policy-debugging.md`

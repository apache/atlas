<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->
# Export & Import Audits

#### Background

The new audits for Export and Import operations also have corresponding REST APIs to programatically fetch the audit entries.

#### REST APIs

|Title           | Replication Audits for a Cluster                                 |
|----------------|------------------------------------------------------------------|
|Example         | See below.                                                       |
|URL             | api/atlas/admin/expimp/audit                                     |
|Method          | GET                                                              |
|URL Parameters  | _sourceClusterName_: Name of source cluster.                     |
|                | _targetClusterName_: Name of target cluster.                     |
|                | _userName_: Name of the user who initiated the operation.        |
|                | _operation_: EXPORT or IMPORT operation type.                    |
|                | _startTime_: Time, in milliseconds, when operation was started.  |
|                | _endTime_: Time, in milliseconds, when operation ended.          |
|                | _limit_: Number of results to be returned                        |
|                | _offset_: Offset                                                 |
|Data Parameters | None                                                             |
|Success Response| List of _ExportImportAuditEntry_                                 |
|Error Response  | Errors Returned as AtlasBaseException                            |
|Notes           | None                                                             |

###### CURL
curl -X GET -u admin:admin -H "Content-Type: application/json" -H "Cache-Control: no-cache" 'http://localhost:21000/api/atlas/admin/expimp/audit?sourceClusterName=cl2'

```json
{
    "queryType": "BASIC",
    "searchParameters": {
        "typeName": "ReplicationAuditEntry",
        "excludeDeletedEntities": false,
        "includeClassificationAttributes": false,
        "includeSubTypes": true,
        "includeSubClassifications": true,
        "limit": 100,
        "offset": 0,
        "entityFilters": {
            "attributeName": "name",
            "operator": "eq",
            "attributeValue": "cl2",
            "criterion": []
        }
    },
    "entities": [{
        "typeName": "ReplicationAuditEntry",
        "attributes": {
            "owner": null,
            "uniqueName": "cl2:EXPORT:1533037289411",
            "createTime": null,
            "name": "cl2",
            "description": null
        },
        "guid": "04844141-af72-498a-9d26-f70f91e8adf8",
        "status": "ACTIVE",
        "displayText": "cl2",
        "classificationNames": []
    }, {
        "typeName": "ReplicationAuditEntry",
        "attributes": {
            "owner": null,
            "uniqueName": "cl2:EXPORT:1533037368407",
            "createTime": null,
            "name": "cl2",
            "description": null
        },
        "guid": "837abe66-20c8-4296-8678-e715498bf8fb",
        "status": "ACTIVE",
        "displayText": "cl2",
        "classificationNames": []
    }]
}
```

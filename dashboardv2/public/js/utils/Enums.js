/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

define(['require'], function(require) {
    'use strict';

    var Enums = {};

    Enums.auditAction = {
        ENTITY_CREATE: "Entity Created",
        ENTITY_UPDATE: "Entity Updated",
        ENTITY_DELETE: "Entity Deleted",
        TAG_ADD: "Tag Added",
        TAG_DELETE: "Tag Deleted",
        TAG_UPDATE: "Tag Updated",
        ENTITY_IMPORT_CREATE: "Entity Created by import",
        ENTITY_IMPORT_UPDATE: "Entity Updated by import",
        ENTITY_IMPORT_DELETE: "Entity Deleted by import"
    }

    Enums.entityStateReadOnly = {
        ACTIVE: false,
        DELETED: true,
        STATUS_ACTIVE: false,
        STATUS_DELETED: true
    }

    Enums.lineageUrlType = {
        INPUT: 'inputs',
        OUTPUT: 'outputs',
        SCHEMA: 'schema'
    }

    Enums.searchUrlType = {
        DSL: 'dsl',
        FULLTEXT: 'basic'
    }

    Enums.profileTabType = {
        'count-frequency': "Count Frequency Distribution",
        'decile-frequency': "Decile Frequency Distribution",
        'annual': "Annual Distribution"
    }

    return Enums;
});
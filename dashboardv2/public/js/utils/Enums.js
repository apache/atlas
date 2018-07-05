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
        CLASSIFICATION_ADD: "Classification Added",
        CLASSIFICATION_DELETE: "Classification Deleted",
        CLASSIFICATION_UPDATE: "Classification Updated",
        PROPAGATED_CLASSIFICATION_ADD: "Propagated Classification Added",
        PROPAGATED_CLASSIFICATION_DELETE: "Propagated Classification Deleted",
        PROPAGATED_CLASSIFICATION_UPDATE: "Propagated Classification Updated",
        ENTITY_IMPORT_CREATE: "Entity Created by import",
        ENTITY_IMPORT_UPDATE: "Entity Updated by import",
        ENTITY_IMPORT_DELETE: "Entity Deleted by import",
        TERM_ADD: "Term Added",
        TERM_DELETE: "Term Deleted"
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
    Enums.extractFromUrlForSearch = {
        "searchParameters": {
            "pageLimit": "limit",
            "type": "typeName",
            "tag": "classification",
            "query": "query",
            "pageOffset": "offset",
            "includeDE": "excludeDeletedEntities",
            "excludeST": "includeSubTypes",
            "excludeSC": "includeSubClassifications",
            "tagFilters": "tagFilters",
            "entityFilters": "entityFilters",
            "attributes": "attributes",
            "term": "termName"
        },
        "uiParameters": "uiParameters"
    }
    Enums.regex = {
        RANGE_CHECK: {
            "byte": {
                min: -128,
                max: 127
            },
            "short": {
                min: -32768,
                max: 32767
            },
            "int": {
                min: -2147483648,
                max: 2147483647
            },
            "long": {
                min: -9223372036854775808,
                max: 9223372036854775807
            },
            "float": {
                min: -3.4028235E38,
                max: 3.4028235E38
            },
            "double": {
                min: -1.7976931348623157E308,
                max: 1.7976931348623157E308
            }
        }
    }
    Enums.graphIcon = {
        // hive_db: { icon: "fa-database", textContent: '\uf1c0' },
        // hive_column: { icon: "fa-columns", textContent: '\uf0db' },
        // hive_table: { icon: "fa-table", textContent: '\uf0ce' }
    }
    var getTermRelationAttributes = function() {
        return {
            description: null,
            expression: null,
            steward: null,
            source: null
        }
    }
    Enums.termRelationAttributeList = {
        seeAlso: getTermRelationAttributes(),
        synonyms: getTermRelationAttributes(),
        antonyms: getTermRelationAttributes(),
        preferredTerms: getTermRelationAttributes(),
        preferredToTerms: getTermRelationAttributes(),
        replacementTerms: getTermRelationAttributes(),
        replacedBy: getTermRelationAttributes(),
        translationTerms: getTermRelationAttributes(),
        translatedTerms: getTermRelationAttributes(),
        isA: getTermRelationAttributes(),
        classifies: getTermRelationAttributes(),
        validValues: getTermRelationAttributes(),
        validValuesFor: getTermRelationAttributes()
    }
    Enums.addOnClassification = ["_CLASSIFIED", "_NOT_CLASSIFIED"]
    return Enums;
});
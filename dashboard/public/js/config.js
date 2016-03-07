/*
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
'use strict';
var host = '',
    port = '',
    baseUrl = '/api/atlas/',
    apiHost = (host !== '') ? host + ':' + port + baseUrl : baseUrl;

angular.module('dgc').constant('atlasConfig', {
    API_ENDPOINTS: {
        ABOUT: apiHost + 'admin/version',
        GET_ENTITY: apiHost + 'entities',
        ATTACH_DETACH_TRAITS: 'traits',
        SCHEMA_LINEAGE_PREPEND: apiHost + 'lineage/hive/table',
        SCHEMA_APPEND: 'schema',
        GRAPH: 'graph',
        TRAITS_LIST: apiHost + 'types?type=TRAIT',
        SEARCH: apiHost + 'discovery/search/',
        CREATE_TRAIT: apiHost + 'types'
    },
    SEARCH_TYPE: {
        dsl: {
            value: 'dsl',
            displayText: 'DSL',
            placeholder: 'Search using a DSL query: e.g. DataSet where name="sales_fact"'
        },
        fulltext: {
            value: 'fulltext',
            displayText: 'Text',
            placeholder: 'Search using a query string: e.g. sales_fact'
        }
    }
});

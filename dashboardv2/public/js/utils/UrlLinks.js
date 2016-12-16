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

define(['require', 'utils/Enums'], function(require, Enums) {
    'use strict';

    var UrlLinks = {
        baseUrl: '/api/atlas',
        baseUrlV2: '/api/atlas/v2',
        taxonomiesApiUrl: function() {
            return this.baseUrl + '/v1/taxonomies';
        },
        taxonomiesTermsApiUrl: function(name) {
            return this.baseUrl + '/v1/taxonomies' + '/' + name + '/terms';
        },
        entitiesApiUrl: function(guid, name) {
            var entitiesUrl = this.baseUrlV2 + '/entity';
            if (guid && name) {
                return entitiesUrl + '/guid/' + guid + '/classification/' + name;
            } else if (guid && !name) {
                return entitiesUrl + '/guid/' + guid;
            } else {
                return entitiesUrl;
            }
        },
        entitiesDefApiUrl: function(name) {
            var entitieDefUrl = this.baseUrlV2 + '/types/entitydef';
            if (name) {
                return entitieDefUrl + '/name/' + name;
            } else {
                return entitieDefUrl;
            }
        },
        entitiesTraitsApiUrl: function(token) {
            return this.baseUrlV2 + '/entity/guid/' + token + "/classifications"; //this.entitiesApiUrl();
        },
        entityCollectionaudit: function(guid) {
            return this.baseUrl + '/entities/' + guid + '/audit';
        },
        typesClassicationApiUrl: function(name, guid) {
            var typeUrl = this.baseUrlV2 + '/types/classificationdef'
            if (name) {
                return typeUrl + '/name/' + name;
            } else if (guid) {
                return typeUrl + '/guid/' + guid;
            } else {
                return typeUrl;
            }
        },
        typesApiUrl: function() {
            return this.baseUrlV2 + '/types/typedefs/headers'
        },
        lineageApiUrl: function(guid) {
            var lineageUrl = this.baseUrlV2 + '/lineage';
            if (guid) {
                return lineageUrl + '/' + guid;
            } else {
                return lineageUrl
            }
        },
        schemaApiUrl: function(guid) {
            var lineageUrl = this.baseUrl + '/lineage';
            if (guid) {
                return lineageUrl + '/' + guid + '/schema'
            } else {
                return lineageUrl
            }
        },
        searchApiUrl: function(searchtype) {
            var searchUrl = this.baseUrl + '/discovery/search';
            if (searchtype) {
                return searchUrl + '/' + searchtype;
            } else {
                return searchUrl;
            }
        },
        versionApiUrl: function() {
            return this.baseUrl + '/admin/version';
        },
        sessionApiUrl: function() {
            return this.baseUrl + '/admin/session';
        }

    };

    return UrlLinks;
});

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

angular.module('dgc.details').factory('detailsResource', ['$resource', 'atlasConfig', function($resource, atlasConfig) {
    return $resource(atlasConfig.API_ENDPOINTS.GET_ENTITY + '/:id', {}, {
        get: {
            method: 'GET',
            transformResponse: function(data) {
                if (data) {
                    return angular.fromJson(data.definition);
                }
            },
            responseType: 'json'
        },
        saveTag: {
            method: 'POST',
            url: atlasConfig.API_ENDPOINTS.GET_ENTITY + '/:id/' + atlasConfig.API_ENDPOINTS.ATTACH_DETACH_TRAITS
        },
        detachTag: {
            method: 'DELETE',
            url: atlasConfig.API_ENDPOINTS.GET_ENTITY + '/:id/' + atlasConfig.API_ENDPOINTS.ATTACH_DETACH_TRAITS + '/:tagName'
        }
    });

}]).factory('schemaResource', ['$resource', 'atlasConfig', function($resource, atlasConfig) {
    return $resource(atlasConfig.API_ENDPOINTS.SCHEMA_LINEAGE_PREPEND + '/:tableName/' + atlasConfig.API_ENDPOINTS.SCHEMA_APPEND, {}, {
        get: {
            method: 'GET',
            transformResponse: function(data) {
                if (data) {
                    return angular.fromJson(data);
                }
            },
            responseType: 'json'
        }
    });
}]);

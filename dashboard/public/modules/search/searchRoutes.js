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

//Setting up route
angular.module('dgc.search').config(['$stateProvider',
    function($stateProvider) {
        $stateProvider.state('search', {
            url: '/search?query',
            templateUrl: '/modules/search/views/search.html',
            controller: 'searchController'
        });
        $stateProvider.state('addTagHome', {
            parent: 'search',
            params: {
                tId: null,
                frm: 'search'
            },
            onEnter: ['$stateParams', '$state', '$modal', 'navigationResource', function($stateParams, $state, $modal, navigationResource) {
                $modal.open({
                    templateUrl: '/modules/tags/instance/views/createTag.html',
                    controller: 'createTagController',
                    windowClass: 'create-tag-entity',
                    resolve: {
                        typesList: function() {
                            return navigationResource.get().$promise;
                        }
                    }
                }).result.finally(function() {
                    $state.go('^');
                });
            }]
        });
    }
]);

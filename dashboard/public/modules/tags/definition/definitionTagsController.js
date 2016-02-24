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

angular.module('dgc.tags.definition').controller('definitionTagsController', ['$scope', '$resource', '$state', '$stateParams', 'lodash', 'attributeDefinition', 'tagClasses', 'tagsResource', 'notificationService', 'navigationResource', '$cacheFactory', 'atlasConfig',
    function($scope, $resource, $state, $stateParams, _, attributeDefinition, categories, tagsResource, notificationService, navigationResource, $cacheFactory, atlasConfig) {
        $scope.categoryList = categories;
        $scope.category = 'TRAIT';
        $scope.tagModel = {
            typeName: null,
            superTypes: [],
            attributeDefinitions: []
        };
        $scope.typesList = navigationResource.get();
        $scope.newtagModel = angular.copy($scope.tagModel);
        $scope.addAttribute = function addAttribute() {
            $scope.tagModel.attributeDefinitions.push(attributeDefinition.getModel());
        };

        $scope.removeAttribute = function(index) {
            $scope.tagModel.attributeDefinitions.splice(index, 1);
        };

        $scope.categoryChange = function categorySwitched() {
            $scope.categoryInst = categories[$scope.category].clearTags();
        };

        $scope.reset = function() {
            $scope.tagModel = angular.copy($scope.newtagModel);
            $scope.selectedParent = undefined;
        };

        $scope.refreshTags = function() {
            var httpDefaultCache = $cacheFactory.get('$http');
            httpDefaultCache.remove(atlasConfig.API_ENDPOINTS.TRAITS_LIST);
            $scope.typesList = navigationResource.get();
        };

        $scope.save = function saveTag(form) {
            $scope.savedTag = null;
            if (form.$valid) {
                $scope.tagModel.superTypes = $scope.selectedParent;
                $scope.categoryInst = categories[$scope.category];
                $scope.categoryInst.clearTags().addTag($scope.tagModel);

                notificationService.reset();
                $scope.saving = true;

                tagsResource.save($scope.categoryInst.toJson()).$promise
                    .then(function tagCreateSuccess() {
                        notificationService.info('"' + $scope.tagModel.typeName + '" has been created', false);
                        var httpDefaultCache = $cacheFactory.get('$http');
                        httpDefaultCache.remove(atlasConfig.API_ENDPOINTS.TRAITS_LIST);
                        $scope.typesList = navigationResource.get();
                        $scope.reset();
                    }).catch(function tagCreateFailed(error) {
                        notificationService.error(error.data.error, false);
                    }).finally(function() {
                        $scope.saving = false;
                    });
            }
        };
    }
]);

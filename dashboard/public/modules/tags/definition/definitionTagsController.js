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

angular.module('dgc.tags.definition').controller('DefinitionTagsController', ['$scope', '$resource', '$state', '$stateParams', 'lodash', 'AttributeDefinition', 'TagClasses', 'TagsResource', 'NotificationService', 'NavigationResource',
    function($scope, $resource, $state, $stateParams, _, AttributeDefinition, Categories, TagsResource, NotificationService, NavigationResource) {
        $scope.categoryList = Categories;
        $scope.category = 'TRAIT';
        $scope.tagModel = {
            typeName: null,
            superTypes: [],
            attributeDefinitions: []
        };
        $scope.typesList = NavigationResource.get();

        $scope.addAttribute = function AddAttribute() {
            $scope.tagModel.attributeDefinitions.push(AttributeDefinition.getModel());
        };

        $scope.removeAttribute = function(index) {
            $scope.tagModel.attributeDefinitions.splice(index, 1);
        };

        $scope.categoryChange = function CategorySwitched() {
            $scope.categoryInst = Categories[$scope.category].clearTags();
        };

        $scope.save = function saveTag(form) {
            $scope.savedTag = null;
            if (form.$valid) {
                $scope.tagModel.superTypes = $scope.selectedParent;
                $scope.categoryInst = Categories[$scope.category];
                $scope.categoryInst.clearTags().addTag($scope.tagModel);

                NotificationService.reset();
                $scope.saving = true;

                TagsResource.save($scope.categoryInst.toJson()).$promise
                    .then(function TagCreateSuccess() {
                        NotificationService.info('"' + $scope.tagModel.typeName + '" has been created', false);
                    }).catch(function TagCreateFailed(error) {
                        NotificationService.error(error.data.error, false);
                    }).finally(function() {
                        $scope.saving = false;
                    });
            }
        };
    }
]);
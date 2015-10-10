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

angular.module('dgc.tags.instance').controller('CreateTagController', ['$scope', 'DetailsResource', '$modalInstance', 'typesList', 'lodash', 'TagsResource', '$stateParams', '$rootScope', 'TagClasses', 'NotificationService',
    function($scope, DetailsResource, $modalInstance, typesList, _, TagsResource, $stateParams, $rootScope, Categories, NotificationService) {
        if (typesList) {
            $scope.typesList = typesList;
        }
        $scope.categoryList = Categories;
        $scope.category = 'TRAIT';

        $scope.getAttributeDefinations = function() {
            TagsResource.get({
                id: $scope.selectedType
            }, function(data) {
                var instanceType = Categories[$scope.category].instanceInfo();
                if (instanceType) {
                    var traitTypes = angular.fromJson(data.definition)[instanceType][0];
                    if (traitTypes) {
                        $scope.propertiesList = {};
                        $scope.isRequired = {};
                        _.each(traitTypes.attributeDefinitions, function(value) {
                            $scope.propertiesList[value.name] = '';
                            $scope.isRequired[value.name] = value.isRequired;
                        });
                    }
                }

            });
        };
        $scope.ok = function(tagDefinitionform) {
            if (tagDefinitionform.$valid) {
                var requestObject = {
                    "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Struct",
                    "typeName": $scope.selectedType,
                    "values": $scope.propertiesList
                };
                DetailsResource.saveTag({
                    id: $stateParams.id
                }, requestObject).$promise.then(function() {
                    $rootScope.$broadcast('refreshResourceData');
                    NotificationService.info('Tag "' + $scope.selectedType + '" has been added to entity', true);
                    $modalInstance.close(true);
                }).catch(function(err) {
                    $scope.isError = true;
                    $scope.error = err.data.error;
                });
            }
        };

        $scope.cancel = function() {
            $modalInstance.dismiss('cancel');
        };
    }
]);

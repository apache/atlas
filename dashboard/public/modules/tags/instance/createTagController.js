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
        var $$ = angular.element;
        $scope.categoryList = Categories;
        $scope.category = 'TRAIT';

        $scope.getAttributeDefinations = function() {
            $scope.propertiesList = {};
            $scope.isRequired = {};
            $scope.getAttributeApi($scope.selectedType);
        };

        $scope.getAttributeApi = function(tagName) {
            TagsResource.get({
                id: tagName
            }, function(data) {
                var instanceType = Categories[$scope.category].instanceInfo();
                if (instanceType) {
                    var traitTypes = angular.fromJson(data.definition)[instanceType];

                    for (var t = 0; t < traitTypes.length; t++) {
                        if (traitTypes[t]) {
                           for(var indx = 0; indx < traitTypes[t].attributeDefinitions.length; indx++)
                            { 
                                var attrDefn = traitTypes[t].attributeDefinitions[indx];
                                $scope.propertiesList[attrDefn.name] = '';
                                $scope.isRequired[attrDefn.name] = attrDefn.isRequired;
                            }
                        }

                        if (traitTypes[t].superTypes && traitTypes[t].superTypes.length > 0) {
                            for (var s = 0; s < traitTypes[t].superTypes.length; s++) {
                                $scope.getAttributeApi(traitTypes[t].superTypes[s]);
                            }
                        }
                    }
                }
            });
        };
        $scope.ok = function($event, tagDefinitionform) {
            if (tagDefinitionform.$valid) {
                var requestObject = {
                    "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Struct",
                    "typeName": $scope.selectedType,
                    "values": $scope.propertiesList
                };
                DetailsResource.saveTag({
                    id: $stateParams.id
                }, requestObject).$promise.then(function(data) {
                    if (data.requestId !== undefined && data.GUID === $stateParams.id) {
                        var tagName = $$("#tagDefinition").val();
                        $rootScope.updateTags(true, {
                            added: $scope.selectedType
                        });
                        $$("#" + $stateParams.id).append("<a class='tabsearchanchor ng-binding ng-scope' data-ui-sref='search({query: " + tagName + "})' title='" + tagName + "' href='#!/search?query=" + tagName + "'>" + tagName + "<span> </span></a>");
                    }
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
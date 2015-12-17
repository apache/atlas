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

angular.module('dgc.details').controller('DetailsController', ['$window', '$scope', '$state', '$stateParams', 'DetailsResource', 'SchemaResource',
    function($window, $scope, $state, $stateParams, DetailsResource, SchemaResource) {

        $scope.tableName = false;
        $scope.isTable = false;
        $scope.isLineage = false;
        
        DetailsResource.get({
            id: $stateParams.id

        }, function(data) {
            $scope.details = data;
            $scope.tableName = data.values.name;
            $scope.onActivate('io');
            $scope.isTags = (typeof data.traits !== 'undefined' && typeof data.traits === 'object') ? true : false;

            if (data && data.values && data.values.name && data.values.name !== "") {
                SchemaResource.get({
                    tableName: data.values.name
                }, function(data1) {
                    if (data1.results) {
                        $scope.schema = data1.results.rows;
                        $scope.isSchema = (data1.results.rows && data1.results.rows.length > 0) ? true : false;
                        for (var t = 0; t < data1.results.rows.length; t++) {
                            if (data1.results.rows[t].$id$) {
                                $scope.isTraitId = true; 
                            }
                            if (data1.results.rows[t].type) {
                                $scope.isHiveSchema = true; 
                            }  
                            if($scope.isTraitId && $scope.isHiveSchema){
                                break;
                            }
                        } 
                    }
                });
            }
        });

        $scope.$on('show_lineage', function() {
            $scope.isLineage = true;
        });

        $scope.isNumber = angular.isNumber;
        $scope.isObject = angular.isObject;
        $scope.isString = angular.isString;
        $scope.isArray = angular.isArray;
        $scope.onActivate = function tabActivate(tabname) {
            $scope.$broadcast('render-lineage', {
                type: tabname,
                tableName: $scope.tableName,
                guid : $stateParams.id
            });
        };

        $scope.openAddTagHome = function(traitId) {
            $state.go('addTagDetails', {
                tId: traitId
            });
        };


        $scope.goDetails = function(id) {
            $state.go("details", {
                id: id
            });
        };

        $scope.goBack = function() {
            $window.history.back();
        };
    }
]);
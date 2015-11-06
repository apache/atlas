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

angular.module('dgc.details').controller('DetailsController', ['$window', '$scope', '$state', '$stateParams', 'DetailsResource',
    function($window, $scope, $state, $stateParams, DetailsResource) {

        $scope.tableName = false;
        $scope.isTable = false;

        DetailsResource.get({
            id: $stateParams.id
        }, function(data) {
            $scope.details = data;
            console.log(data);
            $scope.schemas = data;
            $scope.tableName = data.values.name;
            $scope.isTable = (typeof data.typeName != 'undefined' && data.typeName.toLowerCase().indexOf('table') != -1) ? true : false;
            $scope.onActivate('io');
        });

        $scope.isNumber = angular.isNumber;
        $scope.isObject = angular.isObject;
        $scope.isString = angular.isString;
        $scope.isArray = angular.isArray;
        $scope.onActivate = function tabActivate(tabname) {
            $scope.$broadcast('render-lineage', {
                type: tabname,
                tableName: $scope.tableName
            });
        };

        $scope.goDetails = function(id){
            $state.go("details", {
                id: id
            });
        };

        $scope.goBack = function() {
            $window.history.back();
        };
    }
]);

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

angular.module('dgc.tags.instance').controller('InstanceTagController', ['$scope', 'DetailsResource', '$stateParams', '$state',
    function($scope, DetailsResource, $stateParams, $state) {
        $scope.id = $stateParams.id;

        function getResourceData() {
            DetailsResource.get({
                id: $stateParams.id
            }, function(data) {

                    angular.forEach(data.traits, function(obj, trait) {
                        var pair_arr = [];
                        if (obj.values !== null && Object.keys(obj.values).length > 0) {
                            angular.forEach(obj.values, function(value, key) {
                                   var pair = key+":"+value;
                                   pair_arr.push(pair);                                
                            });
                            data.traits[trait].values = pair_arr.join(" | ");
                        } else {
                            data.traits[trait].values = 'NA';
                        }
                    });
                
                $scope.traitsList = data.traits;
            });
        }
        $scope.openAddTag = function() {
            $state.go('addTag', {
                id: $scope.id
            });
        };

        $scope.detachTag = function($event, name) {
            var r = confirm("Please confirm delete.");
            if (r === true) {
                DetailsResource.detachTag({
                    id: $stateParams.id,
                    tagName: name
                }, function(data) {
                    console.log("Detached Tag");
                    console.log(data);

                    if (data.requestId !== undefined && data.GUID === $stateParams.id && data.traitName === name) {
                        var curent = $event.currentTarget;
                        curent.parentElement.remove();
                        $(".popover").remove();
                    }
                });
            }
        };
        
        getResourceData();
        $scope.$on('refreshResourceData', getResourceData);
    }
]);

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

angular.module('dgc.search').controller('SearchController', ['$scope', '$location', '$http', '$state', '$stateParams', 'SearchResource', 'NotificationService',
    function($scope, $location, $http, $state, $stateParams, SearchResource, NotificationService) {

        $scope.types = ['table', 'column', 'db', 'view', 'loadprocess', 'storagedesc'];
        $scope.results = [];
        $scope.resultCount = 0;
        $scope.isCollapsed = true;
        $scope.currentPage = 1;
        $scope.itemsPerPage = 10;
        $scope.filteredResults = [];
        $scope.resultRows = [];

        $scope.$on('$stateChangeStart', function(event, toState) {
            if (toState.resolve) {
                $scope.loading = true;
            }
        });
        $scope.setPage = function(pageNo) {
            $scope.currentPage = pageNo;
        };
        $scope.search = function(query) {
            $scope.results = [];
            NotificationService.reset();
            $scope.limit = 4;
            $scope.searchMessage = 'searching...';

            $scope.$parent.query = query;
            SearchResource.search({
                query: query
            }, function searchSuccess(response) {
                $scope.querySuceess = true;
                $scope.resultCount = response.count;
                $scope.results = response.results;
                $scope.resultRows = $scope.results.rows;
                $scope.totalItems = $scope.resultCount;
                $scope.$watch('currentPage + itemsPerPage', function() {
                    var begin = (($scope.currentPage - 1) * $scope.itemsPerPage),
                        end = begin + $scope.itemsPerPage;
                    $scope.searchTypesAvailable = $scope.typeAvailable();
                    if ($scope.searchTypesAvailable) {
                        $scope.searchMessage = 'loading results...';
                        $scope.filteredResults = $scope.resultRows.slice(begin, end);
                        $scope.pageCount = function() {
                            return Math.ceil($scope.resultCount / $scope.itemsPerPage);
                        };
                        if ($scope.results.rows)
                            $scope.searchMessage = $scope.results.rows.length + ' results matching your search query ' + $scope.query + ' were found';
                        else
                            $scope.searchMessage = '0 results matching your search query ' + $scope.query + ' were found';
                        if ($scope.results.length < 1) {
                            NotificationService.error('No Result found', false);
                        }
                    } else {
                        $scope.searchMessage = '0 results matching your search query ' + $scope.query + ' were found';
                    }
                });
            }, function searchError(err) {
                NotificationService.error('Error occurred during executing search query, error status code = ' + err.status + ', status text = ' + err.statusText, false);
            });

        };

        $scope.typeAvailable = function() {

            if ($scope.results.dataType) {
                return $scope.types.indexOf($scope.results.dataType.typeName && $scope.results.dataType.typeName.toLowerCase()) > -1;
            }
        };
        $scope.doToggle = function($event, el) {
            this.isCollapsed = !el;
        };
        $scope.filterSearchResults = function(items) {
            var res = {};
            var count = 0;
            angular.forEach(items, function(value, key) {
                if (typeof value !== 'object') {
                    res[key] = value;
                    count++;
                }
            });

            $scope.keyLength = count;
            return res;
        };
        $scope.searchQuery = $location.search();
        $scope.query = ($location.search()).query;
        if ($scope.query) {
            $scope.search($scope.query);
        }
    }
]);

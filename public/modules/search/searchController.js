'use strict';

angular.module('dgc.search').controller('SearchController', ['$scope', '$state', '$stateParams', 'SearchResource', 'DetailsResource',
    function($scope, $state, $stateParams, SearchResource, DetailsResource) {

        $scope.types = ['hive_table', 'hive_database'];
        $scope.results = [];
        $scope.search = function(query) {
            SearchResource.get({
                query: query
            }, function(response) {
                $scope.results = [];
                angular.forEach(response.list, function(typeId) {
                    DetailsResource.get({
                        id: typeId
                    }, function(definition) {
                        $scope.results.push(definition);
                    });
                });
                $state.go('search.results', {
                    query: query
                });
            });
        };

        $scope.query = $stateParams.query;
        if ($scope.query) {
            $scope.search($stateParams.query);
        }

    }
]);

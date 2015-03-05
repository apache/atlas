'use strict';

angular.module('dgc.search').controller('SearchController', ['$scope', '$location', '$http', '$state', '$stateParams', 'SearchResource', 'NotificationService',
    function($scope, $location, $http, $state, $stateParams, SearchResource, NotificationService) {

        $scope.types = [];
        $scope.results = [];
        $scope.search = function(query) {
            $scope.results = [];
            NotificationService.reset();
            SearchResource.search($location.search(query).search(), function searchSuccess(response) {
                $scope.results = response;
                if ($scope.results.length < 1) {
                    NotificationService.error('No Result found', false);
                }
                $state.go('search.results', {}, {
                    location: false
                });
            }, function searchError(err) {
                NotificationService.error('Error occurred during executing search query, error status code = ' + err.status + ', status text = ' + err.statusText, false);
            });
        };

        $scope.typeAvailable = function() {
            return ['hive_table'].indexOf(this.result.type && this.result.type.toLowerCase()) > -1;
        };

        var urlParts = $location.url().split('?');
        $scope.query = urlParts.length > 1 ? urlParts[1] : null;
        if ($scope.query) {
            $scope.search($scope.query);
        }
    }
]);

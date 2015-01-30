'use strict';

angular.module('dgc.search').controller('SearchController', ['$scope', '$location', '$http', '$state', '$stateParams', 'SearchResource', 'NotificationService',
    function($scope, $location, $http, $state, $stateParams, SearchResource, NotificationService) {

        $scope.types = [];
        $scope.results = [];
        $scope.search = function(query) {
            $scope.results = [];
            NotificationService.reset();
            SearchResource.search($location.search(query).$$search, function(response) {
                $scope.results = response;
                if ($scope.results.length < 1) {
                    NotificationService.error("No Result found", false);
                }
                $state.go('search.results', {}, {
                    location: false
                });
            });
        };

        var urlParts = $location.$$url.split('?');
        $scope.query = urlParts.length > 1 ? urlParts[1] : null;
        if ($scope.query) {
            $scope.search($scope.query);
        }
    }
]);

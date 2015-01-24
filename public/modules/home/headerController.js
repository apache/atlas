'use strict';

angular.module('dgc.home').controller('HeaderController', ['$scope', function($scope) {

    $scope.menu = [{'state': 'search', 'title': 'Search'}];

    $scope.isCollapsed = true;
    $scope.isLoggedIn = function() {
        return true;
    };
}]);

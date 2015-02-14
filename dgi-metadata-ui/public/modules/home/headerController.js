'use strict';

angular.module('dgc.home').controller('HeaderController', ['$scope', function($scope) {

    $scope.menu = [];

    $scope.isCollapsed = true;
    $scope.isLoggedIn = function() {
        return true;
    };
}]);

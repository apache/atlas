'use strict';

angular.module('dgc.system').controller('HeaderController', ['$scope', function($scope) {

    $scope.menu = [];

    $scope.isCollapsed = true;
    $scope.isLoggedIn = function() {
        return true;
    };
}
]);

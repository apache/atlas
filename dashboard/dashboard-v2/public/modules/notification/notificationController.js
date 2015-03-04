'use strict';

angular.module('dgc.system.notification').controller('NotificationController', ['$scope', 'NotificationService',
    function($scope, NotificationService) {

        $scope.getNotifications = NotificationService.getAll;

        $scope.close = function(notification) {
            NotificationService.close(notification);
        };
    }
]);

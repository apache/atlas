'use strict';

angular.module('dgc.system.notification').service('NotificationService', ['$timeout', 'lodash', 'ColorCoding', function($timeout, _, colorCoding) {

    var notifications = [],
        service = {
            timeout: 2000,
            getAll: function() {
                return notifications;
            },
            reset: function() {
                notifications = [];
            },
            close: function(notification) {
                _.remove(notifications, notification);
            }
        };

    _.each(colorCoding, function(value, key) {
        service[key] = function(message) {
            var notification = message;
            if (_.isString(message)) {
                notification = {
                    message: message
                };
            }

            notification.message = notification.msg || notification.message;
            delete notification.msg;
            notification.type = value;
            notification.timeout = _.isUndefined(notification.timeout) ? true : notification.timeout;
            notifications.push(notification);

            if (notification.timeout) {
                $timeout(function() {
                    service.close(notification);
                }, angular.isNumber(notification.timeout) ? notification.timeout : service.timeout);
            }
        };
    });

    return service;
}]);

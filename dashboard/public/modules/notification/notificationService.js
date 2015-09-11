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
        service[key] = function(message, timeout) {
            var notification = message;
            if (_.isString(message)) {
                notification = {
                    message: message
                };
            }

            notification.message = notification.msg || notification.message;
            delete notification.msg;
            notification.type = value;
            notification.timeout = _.isUndefined(timeout) ? (_.isUndefined(notification.timeout) ? true : notification.timeout) : timeout;
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

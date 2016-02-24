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

angular.module('dgc', ['ngCookies',
    'ngResource',
    'ui.bootstrap',
    'ui.router',
    'dgc.system',
    'dgc.home',
    'dgc.about',
    'dgc.search',
    'dgc.navigation',
    'dgc.tags',
    'dgc.tags.instance',
    'dgc.tags.definition'
]);

angular.module('dgc.system', ['dgc.system.notification']);

angular.module('dgc').factory('lodash', ['$window',
    function($window) {
        return $window._;
    }
]).factory('d3', ['$window',
    function($window) {
        return $window.d3;
    }
]).factory('global', ['$window', '$location',
    function($window, $location) {
        return {
            user: $location.search()['user.name'],
            authenticated: !!$window.user,
            renderErrors: $window.renderErrors
        };
    }
]).factory('httpInterceptor', ['global', function(global) {
    return {
        'request': function(config) {
            if (config.url && (config.url.indexOf(baseUrl) === 0 || config.url.indexOf(baseUrl) === 0)) {
                config.params = config.params || {};
                config.params['user.name'] = global.user;
            }
            return config;
        }
    };
}]).config(['$httpProvider', function($httpProvider) {
    $httpProvider.interceptors.push('httpInterceptor');
}]).run(['$rootScope', 'global', 'notificationService', 'lodash', 'd3', function($rootScope, global, notificationService, lodash, d3) {
    var errors = global.renderErrors;
    if (angular.isArray(errors) || angular.isObject(errors)) {
        lodash.forEach(errors, function(err) {
            err = angular.isObject(err) ? err : {
                message: err
            };
            err.timeout = false;
            notificationService.error(err);
        });
    } else {
        if (errors) {
            errors.timeout = false;
            notificationService.error(errors);
        }
    }
    $rootScope.$on('$stateChangeStart', function() {
        d3.selectAll('.d3-tip').remove();
    });

    $rootScope.updateTags = function(added, obj) {
        if (added) {
            $rootScope.$broadcast('add_Tag', obj);
        }
    };

    $rootScope.loadTraits = function() {
        $rootScope.$broadcast('load_Traits');
    };

    $rootScope.$on('$stateChangeSuccess', function(evt, to, toParams, from) {
        if (from.name !== '' && to.name === 'search' && to.name !== from.name && typeof to.parent === 'undefined' && typeof from.parent === 'undefined') {
            $rootScope.loadTraits();
        } else if (from.name === '' && to.name === 'search') {
            $rootScope.loadTraits();
        }

        if (typeof to.parent === 'undefined') {
            if (to.name !== 'search') {
                $rootScope.leftNav = true;
            } else {
                $rootScope.leftNav = false;
            }
        }
    });

}]);

'use strict';

angular.module('dgc', ['ngCookies',
    'ngResource',
    'ui.bootstrap',
    'ui.router',
    'dgc.system',
    'dgc.home',
    'dgc.search'
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
]).factory('Global', ['$window',
    function($window) {
        return {
            user: $window.user,
            authenticated: !!$window.user,
            renderErrors: $window.renderErrors
        };
    }
]).run(['$rootScope', 'Global', 'NotificationService', 'lodash', 'd3', function($rootScope, Global, NotificationService, lodash, d3) {
    var errors = Global.renderErrors;
    if (angular.isArray(errors) || angular.isObject(errors)) {
        lodash.forEach(errors, function(err) {
            err = angular.isObject(err) ? err : {
                message: err
            };
            err.timeout = false;
            NotificationService.error(err);
        });
    } else {
        errors.timeout = false;
        NotificationService.error(errors);
    }
    $rootScope.$on('$stateChangeStart', function() {
        d3.selectAll('.d3-tip').remove();
    });
}]);

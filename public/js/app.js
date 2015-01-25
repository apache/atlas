'use strict';

angular.module('dgc', ['ngCookies',
    'ngResource',
    'ui.bootstrap',
    'ui.router',
    'dgc.system',
    'dgc.home',
    'dgc.search',
    'dgc.details'
]);

angular.module('dgc.system', ['dgc.system.notification']);

angular.module('dgc').factory('lodash', ['$window',
    function($window) {
        return $window._;
    }
]).factory('Global', ['$window',
    function($window) {
        return {
            user: $window.user,
            authenticated: !!$window.user,
            renderErrors: $window.renderErrors
        };
    }
]).run(function(Global, NotificationService, lodash) {
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
});

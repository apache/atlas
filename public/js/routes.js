'use strict';

//Setting up route
angular.module('dgc').config(['$stateProvider', '$urlRouterProvider',
    function($stateProvider, $urlRouterProvider) {

        // For unmatched routes:
        $urlRouterProvider.otherwise('/');

        // states for my app
        $stateProvider.state('home', {
            url: '/',
            templateUrl: '/modules/home/views/home.html',
            data: {
                isSecured: false
            }
        });
    }
]);

//Setting HTML5 Location Mode
angular.module('dgc').config(['$locationProvider',
    function($locationProvider) {
        $locationProvider.hashPrefix('!');
    }
]);

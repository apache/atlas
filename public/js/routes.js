'use strict';

//Setting up route
angular.module('dgc').config(['$locationProvider', '$urlRouterProvider', function($locationProvider, $urlRouterProvider) {
    $locationProvider.hashPrefix('!');
    // For unmatched routes:
    $urlRouterProvider.otherwise('/');
    $urlRouterProvider.when('/', '/search');
}]);
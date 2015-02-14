'use strict';

//Setting up route
angular.module('dgc.home.routes', []).config(['$stateProvider',
    function($stateProvider) {

        // states for my app
        $stateProvider.state('home', {
            url: '/',
            templateUrl: '/modules/home/views/home.html'
        });
    }
]);

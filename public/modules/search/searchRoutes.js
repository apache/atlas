'use strict';

//Setting up route
angular.module('dgc.search').config(['$stateProvider',
    function($stateProvider) {

        // states for my app
        $stateProvider.state('search', {
            url: '/search',
            templateUrl: '/modules/search/views/search.html'
        }).state('search.results', {
            url: '/?',
            templateUrl: '/modules/search/views/searchResult.html'
        });
    }
]);

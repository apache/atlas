'use strict';

angular.module('dgc.details').config(['$stateProvider',
    function($stateProvider) {

        // states for my app
        $stateProvider.state('details', {
            url: '/details/:id',
            templateUrl: '/modules/details/views/details.html'
        });
    }
]);

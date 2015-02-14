'use strict';

angular.module('dgc.details').factory('DetailsResource', ['$resource', function($resource) {
    return $resource('/api/metadata/entities/definition/:id', {}, {
        get: {
            method: 'GET',
            transformResponse: function(data) {
                if (data) {
                    return angular.fromJson(data.definition);
                }
            },
            responseType: 'json'
        }
    });
}]);

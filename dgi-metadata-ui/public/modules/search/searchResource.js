'use strict';

angular.module('dgc.search').factory('SearchResource', ['$resource', function($resource) {
    return $resource('/api/metadata/discovery/search/fulltext', {}, {
        search: {
            'method': 'GET',
            'responseType': 'json',
            'isArray': true,
            'transformResponse': function(data) {
                var results = [];
                angular.forEach(data && data.vertices, function(val) {
                    results.push(val);
                });
                return results;
            }
        }
    });
}]);

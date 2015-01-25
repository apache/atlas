'use strict';

angular.module('dgc.search').factory('SearchResource', ['$resource', function($resource) {
    return $resource('/api/metadata/entities/list/:query', {
        'query': '@id'
    });
}]);

'use strict';

angular.module('dgc.lineage').factory('LineageResource', ['$resource', function($resource) {
    return $resource('/api/metadata/discovery/search/relationships/:id', {
        depth: 3,
        edgesToFollow: 'HiveLineage.sourceTables.0,HiveLineage.sourceTables.1,HiveLineage.sourceTables.2,HiveLineage.tableName'
    });
}]);

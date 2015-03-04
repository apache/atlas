'use strict';

angular.module('dgc.details').controller('DetailsController', ['$scope', '$stateParams', 'DetailsResource',
    function($scope, $stateParams, DetailsResource) {

        $scope.details = DetailsResource.get({
            id: $stateParams.id
        });

        $scope.isString = angular.isString;
    }
]);

/**
 * Created by dbhimineni on 5/27/2015.
 */

angular.module('dgc.search').directive(
    "myDirective", function() {
        return {
            restrict: 'EA',
            template: '<a href="javascript: void(0);" button-toggle toggle="isCollapsed" class="show-more" ng-click="isCollapsed = !isCollapsed">..show more</a>',
            link: function($scope, element, attrs){
                $scope.isCollapsed = true;
                console.log($scope.isCollapsed);
            },
            transclude: true,
            scope: {}
        }
    });
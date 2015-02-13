var DgcApp = angular.module('DgcApp', [
  'ngRoute',
  'DgcControllers',
  'ui.bootstrap'
]);

DgcApp.config(['$routeProvider', function($routeProvider) {
  $routeProvider.
  when('/Search', {
    templateUrl: 'partials/search.html',
    controller: 'ListController'
  }).
  when('/Search/:Id', {
    templateUrl: 'partials/wiki.html',
    controller: 'DefinitionController'
  }).
  otherwise({
    redirectTo: '/Search'
  });
}]);

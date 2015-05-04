/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var DgcApp = angular.module('DgcApp', [
    'ui.router',
  'DgcControllers',
  'ui.bootstrap'
]);

DgcApp.config(['$urlRouterProvider','$stateProvider', function($urlRouterProvider, $stateProvider) {
    $urlRouterProvider.otherwise('Home');
   $stateProvider
  .state('Home', {
    url: 'Home',
	 views: {
	  'content': {
    templateUrl: 'partials/search.html',
    controller: 'ListController'
			}
		}
  })
       .state('Search', {
           url: '/Search/:searchid',
           views: {
               'content': {
                   templateUrl: 'partials/search.html',
                   controller: 'ListController'
               }
           }
       })
  .state('details', {
    url: 'details/:Id',
	 views: {
	  'content': {
    templateUrl: 'partials/wiki.html',
    controller: 'DefinitionController'
	  }
    }
  });
}]);

DgcApp.run(['$window', '$rootScope',
function ($window ,  $rootScope) {
  $rootScope.goBack = function(){
    $window.history.back();
    return false;
  }
}]);
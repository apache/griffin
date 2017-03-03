/*
	Copyright (c) 2016 eBay Software Foundation.
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
define(['./module'], function (controllers) {
    'use strict';
    controllers.controller('NavCtrl', ['$scope', '$http', '$config', '$location', '$cookies', '$rootScope', '$timeout', function ($scope, $http, $config, $location, $cookies, $rootScope, $timeout) {
      console.log('Navigation controller');
      var cookieObjs = $cookies.getAll();
      if(cookieObjs.ntAccount){
        $rootScope.ntAccount = cookieObjs.ntAccount;
        $rootScope.fullName = cookieObjs.fullName;
      }else{
        window.location.replace('/login/login.html');
      }

      var adminGroup = ['lzhixing', 'yosha', 'wenzhao', 'aiye', 'lshao'];
	    $rootScope.adminAccess = (adminGroup.indexOf($scope.ntAccount)!=-1);

      // $scope.test = 'test';
      $scope.isActive = function (route) {
          return $location.path().indexOf(route) == 0;
          //return $location.path() == route;
      }

      var timer = null;
      $("#searchBox").off("keyup");
      $("#searchBox").on("keyup", function(event){
        if(timer){
          $timeout.cancel(timer);
        }
        var searchValue = event.target.value;

        if(searchValue != undefined){
            timer = $timeout(function(){
              $rootScope.keyword = searchValue.trim();
            }, 500);
        }
      });

      $scope.$on('$routeChangeStart', function(next, current) {
        $("#searchBox").val('');
        $rootScope.keyword = '';
      });

      $scope.logout = function(){
        $rootScope.ntAccount = undefined;
        $rootScope.fullName = undefined;
        $cookies.remove('ntAccount');
        $cookies.remove('fullName');
        window.location.replace('/login/login.html');
      }

    }]);
});

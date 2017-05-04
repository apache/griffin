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
    controllers.controller('CreateRule0Ctrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route',  function ($scope, $http, $config, $location, toaster, $timeout, $route) {
      console.log('Create rule 0 controller');
      $scope.publishURL = document.location.origin;

      $scope.click = function(type){
        $location.path('/createrule-' + type);
      }
      $scope.$on('$viewContentLoaded', function(){
        $scope.$emit('initReq');
        resizeWindow();
      });

      $scope.$on('resizeHandler', function(e) {
        if($route.current.$$route.controller == 'CreateRule0Ctrl'){
            console.log('createrule0 resize');
            resizeWindow();
        }
      });

      function resizeWindow(){
        $('#panel-2 >.panel-body').css({height: $('#panel-1 >.panel-body').outerHeight() + $('#panel-1 >.panel-footer').outerHeight() - $('#panel-2 >.panel-footer').outerHeight()});
        $('#panel-4 >.panel-body').css({height: $('#panel-3 >.panel-body').outerHeight() + $('#panel-3 >.panel-footer').outerHeight() - $('#panel-4 >.panel-footer').outerHeight()});
      }
    }]);
});

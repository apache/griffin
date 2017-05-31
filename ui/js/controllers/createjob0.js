define(['./module'], function (controllers) {
    'use strict';
    controllers.controller('CreateJob0Ctrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route',  function ($scope, $http, $config, $location, toaster, $timeout, $route) {
      console.log('Create job 0 controller');
      $scope.publishURL = document.location.origin;

      $scope.click = function(type){
        $location.path('/createjob-' + type);
      }
      $scope.$on('$viewContentLoaded', function(){
        $scope.$emit('initReq');
        resizeWindow();
      });

      $scope.$on('resizeHandler', function(e) {
        if($route.current.$$route.controller == 'CreateJob0Ctrl'){
            console.log('createjob0 resize');
            resizeWindow();
        }
      });

      function resizeWindow(){
        $('#panel-2 >.panel-body').css({height: $('#panel-1 >.panel-body').outerHeight() + $('#panel-1 >.panel-footer').outerHeight() - $('#panel-2 >.panel-footer').outerHeight()});
        $('#panel-4 >.panel-body').css({height: $('#panel-3 >.panel-body').outerHeight() + $('#panel-3 >.panel-footer').outerHeight() - $('#panel-4 >.panel-footer').outerHeight()});
      }
    }]);
});

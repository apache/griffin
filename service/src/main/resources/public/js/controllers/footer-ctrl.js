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
    controllers.controller('FooterCtrl', ['$scope', '$http', '$config', '$location', '$cookies', '$interval', function ($scope, $http, $config, $location, $cookies, $interval) {
      console.log('Footer controller');
      $scope.timestamp = new Date();

      var start = 0, number = 3;
      retrieveNotifications();
      $interval(retrieveNotifications, 60000);

      // $interval(function(){
      //   if($scope.allNotifications){
      //     var length = $scope.allNotifications.length;
      //     if(length == 0){
      //       return;
      //     }else if(length <= number){
      //       $scope.notifications = $scope.allNotifications;
      //       return;
      //     }
      //     var index = start%length;
      //     var end = index + number;

      //     $scope.notifications = $scope.allNotifications.slice(index, end);

      //     if(end > length){
      //       $scope.notifications = $scope.notifications.concat($scope.allNotifications.slice(0, end-length ));
      //     }
      //     start ++;
      //   }

      // }, 2000);

      function retrieveNotifications(){
        var notificationUrl = $config.uri.getnotifications;
        $http.get(notificationUrl).success(function(data) {
          // $scope.allNotifications = data.slice(0, 15);
          $scope.notifications = data.slice(0, 3);
        });
      }

    }]);
});

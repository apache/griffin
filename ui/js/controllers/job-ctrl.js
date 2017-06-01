/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
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
    controllers.controller('JobCtrl', ['$scope', '$http', '$config', '$location', '$timeout', '$route', 'toaster', '$filter', function ($scope, $http, $config, $location, $timeout, $route, toaster, $filter) {
      console.log('job controller');
      console.log($scope.ntAccount);
      var allJobs = $config.uri.allJobs;
      var ts = null;
      var start = 0;
      var number = 10;
      var originalRowCollection = undefined;

      $scope.paging = function(tableState){
        console.log(tableState);
        ts = tableState;

        // tableState.pagination.numberOfPages = $scope.rowCollection.length/10 + 1;
        start = tableState.pagination.start || 0;
        number = tableState.pagination.number || 10;

        if(start == 0 && !$scope.rowCollection){
         $http.get(allJobs).success(function(data) {
           originalRowCollection = angular.copy(data);
           $scope.rowCollection = angular.copy(data);

           $scope.displayed = $scope.rowCollection.slice(start, start+number);
           tableState.pagination.numberOfPages = Math.ceil($scope.rowCollection.length/number);
         });
        }else{
         $scope.displayed = $scope.rowCollection.slice(start, start+number);
        }
      };




      $scope.remove = function remove(row) {
//        var getJobUrl = $config.uri.getJob + '/' +row.name;
//        $http.get(getJobUrl).success(function(data){
//  			  $scope.deletedRow = data;
//
//  		  });
        $scope.deletedRow = row;
        $scope.deletedBriefRow = row;
        $('#deleteJobConfirmation').modal('show');
      }

      $scope.confirmDelete = function(){
        var row =   $scope.deletedBriefRow;
        var deleteModelUrl = $config.uri.deleteJob + row.groupName+'/jobs/'+row.jobName;
        $http.delete(deleteModelUrl).success(function(){

          var index = $scope.rowCollection.indexOf(row);
          $scope.rowCollection.splice(index, 1);

          index = $scope.displayed.indexOf(row);
          $scope.displayed.splice(index, 1);

          $('#deleteJobConfirmation').modal('hide');

        }).error(function(data, status){
          toaster.pop('error', 'Error when deleting record', data);
        });
      }



      $scope.edit = function edit() {
      }


      $scope.$on('$viewContentLoaded', function() {
        $scope.$emit('initReq');
      });


/*
       function createRowCollection(){
         var data = [];
         for (var j = 0; j < 22; j++) {
              data.push(createRandomItem());
          }

          return data;
       }

       function createRandomItem() {
         var nameList = ['ViewItem', 'Search', 'BidEvent', 'user_dna', 'LAST_ITEMS_VIEWED'],
             systemList = ['Bullseye', 'PDS', 'GPS', 'IDLS', 'Hadoop'],
             dateList = ['2016-03-10', '2016-03-12', '2016-03-15', '2016-03-19', '2016-03-20'],
             statusList = [0, 1, 2];

           var
               name = nameList[Math.floor(Math.random() * 5)],
               system = Math.floor(Math.random() * 7),
               type = Math.floor(Math.random() * 4),
               description = 'Only for demo purpose',
               createDate = dateList[Math.floor(Math.random() * 5)],
               status = Math.floor(Math.random() * 3);

           return{
               name: name,
               system: system,
               type: type,
               description: description,
               createDate: createDate,
               status:status
           };
       }
*/
    }]);
});
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
    controllers.controller('RuleCtrl', ['$scope', '$http', '$config', '$location', '$timeout', '$route', 'toaster', '$filter', function ($scope, $http, $config, $location, $timeout, $route, toaster, $filter) {
      console.log('rule controller');

      $scope.statusList = $filter('strarr')('modelstatus');
      $scope.systemList = $filter('strarr')('modelsystem');
      $scope.typeList = $filter('strarr')('modeltype');

      var allModels = $config.uri.allModels;
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
         $http.get(allModels).success(function(data) {
           data.sort(function(a,b){
             return -(a.createDate - b.createDate);
           });
           originalRowCollection = angular.copy(data);
           $scope.rowCollection = angular.copy(data);

           $scope.displayed = $scope.rowCollection.slice(start, start+number);
           tableState.pagination.numberOfPages = Math.ceil($scope.rowCollection.length/number);
         });
        }else{
         $scope.displayed = $scope.rowCollection.slice(start, start+number);
        }
      };

      var include = function(keyword, str) {
        if(keyword == undefined || keyword == null){
          return true;
        } else if(str == undefined || str == null){
          return false;
        } else{
          var value = keyword.trim().toLowerCase();
          return str.trim().toLowerCase().includes(value);
        }
      };

      var findValue = function(keyword, assetItem) {
        var date = $filter('date')(assetItem.createDate, 'M/d/yy h:mm a', '-0700')
        return include(keyword, assetItem.name)
          || include(keyword, assetItem.description)
          || include(keyword, assetItem.owner)
          || include(keyword, $filter('strmap')(assetItem.system, $scope.systemList))
          || include(keyword, $filter('strmap')(assetItem.status, $scope.statusList))
          || include(keyword, $filter('strmap')(assetItem.type, $scope.typeList))
          || include(keyword, date);
      };

      $scope.$watch('keyword', function(newValue){
        if(originalRowCollection){
          start = 0;
          if(newValue == undefined || newValue == ''){
            $scope.rowCollection = angular.copy(originalRowCollection);
          }else{
            var result = [];
            for (var i = 0; i < originalRowCollection.length; i++) {
              var item = originalRowCollection[i];
              if(findValue(newValue, item)){
                result.push(item);
              }
            };
            $scope.rowCollection = angular.copy(result);
          }
          $scope.displayed = $scope.rowCollection.slice(start, start+number);
          ts.pagination.numberOfPages = Math.ceil($scope.rowCollection.length/number);
        }
      });


      $scope.remove = function remove(row) {
        var getModelUrl = $config.uri.getModel + '/' +row.name;
        $http.get(getModelUrl).success(function(data){
  			  $scope.deletedRow = data;

  		  });
        // $scope.deletedRow = row;
        $scope.deletedBriefRow = row;
        $('#deleteConfirmation').modal('show');
      }

      $scope.confirmDelete = function(){
        var row =   $scope.deletedBriefRow;
        var deleteModelUrl = $config.uri.deleteModel + '/' + row.name;
        $http.delete(deleteModelUrl).success(function(){

          var index = $scope.rowCollection.indexOf(row);
          $scope.rowCollection.splice(index, 1);

          index = $scope.displayed.indexOf(row);
          $scope.displayed.splice(index, 1);

          $('#deleteConfirmation').modal('hide');

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

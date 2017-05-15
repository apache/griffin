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
define(['./module'], function(controllers) {
    'use strict';
    controllers.controller('MetricsCtrl', ['$scope', '$http', '$config', '$location', '$routeParams', '$timeout', '$compile', '$route', '$barkChart', '$rootScope',function($scope, $http, $config, $location, $routeParams, $timeout, $compile, $route, $barkChart, $rootScope) {
        console.log('Route parameter: ' + $routeParams.sysName);

        var echarts = require('echarts');

        pageInit();

        function pageInit() {
          $scope.$emit('initReq');
          $scope.orgs = [];
          var url_dashboard = $config.uri.dashboard ;
          var url_organization = $config.uri.organization;

          $http.get(url_organization).success(function(res){
                var orgNode = null;
                angular.forEach(res, function(value,key) {
                orgNode = new Object();
                $scope.orgs.push(orgNode);
                orgNode.name = key;
                orgNode.assetMap = value;
              });

              console.log($scope.orgs);
              $scope.originalOrgs = angular.copy($scope.orgs);


              $http.get(url_dashboard).success(function(data){
                  $scope.dashboard = data;
                  angular.forEach(data.hits.hits, function(sys) {
                      var chartData = sys._source;
                      chartData.sortData = function(a,b){
                          return a.tmst - b.tmst;
                      }
                  });
                  $scope.originalData = angular.copy(data);
                  $scope.myData = angular.copy($scope.originalData.hits.hits);
                  $scope.metricName = [];
                  for(var i = 0;i<$scope.myData.length;i++){
                      $scope.metricName.push($scope.myData[i]._source.name);
                  }
                  $scope.metricNameUnique = [];
                  $scope.dataData = [];
                  angular.forEach($scope.metricName,function(name){
                      if($scope.metricNameUnique.indexOf(name) === -1){
                          $scope.dataData[$scope.metricNameUnique.length] = new Array();
                          $scope.metricNameUnique.push(name);
                      }
                  });
                  $scope.numberOfName = $scope.metricNameUnique.length;

                  for(var i = 0;i<$scope.myData.length;i++){
                      for(var j = 0 ;j<$scope.metricNameUnique.length;j++){
                          if($scope.myData[i]._source.name==$scope.metricNameUnique[j]){
                              $scope.dataData[j].push($scope.myData[i]);
                          }
                      }
                  }

                  $scope.original = angular.copy($scope.dataData);
                  $timeout(function() {
                    console.log($scope.dataData);
//                    $scope.$apply();
                    redraw($scope.dataData);
                    console.log("paint over");
                    console.log($scope.dataData);
                  },1000,false);


//                  $timeout(function(){
//
//                  })
//                    setInterval(function(){
//                        console.log($scope.dataData);
//                    },1000)
//                  setTimeout(function(){
//                      redraw($scope.dataData);
//                  });
//                  $scope.apply();
              });
          });


//          $http.post(url_dashboard, {"query": {"match_all":{}},"size":1000}).success(function(res) {

        }

        $scope.$watch('selectedOrgIndex', function(newValue){
          console.log(newValue);
        });

        var redraw = function(data) {
            if($scope.selectedOrgIndex==='' || $scope.selectedOrgIndex == undefined)
                $scope.orgs = angular.copy($scope.originalOrgs);
            else
                $scope.orgs = $scope.originalOrgs[$scope.selectedOrgIndex];

            $scope.chartHeight = $('.chartItem:eq(0)').width()*0.8+'px';

            angular.forEach(data, function(sys, parentIndex) {
                var tmp = document.getElementById('thumbnail'+parentIndex);
                var org = $('#thumbnail'+parentIndex).parent().parent().parent().prev().text();

                for(var i = 0;i<$scope.orgs.length;i++){
                    if($scope.orgs[i].name==org.trim() && $scope.orgs[i].assetMap.indexOf(sys[0]._source.name)!== -1){
                        tmp.style.width = $('#thumbnail'+parentIndex).parent().width()+'px';
                        tmp.style.height = $scope.chartHeight;
                        var abcChart = echarts.init(tmp, 'dark');
                        abcChart.setOption($barkChart.getOptionThum(sys));
                    }
//                    else $('#thumbnail'+parentIndex).parent().parent().empty();
                }
             });
             console.log($scope.dataData);
        }

        $scope.assetOptions = [];

        $scope.changeOrg = function(data) {
        $scope.selectedOrgIndex = data;
        var url_organization = $config.uri.organization;
          $scope.selectedAssetIndex = undefined;
          $scope.assetOptions = [];
          $scope.dataData = [];
          if($scope.selectedOrgIndex === ""){
            $scope.orgs = angular.copy($scope.originalOrgs);
            $scope.dataData = angular.copy($scope.original);
            $timeout(function() {
               redraw($scope.dataData);
            }, 0);
          } else {
            $scope.orgs = $scope.originalOrgs[data];
//            $http.get(url_organization+'/'+$scope.orgs.name).success(function(res){
                $scope.assetOptions = $scope.orgs.assetMap;
                angular.forEach($scope.original,function(data){
                    if($scope.assetOptions.indexOf(data[0]._source.name)!= -1){
                        $scope.dataData.push(data);
                    }
                });
                $timeout(function() {
                  redraw($scope.dataData);
                }, 0);
//            });
          }
        };

        $scope.changeAsset = function() {
          var url_organization = $config.uri.organization;
          $scope.dataData = [];
          if($scope.selectedOrgIndex == ""){
            $scope.dataData = angular.copy($scope.original);
            $timeout(function() {
              redraw($scope.dataData);
            }, 0);
          } else {
//           $http.get(url_organization+'/'+$scope.orgs[$scope.selectedOrgIndex].name).success(function(res){
               angular.forEach($scope.original,function(data){
                   if($scope.originalOrgs[$scope.selectedAssetIndex].measureName.indexOf(data[0]._source.name)!= -1){
                       $scope.dataData.push(data);
                   }
               });
               $timeout(function() {
                   redraw($scope.dataData);
               }, 0);
//           });
          }
        }

        $scope.$on('resizeHandler', function() {
          if($route.current.$$route.controller == 'MetricsCtrl') {
            console.log('metrics resize');
            redraw($scope.dataData);
          }
        });

        /*click the chart to be bigger*/
        $scope.showBig = function(t){
            $rootScope.showBigChart($barkChart.getOptionBig(t));
        }

        $scope.getSample = function(item) {
          $rootScope.$broadcast('downloadSample', item.name);
        };


    }
    ]);
});

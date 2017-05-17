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
    controllers.controller('MetricsCtrl', ['$scope', '$http', '$config', '$location', '$routeParams', '$timeout', '$compile', '$route', '$barkChart', '$rootScope', function($scope, $http, $config, $location, $routeParams, $timeout, $compile, $route, $barkChart, $rootScope) {
        console.log('Route parameter: ' + $routeParams.sysName);

        var echarts = require('echarts');

        pageInit();

        function pageInit() {
          $scope.$emit('initReq');

          var url_dashboard = $config.uri.dashboard + ($routeParams.sysName?('/'+$routeParams.sysName):'');
          var url_organization = $config.uri.organization;
          $http.get(url_organization).success(function(res){
              $scope.orgs = [];
              var orgNode = null;
              angular.forEach(res, function(sys) {
              orgNode = new Object();
              $scope.orgs.push(orgNode);
              orgNode.name = sys;
              orgNode.assetMap = {};
              });
          });

          $http.get(url_dashboard, {  "query": {
                                      "bool":{
                                      "filter":[
                                              {"term" : {"name": "test" }}
                                      ]
                                      }
                                      },cache:true}).success(function(res) {
            $scope.dashboard = res;
            angular.forEach(res.hits.hits, function(sys) {
                var chartData = sys._source;
                chartData.sortData = function(a,b){
                    return a.tmst - b.tmst;
                }
            });
            $scope.originalData = angular.copy(res);
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

            $scope.orgMap = [];

            angular.forEach($scope.metricNameUnique,function(model,index){
                $http.get(url_organization+'/'+model).success(function(res){
                    var orgmap = new Object();
                    orgmap.name = model;
                    orgmap.org = res;
                    $scope.orgMap.push(orgmap);
                })
            })

            for(var i = 0;i<$scope.myData.length;i++){
                for(var j = 0 ;j<$scope.metricNameUnique.length;j++){
                    if($scope.myData[i]._source.name==$scope.metricNameUnique[j]){
                        $scope.dataData[j].push($scope.myData[i]);
                    }
                }
            }
            $scope.original = angular.copy($scope.dataData);
//            angular.forEach($scope.dataData,function(data){
//
//            })
//            if($routeParams.sysName && $scope.originalData && $scope.originalData.length > 0){
//              for(var i = 0; i < $scope.originalData.length; i ++){
//                if($scope.originalData[i].name == $routeParams.sysName){
//                  $scope.selectedOrgIndex = i;
//                  $scope.changeOrg();
//                  $scope.orgSelectDisabled = true;
//                  break;
//                }
//
//              }
//            }


            $timeout(function() {
              redraw($scope.dataData);
            });

          });
        }
        $scope.$watch('selectedOrgIndex', function(newValue){
          console.log(newValue);
        });

        var redraw = function(data) {
          $scope.chartHeight = $('.chartItem:eq(0)').width()*0.8+'px';
            angular.forEach(data, function(sys, parentIndex) {
                var tmp = document.getElementById('thumbnail'+parentIndex);
                tmp.style.width = $('#thumbnail'+parentIndex).parent().width()+'px';
                tmp.style.height = $scope.chartHeight;
                var abcChart = echarts.init(tmp, 'dark');
                abcChart.setOption($barkChart.getOptionThum(sys));
            });}

        $scope.assetOptions = [];

        $scope.changeOrg = function(data) {
        $scope.selectedOrgIndex = data;
        var url_organization = $config.uri.organization;
          $scope.selectedAssetIndex = undefined;
          $scope.assetOptions = [];
          $scope.dataData = [];
          if($scope.selectedOrgIndex === ""){
            $scope.dataData = angular.copy($scope.original);
            $timeout(function() {
               redraw($scope.dataData);
            }, 0);
          } else {
            $http.get(url_organization+'/'+$scope.orgs[data].name).success(function(res){
                $scope.assetOptions = res;
                angular.forEach($scope.original,function(data){
                    if(res.indexOf(data[0]._source.name)!= -1){
//                        $scope.dataData[$scope.dataData.length-1] = new Array();
                        $scope.dataData.push(data);
                    }
                });
                $timeout(function() {
                  redraw($scope.dataData);
                }, 0);
            });

          }
          console.log($scope.dataData);
          console.log(typeof($scope.dataData));
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
           $http.get(url_organization+'/'+$scope.orgs[$scope.selectedOrgIndex].name).success(function(res){
               angular.forEach($scope.original,function(data){
                   if(res[$scope.selectedAssetIndex].indexOf(data[0]._source.name)!= -1){
                       $scope.dataData.push(data);
                   }
               });
//               if($scope.selectedAssetIndex != undefined && $scope.selectedAssetIndex != ''){
//                 var asset = $scope.assetOptions[$scope.selectedAssetIndex];
//                 var oldMetrics = angular.copy($scope.dataData);
//                 angular.forEach(oldMetrics, function(sys,index) {
////                   $scope.dataData[index]=[];
//                   if(sys[0]._source.name == asset) {
//                     $scope.dataData.push(sys);
//                   }
//                 });
//               }
               console.log($scope.dataData);
               $timeout(function() {
                   redraw($scope.dataData);
               }, 0);
           });
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

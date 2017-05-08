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
          //          var url_organization = $config.uri.organization;
          //          $http.get(url_organization).success(function(res){
          //              $scope.orgs = [];
          //              var orgNode = null;
          //              angular.forEach(res, function(sys) {
          //              orgNode = new Object();
          //              $scope.orgs.push(orgNode);
          //              orgNode.name = sys;
          //              });
          //          });

          $http.get(url_dashboard, {  "query": {
                                      "bool":{
                                      "filter":[
                                              {"term" : {"name": "bevssoj" }}
                                      ]
                                      }
                                      },cache:true}).success(function(res) {
            $scope.dashboard = res;
            // console.log(res);
            console.log(res);
            console.log(res.hits);

            angular.forEach(res.hits.hits, function(sys) {
//              orgNode.assetMap = {};
                console.log(sys);
//                angular.forEach(sys, function(metric) {
//                if(!metric.assetName){
//                  metric.assetName = 'unknown';
//                }
//                if(Object.getOwnPropertyNames(orgNode.assetMap).indexOf(metric.assetName) == -1){//not existed
//                  orgNode.assetMap[metric.assetName] = {};
//
//                }
                var chartData = sys._source;
                chartData.sortData = function(a,b){
                    return a.tmst - b.tmst;
                }
//                orgNode.assetMap[metric.assetName].details = chartData;
//              });

//                $scope.orgs.push(orgNode);
            });
            $scope.originalData = angular.copy(res);
            // console.log($scope.originalData);
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
              redraw($scope.dashboard);
            });

          });
        }


        $scope.$watch('selectedOrgIndex', function(newValue){
          console.log(newValue);
        });

        var redraw = function(data) {
           console.log(data);

          $scope.chartHeight = $('.chartItem:eq(0)').width()*0.8+'px';
                var tmp = document.getElementById('abc-bevssoj');
                tmp.style.width = $('#abc-bevssoj').parent().width()+'px';
                tmp.style.height = $scope.chartHeight;
                var abcChart = echarts.init(tmp, 'dark');
                abcChart.setOption($barkChart.getOptionThum(data));

//            angular.forEach(data, function(sys, parentIndex) {
//                var parentIndex = parentIndex
//                angular.forEach(data.hits.hits, function(metric, index) {
//                console.log(sys);
//                console.log($('#abc'+parentIndex));

//                $('#abc-'+index).style.width = $('#abc-'+index).parent().width()+'px';

//                $( '#abc-'+index ).css( "width", $('#abc-'+index).parent().width()+'px' );
//                $( '#abc-'+index ).css( "height", $scope.chartHeight );
//                angular.element("#abc-"+index).css( "width", $('#abc-'+index).parent().width()+'px' );
//                angular.element("#abc-"+index).css( "height", $scope.chartHeight );
//                $('#abc-'+index).style.height = $scope.chartHeight;


//                });

        }

        $scope.assetOptions = [];

        $scope.changeOrg = function() {
          $scope.selectedAssetIndex = undefined;
          $scope.assetOptions = [];
          $scope.dashboard = [];
          if($scope.selectedOrgIndex === ""){
            $scope.dashboard = angular.copy($scope.originalData);
          } else {
            var org = angular.copy($scope.originalData[$scope.selectedOrgIndex]);
            $scope.dashboard.push(org);
            angular.forEach(org.metrics, function(metric, index) {
              if($scope.assetOptions.indexOf(metric.assetName) == -1) {
                $scope.assetOptions.push(metric.assetName);
              }
            });
          }
          // redraw($scope.dashboard);
          $timeout(function() {
              redraw($scope.dashboard);
            }, 0);
        };

        $scope.changeAsset = function() {
          $scope.dashboard = [];
          if($scope.selectedOrgIndex == ""){
            $scope.dashboard = angular.copy($scope.originalData);
          } else {
            var org = angular.copy($scope.originalData[$scope.selectedOrgIndex]);
            $scope.dashboard.push(org);
          }
          if($scope.selectedAssetIndex != undefined && $scope.selectedAssetIndex != ''){
            var asset = $scope.assetOptions[$scope.selectedAssetIndex];
            angular.forEach($scope.dashboard, function(sys) {
              var oldMetrics = sys.metrics;
              sys.metrics = [];
              angular.forEach(oldMetrics, function(metric, index) {
                if(metric.assetName == asset) {
                  sys.metrics.push(metric);
                }
              });
            });
          }
          $timeout(function() {
              redraw($scope.dashboard);
            }, 0);
        }

        $scope.$on('resizeHandler', function() {
          if($route.current.$$route.controller == 'MetricsCtrl') {
            console.log('metrics resize');
            redraw($scope.dashboard);
          }
        });

        /*click the chart to be bigger*/
        $scope.showBig = function(t){
          var metricDetailUrl = $config.uri.metricdetail + '/' + t.name;
          // var metricDetailUrl = '/js/mock_data/anom.json';
          $http.get(metricDetailUrl).success(function (data){
            $rootScope.showBigChart($barkChart.getOptionBig(data));
          });

        }

        $scope.getSample = function(item) {
          $rootScope.$broadcast('downloadSample', item.name);
        };

        
    }
    ]);
});

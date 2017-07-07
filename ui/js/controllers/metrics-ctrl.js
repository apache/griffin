/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

define(['./module'], function(controllers) {
    'use strict';
    controllers.controller('MetricsCtrl', ['$scope', '$http', '$config', '$location', '$routeParams', '$timeout', '$compile', '$route', '$barkChart', '$rootScope',function($scope, $http, $config, $location, $routeParams, $timeout, $compile, $route, $barkChart, $rootScope) {
        console.log('Route parameter: ' + $routeParams.sysName);

        var echarts = require('echarts');

        pageInit();
        $scope.dataData = [];
        $scope.finalData = [];
        function pageInit() {
          $scope.$emit('initReq');
          $scope.orgs = [];
          var url_dashboard = $config.uri.dashboard ;
          var url_organization = $config.uri.organization;

          $http.get(url_organization).then(function successCallback(res){
               var orgNode = null;
               res = res.data;
               angular.forEach(res, function(value,key) {
                    orgNode = new Object();
                    $scope.orgs.push(orgNode);
                    orgNode.name = key;
                    orgNode.assetMap = value;
               });
               $scope.originalOrgs = angular.copy($scope.orgs);
//               $http.get(url_dashboard).then(function successCallback(data){
                $http.post(url_dashboard, {"query": {"match_all":{}},  "sort": [{"tmst": {"order": "asc"}}],"size":1000}).then(function successCallback(data) {
                    data = data.data;
                    angular.forEach(data.hits.hits, function(sys) {
                        var chartData = sys._source;
                        chartData.sort = function(a,b){
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
                    angular.forEach($scope.metricName,function(name){
                        if($scope.metricNameUnique.indexOf(name) === -1){
                            $scope.dataData[$scope.metricNameUnique.length] = new Array();
                            $scope.metricNameUnique.push(name);
                        }
                    });

                    for(var i = 0;i<$scope.myData.length;i++){
            //push every point to its metric
                        for(var j = 0 ;j<$scope.metricNameUnique.length;j++){
                            if($scope.myData[i]._source.name==$scope.metricNameUnique[j]){
                                $scope.dataData[j].push($scope.myData[i]);
                            }
                        }
                    }
                    angular.forEach($scope.originalOrgs,function(sys,parentIndex){
                        var node = null;
                        node = new Object();
                        node.name = sys.name;
                        node.dq = 0;
                        node.metrics = new Array();
                        angular.forEach($scope.dataData,function(metric,index){
                            if(sys.assetMap.indexOf(metric[metric.length-1]._source.name)!= -1){
                                var metricNode = new Object();
                                var dq = 0.0;
                                if (metric[metric.length-1]._source.total > 0)
                                    dq = metric[metric.length-1]._source.matched/metric[metric.length-1]._source.total*100;
                                metricNode.dq = dq;
                                metricNode.name = metric[metric.length-1]._source.name;
                                metricNode.timestamp = metric[metric.length-1]._source.tmst;
                                metricNode.details = angular.copy(metric);
                                node.metrics.push(metricNode);
                            }
                        })
                        $scope.finalData.push(node);
                    })

                    $scope.originalData = angular.copy($scope.finalData);

                    if($routeParams.sysName && $scope.originalData && $scope.originalData.length > 0){
                      for(var i = 0; i < $scope.originalData.length; i ++){
                        if($scope.originalData[i].name == $routeParams.sysName){
                          $scope.selectedOrgIndex = i;
                          $scope.changeOrg();
                          $scope.orgSelectDisabled = true;
                          break;
                        }

                      }
                    }
                    $timeout(function() {
                        redraw($scope.finalData);
                    });
                });
            });//          $http.post(url_dashboard, {"query": {"match_all":{}},"size":1000}).success(function(res) {
        }

        $scope.$watch('selectedOrgIndex', function(newValue){
          console.log(newValue);
        });
        var redraw = function(data) {
          $scope.chartHeight = $('.chartItem:eq(0)').width()*0.8+'px';
            angular.forEach(data, function(sys, parentIndex) {
                var parentIndex = parentIndex
                angular.forEach(sys.metrics, function(metric, index) {
                    $('#thumbnail'+parentIndex+'-'+index).get(0).style.width = $('#thumbnail'+parentIndex+'-'+index).parent().width()+'px';
                    $('#thumbnail'+parentIndex+'-'+index).get(0).style.height = $scope.chartHeight;
                    var thumbnailChart = echarts.init($('#thumbnail'+parentIndex+'-'+index).get(0), 'dark');
                    thumbnailChart.setOption($barkChart.getOptionThum(metric));
                });
            });
        }
        $scope.assetOptions = [];

        $scope.changeOrg = function() {
            $scope.selectedAssetIndex = undefined;
            $scope.assetOptions = [];
            $scope.finalData = [];
            if($scope.selectedOrgIndex === ""){
              $scope.finalData = angular.copy($scope.originalData);
            }
            else {
              var org = angular.copy($scope.originalData[$scope.selectedOrgIndex]);
              $scope.finalData.push(org);
              angular.forEach(org.metrics, function(metric, index) {
                if($scope.assetOptions.indexOf(metric.name) == -1) {
                  $scope.assetOptions.push(metric.name);
                }
              });
            }
            $timeout(function() {
                redraw($scope.finalData);
            }, 0);
        };

        $scope.changeAsset = function() {
            $scope.finalData = [];
            if($scope.selectedOrgIndex === ""){
              $scope.finalData = angular.copy($scope.originalData);
            } else {
              var org = angular.copy($scope.originalData[$scope.selectedOrgIndex]);
              $scope.finalData.push(org);
            }
            if($scope.selectedAssetIndex != undefined && $scope.selectedAssetIndex != ''){
              var asset = $scope.assetOptions[$scope.selectedAssetIndex];
              angular.forEach($scope.finalData, function(sys) {
                var oldMetrics = sys.metrics;
                sys.metrics = [];
                angular.forEach(oldMetrics, function(metric, index) {
                  if(metric.name == asset) {
                    sys.metrics.push(metric);
                  }
                });
              });
            }
            $timeout(function() {
                redraw($scope.finalData);
            }, 0);
        }

        function resizePieChart() {
          $('#data-asset-pie').css({
              height: $('#data-asset-pie').parent().width(),
              width: $('#data-asset-pie').parent().width()
          });
        }

        $scope.$on('resizeHandler', function() {
          if($route.current.$$route.controller == 'MetricsCtrl') {
            console.log('metrics resize');
            redraw($scope.dataData);
          }
        });

        /*click the chart to be bigger*/
        $scope.showBig = function(t){
            // $rootScope.showBigChart($barkChart.getOptionBig(t));
             window.location.href = '/#!/detailed/'+t.name;
        }

        $scope.getSample = function(item) {
          $rootScope.$broadcast('downloadSample', item.name);
        };
      }
   ]);
});

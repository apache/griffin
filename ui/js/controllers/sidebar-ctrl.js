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
    controllers.controller('SideBarCtrl', ['$scope', '$http', '$config', '$filter', '$timeout', '$compile', '$routeParams', '$barkChart', '$rootScope','$location', function($scope, $http, $config, $filter, $timeout, $compile, $routeParams, $barkChart, $rootScope,$location) {

        var echarts = require('echarts');
        var renderDataAssetPie = function(status) {
            resizePieChart();
            $scope.dataAssetPieChart = echarts.init($('#data-asset-pie')[0], 'dark');
            $scope.dataAssetPieChart.setOption($barkChart.getOptionPie(status));
        }
        pageInit();

        $scope.orgs = [];
        $scope.finalData = [];
        $scope.metricData = [];

        function pageInit() {
          var allDataassets = $config.uri.dataassetlist;
              var health_url = $config.uri.statistics;
              $scope.status = new Object();
              $http.get(health_url).then(function successCallback(response){
                  response = response.data;
                  $scope.status.health = response.healthyJobCount;
                  $scope.status.invalid = response.jobCount - response.healthyJobCount;
                  // renderDataAssetPie($scope.status);
                  sideBarList();
              },function errorCallback(response){

              });
              var dataasset = 0;
              $http.get(allDataassets).then(function successCallback(data) {
                angular.forEach(data.data,function(db){
                  angular.forEach(db,function(table){
                    dataasset = dataasset + 1;
                  });
                });
                $scope.dataasset = dataasset;
              });

        }

        $scope.$watch(function(){return $routeParams.sysName;}, function(value){
          console.log('Watched value: ' + value);
        });

        $scope.draw = function(metric, parentIndex, index) {
            var chartId = 'chart' + parentIndex + '-' + index;
            document.getElementById(chartId).style.width = ($('.panel-heading').innerWidth()-20)+'px';
            document.getElementById(chartId).style.height = '200px';
            var myChart = echarts.init($('#'+chartId).get(0), 'dark');
            metric.myOption = $barkChart.getOptionSide(metric);
            myChart.setOption(metric.myOption);

            $('#'+chartId).unbind('click');
            $('#'+chartId).click(function(e) {
              window.location.href = '/#!/detailed/'+$scope.finalData[parentIndex].metrics[index].name;
            });

        };

        var showBig = function(metric){
            $rootScope.showBigChart($barkChart.getOptionBig(metric));
        }

        function sideBarList(sysName){
             var url_organization = $config.uri.organization;
//            var url_organization = 'org.json';
            $http.get(url_organization).then(function successCallback(res){
               var orgNode = null;
               angular.forEach(res.data, function(value,key) {
               orgNode = new Object();
               $scope.orgs.push(orgNode);
               orgNode.name = key;
               orgNode.assetMap = value;
               });
               $scope.originalOrg = angular.copy($scope.orgs);
               var url_briefmetrics = $config.uri.dashboard;
                $http.post(url_briefmetrics, {"query": {"match_all":{}},  "sort": [{"tmst": {"order": "asc"}}],"size":1000}).then(function successCallback(data) {
//               $http.get('data.json').then(function successCallback(data){
                   angular.forEach(data.data.hits.hits, function(sys) {
                        var chartData = sys._source;
                        chartData.sort = function(a,b){
                        return a.tmst - b.tmst;
                        }
                   });
               $scope.originalData = angular.copy(data.data);
               $scope.myData = angular.copy($scope.originalData.hits.hits);
               $scope.metricName = [];
               for(var i = 0;i<$scope.myData.length;i++){
                   $scope.metricName.push($scope.myData[i]._source.name);
               }
               $scope.metricNameUnique = [];
               angular.forEach($scope.metricName,function(name){
                   if($scope.metricNameUnique.indexOf(name) === -1){
                        $scope.metricData[$scope.metricNameUnique.length] = new Array();
                        $scope.metricNameUnique.push(name);
                 }
               });
               $scope.metricsCount = $scope.metricNameUnique.length;

               for(var i = 0;i<$scope.myData.length;i++){
            //push every point to its metric
                    for(var j = 0 ;j<$scope.metricNameUnique.length;j++){
                        if($scope.myData[i]._source.name==$scope.metricNameUnique[j]){
                            $scope.metricData[j].push($scope.myData[i]);
                        }
                    }
                }
               angular.forEach($scope.originalOrg,function(sys,parentIndex){
                   var node = null;
                   node = new Object();
                   node.name = sys.name;
                   node.dq = 0;
                   node.metrics = new Array();
                   angular.forEach($scope.metricData,function(metric,index){
                        if(sys.assetMap.indexOf(metric[metric.length-1]._source.name)!= -1){
                            var metricNode = new Object();
                            metricNode.name = metric[metric.length-1]._source.name;
                            metricNode.timestamp = metric[metric.length-1]._source.tmst;
                            metricNode.dq = metric[metric.length-1]._source.matched/metric[metric.length-1]._source.total*100;
                            metricNode.details = angular.copy(metric);
                            node.metrics.push(metricNode);
                        }
                   });
                   $scope.finalData.push(node);
                });

            if(!sysName){
              $scope.backup_metrics = angular.copy($scope.finalData);
            }
            $timeout(function() {
                resizeSideChart();
            }, 0);
           });
          });
        }

        $(window).resize(function() {
            console.log('sidebar resize');
            if(window.innerWidth < 992) {
              $('#rightbar').css('display', 'none');
            } else {
              $('#rightbar').css('display', 'block');
              resizePieChart();
              $scope.dataAssetPieChart.resize();
              resizeSideChart();
            }
        });

        function resizeSideChart() {
            $('#side-bar-metrics').css({
                height: $('#mainContent').height()-$('#side-bar-stats').outerHeight()+70
            });
            angular.forEach($scope.finalData, function(sys, sysIndex) {
            var sysIndex = sysIndex;
            angular.forEach(sys.metrics, function(metric, index) {
                if (!metric.tag) {
                  $scope.draw(metric, sysIndex, index);
                }
            })
          });
        }

        function resizePieChart() {
          $('#data-asset-pie').css({
              height: $('#data-asset-pie').parent().width(),
              width: $('#data-asset-pie').parent().width()
          });
        }
       }
    ]);
});

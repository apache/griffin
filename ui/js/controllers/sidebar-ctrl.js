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
    controllers.controller('SideBarCtrl', ['$scope', '$http', '$config', '$filter', '$timeout', '$compile', '$routeParams', '$barkChart', '$rootScope', function($scope, $http, $config, $filter, $timeout, $compile, $routeParams, $barkChart, $rootScope) {

        var echarts = require('echarts');

        pageInit();

        $scope.orgs = [];
        $scope.finalData = [];
        $scope.dataData = [];

        var renderDataAssetPie = function(status) {
            resizePieChart();
            $scope.dataAssetPieChart = echarts.init($('#data-asset-pie')[0], 'dark');
            $scope.dataAssetPieChart.setOption($barkChart.getOptionPie(status));
        }
        function pageInit() {
              $scope.status = new Object();
              $scope.status.health = '100';
              $scope.status.invalid = '6';
//              renderDataAssetPie($scope.status);
              sideBarList();
        }

        $scope.$watch(function(){return $routeParams.sysName;}, function(value){
          console.log('Watched value: ' + value);
          if(value){
            sideBarList(value);
          }else{
            $scope.briefmetrics = $scope.backup_metrics;
          }
        });

        $scope.draw = function(metric, parentIndex, index) {
            var chartId = 'chart' + parentIndex + '-' + index;
            document.getElementById(chartId).style.width = ($('.panel-heading').innerWidth()-20)+'px';
            document.getElementById(chartId).style.height = '200px';
            var myChart = echarts.init($('#'+chartId).get(0), 'dark');
            metric.myOption = $barkChart.getOptionSide(metric);
            myChart.setOption(metric.myOption);

            $('#'+chartId).unbind('click');
            $('#'+chartId).click(function() {
              showBig($scope.finalData[parentIndex].metrics[index]);
            });

        };

        var showBig = function(metric){
            $rootScope.showBigChart($barkChart.getOptionBig(metric));
        }

        function sideBarList(sysName){
            var url_organization = $config.uri.organization;
            $http.get(url_organization).success(function(res){
               var orgNode = null;
               angular.forEach(res, function(value,key) {
               orgNode = new Object();
               $scope.orgs.push(orgNode);
               orgNode.name = key;
               orgNode.assetMap = value;
               });
               $scope.originalOrg = angular.copy($scope.orgs);
               var url_briefmetrics = $config.uri.dashboard;
               $http.get(url_briefmetrics, {cache:true}).success(function(data) {
                   $scope.briefmetrics = data;
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
               angular.forEach($scope.originalOrg,function(sys,parentIndex){
                   var node = null;
                   node = new Object();
                   node.name = sys.name;
                   node.dq = 0;
                   node.metrics = new Array();
                   angular.forEach($scope.dataData,function(metric,index){
                        if(sys.assetMap.indexOf(metric[0]._source.name)!= -1){
                            var metricNode = new Object();
                            metricNode.name = metric[0]._source.name;
                            metricNode.timestamp = metric[0]._source.tmst;
                            metricNode.dq = metric[0]._source.matched/metric[0]._source.total*100;
                            metricNode.details = angular.copy(metric);
                            node.metrics.push(metricNode);
                        }
                   });
                   $scope.finalData.push(node);
                })
//            if(!sysName){
//              $scope.backup_metrics = angular.copy(res);
//            }
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

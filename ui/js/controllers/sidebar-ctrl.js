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

        $scope.orgs=[];
        function renderDataAssetPie(status){
            resizePieChart();
            $scope.dataAssetPieChart = echarts.init($('#data-asset-pie')[0], 'dark');
            $scope.dataAssetPieChart.setOption($barkChart.getOptionPie(status));
        }

        function pageInit() {
//          var url = $config.uri.statistics;

//          $http.get(url).success(function(res) {
//              $scope.datasets = res.assets;
//              $scope.metrics = res.metrics;
              $scope.status = new Object();
              $scope.status.health = '100';
              $scope.status.invalid = '6';

              renderDataAssetPie($scope.status);
              sideBarList();
//          });


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
            console.log(metric);
            var chartId = 'chart' + parentIndex + '-' + index;
            document.getElementById(chartId).style.width = ($('.panel-heading').innerWidth()-20)+'px';
            document.getElementById(chartId).style.height = '200px';
            var myChart = echarts.init($('#'+chartId).get(0), 'dark');
            metric.myOption = $barkChart.getOptionSide($scope.dataData[parentIndex]);
            myChart.setOption(metric.myOption);
            console.log(parentIndex);
            console.log(index);
            console.log($scope.dataData[parentIndex]);
            $('#'+chartId).unbind('click');
            $('#'+chartId).click(function() {
              showBig($scope.dataData[parentIndex]);
            });

        };

        var showBig = function(metric){
//          var metricDetailUrl = $config.uri.metricdetail + '/' + metric.name;
//          $http.get(metricDetailUrl).success(function (data){
            $rootScope.showBigChart($barkChart.getOptionBig(metric));
//          });
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
          });

          var url_briefmetrics = $config.uri.dashboard;
          $http.get(url_briefmetrics, {cache:true}).success(function(data) {
          $scope.briefmetrics = data;

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
                      $scope.myData[i].value = $scope.myData[i]._source.matched/$scope.myData[i]._source.total*100;
                      $scope.dataData[j].push($scope.myData[i]);
                  }
              }
          }
          console.log($scope.dataData);
          if(!sysName){
            $scope.backup_metrics = angular.copy(data);
          }

          $timeout(function() {
            resizeSideChart();
          }, 0);
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

            console.log($scope.briefmetrics);
            angular.forEach($scope.briefmetrics, function(sys, sysIndex) {
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

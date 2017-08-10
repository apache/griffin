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
    controllers.controller('BigChartCtrl', ['$scope', '$http', '$config', '$filter', '$timeout', '$compile', '$routeParams', '$barkChart', '$rootScope', '$location',function($scope, $http, $config, $filter, $timeout, $compile, $routeParams, $barkChart, $rootScope,$location) {

      var echarts = require('echarts');

      pageInit();

      function pageInit() {
        resizeBigChart();
        // $scope.bigChart = echarts.init($('#bigChartDiv').get(0), 'dark');
        $rootScope.bigChart = echarts.init($('#bigChartDiv').get(0), 'dark');
      }

      function resizeBigChart() {
        document.getElementById('bigChartDiv').style.width = window.innerWidth+'px';
        document.getElementById('bigChartDiv').style.height = window.innerHeight+'px';
      }

      $scope.closeBigChart = function(){
        console.log('close big chart!');
        // $('#bigChartContainer').hide();
        // history.back();
        $location.path('/health');
        // $('#mainWindow').show();
      }

      $scope.downloadSample = function() {
        $rootScope.$broadcast('downloadSample', $scope.selectedModel);
      }

      $(window).resize(function() {
        console.log('big chart resize');
          resizeBigChart();
          $rootScope.bigChart.resize();
      });

      // $rootScope.showBigChart = function(option) {
      //   $scope.selectedModel = option.title.text;
      //   $('#bigChartContainer').show();
      //   // $('#mainWindow').hide();
      //   $scope.bigChart.clear();
      //   $scope.bigChart.setOption(option);
      // }
         if($rootScope.showBigChart==undefined){
              $rootScope.showBigChart = function(option) {
              $scope.selectedModel = option.title.text;
              $('#bigChartContainer').show();
              // $('#mainWindow').hide();
              $rootScope.bigChart.clear();
              $rootScope.bigChart.setOption(option);
            }
          }

    }]);
});
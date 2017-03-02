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
    controllers.controller('MyDashboardCtrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route', '$filter', '$compile', '$barkChart', '$rootScope', function($scope, $http, $config, $location, toaster, $timeout, $route, $filter, $compile, $barkChart, $rootScope) {

        var echarts = require('echarts');

        pageInit();

        function pageInit() {
          $scope.$emit('initReq');

          var url_dashboard = $config.uri.getmydashboard + $scope.ntAccount;
          $http.get(url_dashboard).success(function(res) {
              $scope.dashboard = res;
              angular.forEach(res, function(sys) {
                angular.forEach(sys.metrics, function(metric) {
                  var chartData = metric.details;
                  chartData.sort(function(a, b){
                    return a.timestamp - b.timestamp;
                  });

                });
              });
              $scope.originalData = angular.copy(res);
              $timeout(function() {
                redraw($scope.dashboard);
              }, 0);
          });

        }

        var redraw = function(data) {

          $scope.chartHeight = $('.chartItem:eq(0)').width()*0.8+'px';

            angular.forEach(data, function(sys, parentIndex) {
                var parentIndex = parentIndex;
                angular.forEach(sys.metrics, function(metric, index) {
                    $('#thumbnail'+parentIndex+'-'+index).get(0).style.width = $('#thumbnail'+parentIndex+'-'+index).parent().width()+'px';
                    $('#thumbnail'+parentIndex+'-'+index).get(0).style.height = $scope.chartHeight;

                    var thumbnailChart = echarts.init($('#thumbnail'+parentIndex+'-'+index).get(0), 'dark');
                    thumbnailChart.setOption($barkChart.getOptionThum(metric));
                });
            });
        }

        $scope.$on('resizeHandler', function() {
            if($route.current.$$route.controller == 'MyDashboardCtrl') {
                console.log('mydashboard resize');
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

    }]);
});

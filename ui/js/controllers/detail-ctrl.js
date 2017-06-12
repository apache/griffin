/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
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
    controllers.controller('DetailCtrl', ['$scope', '$http', '$config', '$location','$timeout', '$route', '$barkChart', '$rootScope','$routeParams', function ($scope, $http, $config, $location, $timeout, $route, $barkChart, $rootScope,$routeParams) {
      console.log('detail controller');
      // var url="/js/controllers/heatmap.json";

        var echarts = require('echarts');
        var formatUtil = echarts.format;


        var showBig = function(metricName){
          var metricDetailUrl = $config.uri.dashboard;
          // var metricDetailUrl = 'data.json';
          $http.post(metricDetailUrl, {"query": {  "bool":{"filter":[ {"term" : {"name": metricName }}]}},  "sort": [{"tmst": {"order": "asc"}}],"size":1000}).success(function(data) {
            // $http.get(metricDetailUrl).success(function (data) {
                // body...
            
            var metric = new Object();
            metric.name = data.hits.hits[0]._source.name;
            metric.timestamp = data.hits.hits[data.hits.hits.length-1]._source.tmst;
            metric.dq = data.hits.hits[data.hits.hits.length-1]._source.matched/data.hits.hits[data.hits.hits.length-1]._source.matched*100;
            metric.details = new Array();
            angular.forEach(data.hits.hits,function(point){
                metric.details.push(point);
            })
            $rootScope.showBigChart($barkChart.getOptionBig(metric));
          });
        }

        pageInit();

        function pageInit() {
            $scope.$emit('initReq');
            showBig($routeParams.modelname);
        }

        $scope.$on('resizeHandler', function(e) {
            if($route.current.$$route.controller == 'HealthCtrl'){
                console.log('health resize');
                resizeTreeMap();
                $scope.myChart.resize();
            }
        });

        function resizeTreeMap() {
            $('#chart1').height( $('#mainWindow').height() - $('.bs-component').outerHeight() );
        }

    }]);
});

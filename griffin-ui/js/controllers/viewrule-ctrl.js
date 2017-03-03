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
define(['./module'], function (controllers) {
    'use strict';
    controllers.controller('ViewRuleCtrl', ['$filter', '$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$routeParams', '$barkChart', '$route',  function ($filter, $scope, $http, $config, $location, toaster, $timeout, $routeParams, $barkChart, $route) {

      var echarts = require('echarts');

      pageInit();

      function pageInit() {
        $scope.$emit('initReq');

        var getModelUrl = $config.uri.getModel+"/"+$routeParams.modelname;
        $http.get(getModelUrl).success(function(data){
          $scope.ruleData = data;
        }).error(function(data){
          // errorMessage(0, 'Save model failed, please try again!');
          toaster.pop('error', data.message);
        });

        $scope.anTypes = ['', 'History Trend Detection', 'Bollinger Bands Detection', 'Deviation Detection'];

        var url= $config.uri.rulemetric+"/"+$routeParams.modelname;

        $http.get(url).success(function(res){
            $scope.modelresultData = res;
            if (res.details) {
              $('#viewrule-chart').height(200);
              $scope.ruleChart = echarts.init(document.getElementById('viewrule-chart'), 'dark');
              $scope.ruleChart.setOption($barkChart.getOptionSide(res));

            }
            resizeWindow();
        }).error(function(data) {
          resizeWindow();
          toaster.pop('error', data.message);
        });
      }

      $scope.confirmDeploy = function(){
        var deployModelUrl = $config.uri.enableModel + '/' + $scope.ruleData.basic.name;
        var answer = confirm('Are you sure you want to deploy this model to production?')

        if(answer){
          $http.get(deployModelUrl).success(function(){
            $scope.ruleData.basic.status = 2;
            toaster.pop('info', 'Your model has been deployed to prduction!');
          });
        }

      }


      $scope.$on('resizeHandler', function(e) {
            if ($route.current.$$route.controller == "ViewRuleCtrl") {
                resizeWindow();
            }
        });

      function resizeWindow(){

            var h1 = $('#viewruleDefinition').height();
            var h2 = $('#viewTestResult').height();
            var height = Math.max(h1, h2);

            $('#viewruleDefinition').height(height);
            $('#viewTestResult').height(height);

            if ($scope.ruleChart) {
              $scope.ruleChart.resize();
            }
      }
    }]);
});

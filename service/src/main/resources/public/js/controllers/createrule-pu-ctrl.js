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
    controllers.controller('CreateRulePUCtrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route', '$filter', function($scope, $http, $config, $location, toaster, $timeout, $route, $filter) {
        console.log('Create rule controller');
        $scope.currentStep = 1;

        // var publishBasic = 'https://bark.vip.ebay.com/api/v1/metrics';
        var publishBasic = document.location.origin;
        // $scope.form.publishUrl = publishBasic;
        $scope.$watch('form.basic.name', function(newValue){
          $scope.form.publishUrl = publishBasic;// + (newValue?newValue:'');
        });

        $scope.$on('$viewContentLoaded', function() {
            $scope.$emit('initReq');
            resizeWindow();
        });

        var dbtreeUrl = $config.uri.dbtree;
          $http.get(dbtreeUrl).success(function(data) {
            // console.log(data);
            angular.forEach(data, function(p){
              if(p.platform == 'Apollo'){ //::TODO:Only Apollo is supported cussrently
                $scope.allSystem = {};
                angular.forEach(p.systems, function(sys){ //retrieve all systems
                  $scope.allSystem[sys.name] = sys.assets;
                });
              }
            });

            $scope.sysChange();

          });

        $scope.sysChange = function(){
          console.log($scope.form.basic.system);
          var sys = $scope.form.basic.system;
          var sysName = $filter('strmap')(sys, 'modelsystem');
          $scope.assets = $scope.allSystem[sysName];

        }

        $scope.$on('resizeHandler', function(e) {
            if ($route.current.$$route.controller == "CreateRulePUCtrl") {
                resizeWindow();
            }
        });

        function resizeWindow() {
                    var stepSelection = '.formStep[id=step-' + $scope.currentStep + ']';
                    $(stepSelection).css({
                        height: window.innerHeight - $(stepSelection).offset().top - $('#footerwrap').outerHeight()
                    });
                    $('fieldset').height($(stepSelection).height() - $(stepSelection + '>.stepDesc').height() - $('.btn-container').height() - 80);
                    $('.y-scrollable').css({
                        'max-height': $('fieldset').height()
                    });
        }

        $scope.ruleTypes = $filter('strarr')('modeltype');//['Accuracy', 'Validity', 'Anomaly Detection', 'Publish Metrics'];
        $scope.scheduleTypes = $filter('strarr')('scheduletype');//['Daily', 'Weekly', 'Monthly', 'Hourly'];
        $scope.ruleSystems = $filter('strarr')('modelsystem');//['Bullseye', 'GPS', 'Hadoop', 'PDS', 'IDLS', 'Pulsar', 'Kafka'];

        // Initial Value
        $scope.form = {
            submit: function(form) {
                if (!form.$valid) {
                    var field = null
                      , firstError = null ;
                    for (field in form) {
                        if (field[0] != '$') {
                            if (firstError === null  && !form[field].$valid) {
                                firstError = form[field].$name;
                            }

                            if (form[field].$invalid) {
                                form[field].$dirty = true;
                            }
                        }
                    }
                    angular.element('.ng-invalid[name=' + firstError + ']').focus();
                    errorMessage($scope.currentStep);
                } else {
                  if(this.basic.system == -1){
                    errorMessage(0, 'Please select Organization');
                  }else{
                    form.$setPristine();
                    if(this.basic.dataaset == '-1'){
                      this.basic.dataaset = undefined;
                    }
                    this.data={
                      basic: this.basic,
                      extra: {publishUrl: this.publishUrl}
                    };

                    if($scope.assets){
                      for(var i = 0; i < $scope.assets.length; i ++){
                        var aset = $scope.assets[i];
                        if(aset.name == this.basic.dataaset){
                          this.data.basic.dataasetId = aset.id;

                        }
                      }
                    }

                    $('#confirm-pu').modal('show');
                  }
                }
            },

            save: function() {


                //::TODO: Need to save the data to backend with POST/PUT method
                console.log(JSON.stringify($scope.form.data));


                var newModel = $config.uri.newPublishModel;
                $http.post(newModel, this.data).success(function() {

                    $('#confirm-pu').on('hidden.bs.modal', function(e) {
                        $('#confirm-pu').off('hidden.bs.modal');
                        $location.path('/rules');
                        $scope.$apply();
                    });
                  	$('#confirm-pu').modal('hide');

                }).error(function(data){
                  // errorMessage(0, 'Save model failed, please try again!');
                  toaster.pop('error', 'Save model failed, please try again!', data.message);
                });
            },

        };

        var errorMessage = function(i, msg) {
            var errorMsgs = ['Please fill in all required fields'];
            if (!msg) {
                toaster.pop('error', 'Error', errorMsgs[i - 1], 0);
            } else {
                toaster.pop('error', 'Error', msg, 0);
            }
        }
        ;
    }
    ]);
});

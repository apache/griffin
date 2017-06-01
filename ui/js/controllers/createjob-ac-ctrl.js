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


define(['./module'], function(controllers) {
    'use strict';
    controllers.controller('CreateJobACCtrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route', '$filter', function($scope, $http, $config, $location, toaster, $timeout, $route, $filter) {
        console.log('Create job controller');
        $scope.currentStep = 1;

        $scope.Measures = [];
        $scope.$on('$viewContentLoaded', function() {
            // console.log($('#footerwrap').css('height'));
            // console.log($('.formStep').offset());
            $scope.$emit('initReq');
            resizeWindow();

            //  $('#confirm').on('hidden.bs.modal', function (e) {
            //    console.log('hidden');
            //   //  $('#confirm').off('hidden.bs.modal');
            //    $location.path('/rules');
            //   });

            // $('.formStep').css({height: 800});
        });
        var getMeasureUrl = $config.uri.getMeasuresByOwner+$scope.ntAccount;
        $http.get(getMeasureUrl).success(function(res){
            angular.forEach(res,function(measure){
                $scope.Measures.push(measure);
            })
            console.log($scope.Measures);
        })



        $scope.$on('resizeHandler', function(e) {
            if ($route.current.$$route.controller == "CreateRuleACCtrl") {
                $scope.$emit('initReq');
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
                        'max-height': $('fieldset').height()- $('.add-dataset').outerHeight()
                    });

        }

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
                    //  $location.path('/rules');
                    form.$setPristine();
                    var rule = '';
                    this.data={
                      "sourcePat":$scope.sourcePat,
                      "targetPat":$scope.targetPat,
                      "jobStartTime":$scope.jobStartTime,
                      "periodTime":$scope.periodTime,
                      "groupName":'BA',
                    };
                    $('#confirm-job').modal('show');
                }
            },

            save: function() {


                //::TODO: Need to save the data to backend with POST/PUT method
                console.log(JSON.stringify($scope.form.data));

//                var newModel = $config.uri.newAccuracyModel;
//                var BACKEND_SERVER = '';
                var date = new Date();
                var month = date.getMonth()+1;
                var timestamp = Date.parse(date);
                timestamp = timestamp / 1000;
                var jobName = $scope.Measures[$scope.measure] + '-BA-' + $scope.ntAccount + '-' + date.getFullYear() + '-'+ month + '-'+date.getDate();
                var newJob = $config.uri.addJobs + this.data.groupName + '/' + jobName + '/' + $scope.Measures[$scope.measure];
                console.log(newJob);
                console.log(this.data);
                $http.post(newJob, this.data).success(function(data) {
                	// if(data.status=='0')
                	// {
                	  console.log(data);
                      if(data=='fail'){
                          toaster.pop('error', 'Please modify the name of job, because there is already a same model in database ', data.message);
                          return;
                      }

	                  $('#confirm-job').on('hidden.bs.modal', function(e) {
	                      $('#confirm-job').off('hidden.bs.modal');
	                      $location.path('/jobs').replace();
	                      $scope.$apply();
	                  });
	                	$('#confirm-job').modal('hide');
	                // }
                	// else
                	// {
                	// 	errorMessage(0, data.result);
                	// }

                }).error(function(data){
                  // errorMessage(0, 'Save model failed, please try again!');
                  toaster.pop('error', 'Save model failed, please try again!', data.message);
                });

            },

        }

        var errorMessage = function(i, msg) {
            var errorMsgs = ['Please select at least one attribute!', 'Please select at least one attribute in target, make sure target is different from source!', 'Please make sure to map each target to a unique source.', 'please complete the form in this step before proceeding'];
            if (!msg) {
                toaster.pop('error', 'Error', errorMsgs[i - 1], 0);
            } else {
                toaster.pop('error', 'Error', msg, 0);
            }
        };
    }
    ]);


});

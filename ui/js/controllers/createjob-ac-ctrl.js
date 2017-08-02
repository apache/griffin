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
    controllers.controller('CreateJobACCtrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route', '$filter', function($scope, $http, $config, $location, toaster, $timeout, $route, $filter) {
        console.log('Create job controller');
        $scope.currentStep = 1;

        $( "#datepicker" ).datepicker();

        $scope.Times = ['seconds','minutes','hours'];
        $scope.timeType = 'seconds';

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
        $http.get(getMeasureUrl).then(function successCallback(res){
            angular.forEach(res.data,function(measure){
                $scope.Measures.push(measure);
            })
            $scope.measure = 0;
        })



        $scope.$on('resizeHandler', function(e) {
            if ($route.current.$$route.controller == "CreateRuleACCtrl") {
                $scope.$emit('initReq');
                resizeWindow();
            }
        });


        function resizeWindow() {
            var stepSelection = '.formStep';
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
            prev:function(form){
                history.back();
            },
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
                    var period;
                    if($scope.timeType=='minutes')
                        period = $scope.periodTime *60;
                    else if($scope.timeType=='hours')
                        period = $scope.periodTime * 3600;
                    else period = $scope.periodTime;
                    var rule = '';
                    var startTime = $scope.jobStartTime;
                    console.log(startTime);
                    var year = $scope.jobStartTime.split('/')[2];
                    var month = $scope.jobStartTime.split('/')[0];
                    var day = $scope.jobStartTime.split('/')[1];
                    startTime = year +'-'+ month + '-'+ day + ' '+ $scope.time;
                    startTime = Date.parse(startTime);
                    if(isNaN(startTime)){
                        toaster.pop('error','Please input the right format of start time');
                        return;
                    }
                    this.data={
                      "sourcePat":$scope.sourcePat,
                      "targetPat":$scope.targetPat,
                      "jobStartTime":startTime,
                      "periodTime":period,
                      "groupName":'BA',
                    };
                    $('#confirm-job').modal('show');
                }
            },

            save: function() {
                //::TODO: Need to save the data to backend with POST/PUT method
                var date = new Date();
                var month = date.getMonth()+1;
                var timestamp = Date.parse(date);
                var jobName = $scope.Measures[$scope.measure] + '-BA-' + $scope.ntAccount + '-' + timestamp;

                var newJob = $config.uri.addJobs + '?group=' + this.data.groupName + '&jobName=' + jobName + '&measureName=' + $scope.Measures[$scope.measure];
                console.log(newJob);
                console.log(this.data);
                $http.post(newJob, this.data).then(function successCallback(data) {
	                  $('#confirm-job').on('hidden.bs.modal', function(e) {
	                      $('#confirm-job').off('hidden.bs.modal');
	                      $location.path('/jobs').replace();
	                      $scope.$apply();
	                  });
	                   $('#confirm-job').modal('hide');
                      },function errorCallback(response) {
                        toaster.pop('error', 'Error when creating job', response.message);
                });
            },
        }

        var errorMessage = function(i, msg) {
            var errorMsgs = ['Please complete the form!', 'please complete the form in this step before proceeding'];
            if (!msg) {
                toaster.pop('error', 'Error', errorMsgs[i - 1], 0);
            } else {
                toaster.pop('error', 'Error', msg, 0);
            }
        };
    }
    ]);


});

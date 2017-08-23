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
    controllers.controller('CreateJobACCtrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route', '$filter', '$barkChart',function($scope, $http, $config, $location, toaster, $timeout, $route, $filter,$barkChart) {
        console.log('Create job controller');
        $scope.currentStep = 1;
        var echarts = require('echarts');
        $( "#datepicker" ).datepicker();

        $scope.Times = ['seconds','minutes','hours'];
        $scope.timeType = 'seconds';
        $scope.isOpen = false;
        $scope.maskOpen = false;

        $scope.hourDetail = '00';
        $scope.minuteDetail = '00';
        $scope.secondDetail = '00';
        $scope.timeDetail = '00:00:00';

        var changeTime = function(min,max,increase,time){
            time = parseInt(time);
            if(increase){
                if(time==max)
                    time = min;
                else time = time + 1;
            }
            else{
                if(time==min)
                    time = max;
                else time = time - 1;
            }
            console.log(typeof(time));
            if(time < 10)
                time = '0' + time;
            console.log(typeof(time));
            return time;
        }

        $scope.showTime = function(){
            $scope.isOpen = !$scope.isOpen;
            $scope.maskOpen = !$scope.maskOpen;
        }

        $scope.close = function(){
            $scope.isOpen = false;
            $scope.maskOpen = false;
        }

        $scope.hourIncrease = function(){
            $scope.hourDetail = changeTime(0,24,true,$scope.hourDetail);
            $scope.timeDetail = $scope.hourDetail+':'+$scope.minuteDetail+':'+$scope.secondDetail;
        }

        $scope.hourDecrease = function(){
            $scope.hourDetail = changeTime(0,24,false,$scope.hourDetail);
            $scope.timeDetail = $scope.hourDetail+':'+$scope.minuteDetail+':'+$scope.secondDetail;
        }

        $scope.minuteIncrease = function(){
            $scope.minuteDetail = changeTime(0,59,true,$scope.minuteDetail);
            $scope.timeDetail = $scope.hourDetail+':'+$scope.minuteDetail+':'+$scope.secondDetail;
        }

        $scope.minuteDecrease =function(){
            $scope.minuteDetail = changeTime(0,59,false,$scope.minuteDetail);
            $scope.timeDetail = $scope.hourDetail+':'+$scope.minuteDetail+':'+$scope.secondDetail;
        }

        $scope.secondIncrease = function(){
            $scope.secondDetail = changeTime(0,59,true,$scope.secondDetail);
            $scope.timeDetail = $scope.hourDetail+':'+$scope.minuteDetail+':'+$scope.secondDetail;
        }

        $scope.secondDecrease =function(){
            $scope.secondDetail = changeTime(0,59,false,$scope.secondDetail);
            $scope.timeDetail = $scope.hourDetail+':'+$scope.minuteDetail+':'+$scope.secondDetail;
        }

        $scope.Measures = [];
        $scope.$on('$viewContentLoaded', function() {
            $scope.$emit('initReq');
            resizeWindow();
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
            $('#data-asset-pie').css({
                height: $('#data-asset-pie').parent().width(),
                width: $('#data-asset-pie').parent().width()
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
                    startTime = year +'-'+ month + '-'+ day + ' '+ $scope.timeDetail;
                    console.log(startTime);
                    startTime = Date.parse(startTime);
                    if(isNaN(startTime)){
                        toaster.pop('error','Please input the right format of start time');
                        return;
                    }
                    this.data={
                        "sourcePattern":$scope.sourcePat,
                        "targetPattern":$scope.targetPat,
                        "jobStartTime":startTime,
                        "interval":period,
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
                var jobName = $scope.Measures[$scope.measure].id + '-BA-' + $scope.ntAccount + '-' + timestamp;

                var newJob = $config.uri.addJobs + '?group=' + this.data.groupName + '&jobName=' + jobName + '&measureId=' + $scope.Measures[$scope.measure].id;
                console.log(newJob);
                console.log(this.data);
                $http.post(newJob, this.data).then(function successCallback(data) {
	                $('#confirm-job').on('hidden.bs.modal', function(e) {
	                    $('#confirm-job').off('hidden.bs.modal');
	                    $location.path('/jobs').replace();
	                    $scope.$apply();
	                });
	                $('#confirm-job').modal('hide');
                    var health_url = $config.uri.statistics;
                    $scope.status = new Object();
                    $http.get(health_url).then(function successCallback(response){
                        response = response.data;
                        $scope.status.health = response.healthyJobCount;
                        $scope.status.invalid = response.jobCount - response.healthyJobCount;
                        $('#data-asset-pie').css({
                            height: $('#data-asset-pie').parent().width(),
                            width: $('#data-asset-pie').parent().width()
                        });
                        $scope.dataAssetPieChart = echarts.init($('#data-asset-pie')[0], 'dark');
                        $scope.dataAssetPieChart.setOption($barkChart.getOptionPie($scope.status));
                    });
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

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


define(['./module'], function (controllers) {
    'use strict';
    controllers.controller('JobCtrl', ['$scope', '$http', '$config', '$location', '$timeout', '$route', 'toaster', '$filter','$interval', function ($scope, $http, $config, $location, $timeout, $route, toaster, $filter,$interval) {
        console.log('job controller');
        console.log($scope.ntAccount);
        var allJobs = $config.uri.allJobs;
        var ts = null;
        var start = 0;
        var number = 10;    
        var originalRowCollection = undefined;

        function getJobs(start,number,tableState){
            $http.get(allJobs).then(function successCallback(data) {
                angular.forEach(data.data,function(job){
                    job.Interval = job.interval;
                    if(job.Interval<60)
                        job.Interval = job.periodTime + 's';
                    else if(job.Interval<3600)
                    {
                        if(job.Interval%60==0)
                            job.Interval = job.periodTime/60 + 'min';
                        else 
                            job.Interval = (job.periodTime - job.periodTime%60)/60 + 'min'+job.periodTime%60 + 's';
                    }
                    else 
                    {
                        if(job.Interval%3600==0)
                            job.Interval = job.periodTime/3600 + 'h';
                        else
                        {
                            job.Interval = (job.periodTime - job.periodTime%3600)/3600 + 'h';
                            var s = job.periodTime%3600;
                            job.Interval = job.Interval + (s-s%60)/60+'min'+s%60+'s';
                        }
                    }
                    var length = job.jobName.split('-').length;
                    job.createTime = job.jobName.split('-')[length-1];
                });
                data.data.sort(function(a,b){
                    var dateA = a.createTime;
                    var dateB = b.createTime;
                    return -(dateA-dateB);
                });
                originalRowCollection = angular.copy(data.data);
                $scope.rowCollection = angular.copy(data.data);
                $scope.displayed = $scope.rowCollection.slice(start, start+number);
                tableState.pagination.numberOfPages = Math.ceil($scope.rowCollection.length/number);
            },function errorCallback(response){});
        }
    
        $scope.pagingJob = function(tableState){
            ts = tableState;
            start = tableState.pagination.start || 0;
            number = tableState.pagination.number || 10;
            getJobs(start,number,tableState);
            $interval(function(){
                getJobs(start,number,tableState);
            },600000);
        };

        function addCurrent(p_index,number){
            $('#'+p_index+'-'+number).addClass('page-active');
            $('#'+p_index+'-'+number).siblings().removeClass('page-active');
        }

        $scope.showInstances = function showInstances(row,number){
            var p_index = $scope.displayed.indexOf(row);
            $scope.currentJob = row;
  
            var allInstances = $config.uri.getInstances + '?group=' + 'BA' + '&jobName=' + row.jobName +'&page='+'0'+'&size='+'100';
            $http.get(allInstances).then(function successCallback(data){
                row.instances = data.data;
                row.pageCount = new Array();
                for(var i = 0;i<Math.ceil(row.instances.length/10);i++){
                    row.pageCount.push(i);
                }
                addCurrent(p_index,number);
            });
            var url = $config.uri.getInstances + '?group=' + 'BA' + '&jobName=' + row.jobName +'&page='+number+'&size='+'10';
            $http.get(url).then(function successCallback(data){
                row.currentInstances = data.data;
                addCurrent(p_index,number);
            });
            $timeout(function(){
                addCurrent(p_index,number);
            },200);
        }

        $scope.remove = function remove(row) {
            $scope.deletedRow = row;
            $scope.deletedBriefRow = row;
            $('#deleteJobConfirmation').modal('show');
        }

        $scope.confirmDelete = function(){
            var row = $scope.deletedBriefRow;
            var deleteModelUrl = $config.uri.deleteJob + '?group=' + row.groupName+'&jobName='+row.jobName;
            $http.delete(deleteModelUrl).then(function successCallback(){
  
                var index = $scope.rowCollection.indexOf(row);
                $scope.rowCollection.splice(index, 1);
  
                index = $scope.displayed.indexOf(row);
                $scope.displayed.splice(index, 1);
  
                $('#deleteJobConfirmation').modal('hide');
            },function errorCallback(response) {
                toaster.pop('error', 'Error when deleting record', response.message);
            });
        }

        $scope.$on('$viewContentLoaded', function() {
            $scope.$emit('initReq');
        });
    }]);
});
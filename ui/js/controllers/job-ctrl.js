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
        var originalInstances = undefined;
        $scope.n = 0;
        $scope.currentJob = undefined;
        $scope.tableState = [];
        $scope.oldindex = undefined;

        function getJobs(start,number,tableState){
            $http.get(allJobs).then(function successCallback(data) {
                angular.forEach(data.data,function(job){
                    job.Interval = job.interval;
                    if(job.Interval<60)
                        job.Interval = job.interval + 's';
                    else if(job.Interval<3600)
                    {
                        if(job.Interval%60==0)
                            job.Interval = job.interval/60 + 'min';
                        else
                            job.Interval = (job.interval - job.interval%60)/60 + 'min'+job.interval%60 + 's';
                    }
                    else
                    {
                        if(job.Interval%3600==0)
                            job.Interval = job.interval/3600 + 'h';
                        else
                        {
                            job.Interval = (job.interval - job.interval%3600)/3600 + 'h';
                            var s = job.interval%3600;
                            job.Interval = job.Interval + (s-s%60)/60+'min'+s%60+'s';
                        }
                    }
                    var length = job.jobName.split('-').length;
                    job.createTime = job.jobName.split('-')[length-1];
                    $scope.jobName = job.jobName;
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


        function getInstances(start,number,tableState){
            var allInstances = $config.uri.getInstances + '?group=' + 'BA' + '&jobName=' + $scope.currentJob.jobName +'&page='+'0'+'&size='+'200';
            $http.get(allInstances).then(function successCallback(data) {
                originalInstances = angular.copy(data.data);
                $scope.row_currentInstances = angular.copy(data.data);
                $scope.currentInstances = $scope.row_currentInstances.slice(start, start+number);
                tableState.pagination.numberOfPages = Math.ceil($scope.row_currentInstances.length/number);
            },function errorCallback(response){});
        }


        $scope.pagingJob = function(tableState){
            $scope.n = 0;
            $scope.oldindex = undefined;
            ts = tableState;
            start = tableState.pagination.start || 0;
            number = tableState.pagination.number || 10;
            getJobs(start,number,tableState);
            $interval(function(){
                getJobs(start,number,tableState);
            },600000);
        };


        $scope.pagingInstances = function(tableState){
            $scope.tableState[$scope.n] = tableState;
            $scope.n = $scope.n+1;
            ts = tableState;
            start = tableState.pagination.start || 0;
            number = tableState.pagination.number || 10;
             if($scope.currentJob!=undefined && $scope.currentJob.jobName!=undefined)
                getInstances(start,number,tableState);
        };

        $scope.showInstances = function showInstances(row){
            var index = $scope.displayed.indexOf(row);
            if ($scope.oldindex!=undefined && $scope.oldindex != index)
                $scope.displayed[$scope.oldindex].showDetail = false;
            row.showDetail = !row.showDetail;
            $scope.currentJob = row;
            $scope.currentJob.tableState = $scope.tableState[index];
            $scope.pagingInstances($scope.currentJob.tableState);
            $scope.oldindex = index;
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
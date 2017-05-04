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
    controllers.controller('EditDataAssetCtrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route', '$routeParams', '$filter', function ($scope, $http, $config, $location, toaster, $timeout, $route, $routeParams, $filter) {
        $scope.assetId = $routeParams.assetId;
        $scope.currentStep = 1;
        $scope.form = {};
        $scope.form.basic = {};

        // $scope.platformOptions = ['Teradata', 'Apollo'];
        // $scope.systemOptions = ['Sojourner', 'SiteSpeed', 'Bullseye', 'PDS', 'GPS'];
        $scope.systemOptions = $filter('strarr')('modelsystem');//['Bullseye', 'GPS', 'Hadoop', 'PDS', 'IDLS', 'Pulsar', 'Kafka'];
        $scope.assetTypeOptions = ['hdfsfile', 'hivetable'];

        $scope.formatTypeOptions = ['yyyyMMdd', 'yyyy-MM-dd','HH'];

        $scope.$on('$viewContentLoaded', function() {
            $scope.$emit('initReq');
            resizeWindow();

        });

        $scope.$on('resizeHandler', function(e) {
            if ($route.current.$$route.controller == "EditDataAssetCtrl") {
                resizeWindow();
            }
        });

        function resizeWindow() {
                    $('.formStep').height(window.innerHeight  -  $('.formStep').offset().top - $('#footerwrap').outerHeight());
                    $('fieldset').height(window.innerHeight  -  $('fieldset').offset().top - $('#footerwrap').outerHeight()- $('.btn-container').height() -80);
                    $('.y-scrollable').css({
                        'max-height': $('fieldset').height()
                    });

        }

        // var dbtree = $config.uri.dbtree;
        // $http.get(dbtree).success(function(data) {
        //     $scope.platformOptions = data;
        //     $scope.getDataAsset();
        // });

        var assetUri = $config.uri.getdataasset + '/' + $scope.assetId;
        $scope.getDataAsset = function(){
            $http.get(assetUri).success(function(data) {
                $scope.form.basic.assetName = data.assetName;
                $scope.form.basic.path = data.assetHDFSPath;
                $scope.form.basic.type = $scope.assetTypeOptions.indexOf(data.assetType).toString();
                $scope.form.basic.platform = data.platform;
                //$scope.getSystemOptions($scope.form.basic.platform);
                $scope.form.basic.system = $scope.getSystemIndex(data.system).toString();
                $scope.form.basic.schema = data.schema;
                $scope.form.basic.partitions = data.partitions;
            });
        };

        $scope.getDataAsset();

        // $scope.getPlatformIndex = function(platform) {
        //     for (var i = 0; i < $scope.platformOptions.length; i++) {
        //         if($scope.platformOptions[i].platform == platform){
        //             return i;
        //         }
        //     };
        //     return -1;
        // };

        $scope.getSystemIndex = function(system) {
            for (var i = 0; i < $scope.systemOptions.length; i++) {
                if($scope.systemOptions[i] == system) {
                    return i;
                }
            };
            return -1;
        }

        // $scope.getSystemOptions = function(platformIndex){
        //     if(platformIndex==undefined){
        //         $scope.systemOptions = [];
        //     }else{
        //         $scope.systemOptions = $scope.platformOptions[platformIndex].systems;
        //     }
        // };

        $scope.updateHdfsPath = function(typeIndex){
            if(typeIndex != 0 ){
                $scope.form.basic.path = '';
            }
        };

	    $scope.addSchema = function() {
	        $scope.form.basic.schema.push({
	            name: '',
	            type: 'string',
	            desc: '',
	            sample: ''
	        });
	    };

      $scope.addPatitionColumn = function() {
	        $scope.form.basic.partitions.push({
	            name: '',
	            format: "yyyyMMdd"
	        });
	    };

	    $scope.deleteSchema = function(index) {
	        $scope.form.basic.schema.splice(index, 1);
	    };

      $scope.deletePartition = function(index) {
	        $scope.form.basic.partitions.splice(index, 1);
	    };

	    // Initial Value
        $scope.form = {
            basic: {
                schema: [{
                    name: '',
                    type: 'string',
                    desc: '',
                    sample: ''
                }],
                partitions: []
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
                    form.$setPristine();
                    this.data={
                      basic: this.basic,
                      extra: {publishUrl: this.publishUrl}
                    };



                    $('#confirm-pu').modal('show');
                }
            },

            save: function() {
                $('#confirm-pu').on('hidden.bs.modal', function(e) {
                    $('#confirm-pu').off('hidden.bs.modal');
                    $location.path('/dataassets');
                    $scope.$apply();
                });

                console.log(JSON.stringify($scope.form.basic));
                var msg = {
                	'system' : $scope.systemOptions[$scope.form.basic.system],
                	'assetType' : $scope.assetTypeOptions[$scope.form.basic.type],
                    'assetName' : $scope.form.basic.assetName,
                	'assetHDFSPath' : $scope.form.basic.path,
                	'platform' : $scope.form.basic.platform,
                	'schema' : $scope.form.basic.schema,
                    'owner' : $scope.form.basic.owner,
                    'partitions' : $scope.form.basic.partitions
                }

                $http.put($config.uri.updatedataasset, msg).success(function(data) {
                	if(data.result=='success')
                	{
                      	$('#confirm-pu').modal('hide');
                    }
                	else
                	{
                		errorMessage(0, data.result);
                	}
                });
            },

        };

        var errorMessage = function(i, msg) {
            var errorMsgs = ['Please fill in all required fields'];
            if (!msg) {
                toaster.pop('error', 'Error', errorMsgs[i - 1]);
            } else {
                toaster.pop('error', 'Error', msg);
            }
        };

    }]);
});

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
    controllers.controller('CreateDataAssetCtrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route', '$filter', function ($scope, $http, $config, $location, toaster, $timeout, $route, $filter) {
        $scope.currentStep = 1;

        // $scope.platformOptions = ['Teradata', 'Apollo'];
        // $scope.systemOptions = ['Sojourner', 'SiteSpeed', 'Bullseye', 'PDS', 'GPS'];
        //$scope.assetTypeOptions = ['hdfsfile', 'hivetable'];
        $scope.assetTypeOptions = ['hivetable'];

        $scope.formatTypeOptions = ['yyyyMMdd', 'yyyy-MM-dd','HH'];

        $scope.regex = '^\\/(?:[0-9a-zA-Z\\_\\-\\.]+\\/?)+';

        // $scope.regex = new RegExp('^\\/(?:[0-9a-zA-Z\\_\\-\\.]+\\/?)+');

        var allModels = $config.uri.dbtree;
        $http.get(allModels).success(function(data) {
            $scope.platformOptions = data;
        });

        //$scope.platformOptions = [{"id":null,"platform":"Teradata","systems":[{"id":null,"name":"gdw_tables","assets":[{"id":108,"name":"dw_bid"},{"id":109,"name":"dw_trans"}]}]},{"id":null,"platform":"Apollo","systems":[{"id":null,"name":"Sojourner","assets":[{"id":22,"name":"ubi_event"}]},{"id":null,"name":"SiteSpeed","assets":[{"id":21,"name":"sitespeed"}]},{"id":null,"name":"PDS","assets":[{"id":20,"name":"last_categories_accessed"}]},{"id":null,"name":"Bullseye","assets":[{"id":1,"name":"be_view_event_queue"},{"id":2,"name":"be_search_event_queue"},{"id":3,"name":"be_item_watch_event_queue"},{"id":4,"name":"be_bid_event_queue"},{"id":5,"name":"be_transaction_event_queue"},{"id":6,"name":"dmg"},{"id":7,"name":"loyaltysgmnt"},{"id":8,"name":"cust_dna_cat_score"},{"id":9,"name":"badge_interest"},{"id":10,"name":"cust_dna_vq_feed"},{"id":11,"name":"user_dna"},{"id":12,"name":"adchoice_user_pref"},{"id":13,"name":"cust_dna_vq_cat_feed"},{"id":14,"name":"cpcp_dealsv17"},{"id":15,"name":"bbe_tbl_trx"},{"id":17,"name":"bbe_tbl_neg"},{"id":16,"name":"bbe_tbl_neu"},{"id":19,"name":"rtm_segment_dict"},{"id":18,"name":"bbe_tbl_pos"}]}]}];
        $scope.systemOptions = $filter('strarr')('modelsystem');//['Bullseye', 'GPS', 'Hadoop', 'PDS', 'IDLS', 'Pulsar', 'Kafka'];

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

        $scope.$on('$viewContentLoaded', function() {
            $scope.$emit('initReq');
            resizeWindow();
        });

        $scope.$on('resizeHandler', function(e) {
            if ($route.current.$$route.controller == "CreateDataAssetCtrl") {
                resizeWindow();
            }
        });

        function resizeWindow() {
                    $('.formStep').height(window.innerHeight  -  $('.formStep').offset().top - $('#footerwrap').outerHeight()-20);
                    $('fieldset').height(window.innerHeight  -  $('fieldset').offset().top - $('#footerwrap').outerHeight()- $('.btn-container').height() -80);
                    $('.y-scrollable').css({
                        'max-height': $('fieldset').height()
                    });

        }

	    // Initial Value
        $scope.form = {
            basic: {
                platform: 'Apollo',
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
                      basic: this.basic
                    };
                    this.data.basic.path += this.data.basic.path.substring(this.data.basic.path.length-1)=="/"?'':'/';
                    //this.data.basic.path += (this.basic.folderFormat==undefined?"":this.basic.folderFormat);



                    $('#confirm-pu').modal('show');
                }
            },

            save: function() {


                var msg = {
                	'system' : $scope.systemOptions[$scope.form.basic.system],
                	'assetType' : $scope.assetTypeOptions[$scope.form.basic.type],
                  'assetName' : $scope.form.basic.assetName,
                	'assetHDFSPath' : $scope.form.data.basic.path + ($scope.form.data.basic.folderFormat==undefined?"":$scope.form.data.basic.folderFormat),
                	'platform' : $scope.form.basic.platform,
                	'schema' : $scope.form.basic.schema,
                  'partitions' : $scope.form.basic.partitions,
                  'owner' : $scope.form.basic.owner
                }

                $http.post($config.uri.adddataasset, msg).success(function() {
                    $('#confirm-pu').on('hidden.bs.modal', function(e) {
                        $('#confirm-pu').off('hidden.bs.modal');
                        $location.path('/dataassets');
                        $scope.$apply();
                    });

	                  $('#confirm-pu').modal('hide');

                }).error(function(data){
                  toaster.pop('error', 'Save data asset failed, please try again!', data.message);
                });
            },

        };

        var errorMessage = function(i, msg) {
            var errorMsgs = ['Please input valid values'];
            if (!msg) {
                toaster.pop('error', 'Error', errorMsgs[i - 1], 0);
            } else {
                toaster.pop('error', 'Error', msg, 0);
            }
        };

    }]);
});

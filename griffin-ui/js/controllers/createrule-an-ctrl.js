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
    controllers.controller('CreateRuleANCtrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route', '$filter', function($scope, $http, $config, $location, toaster, $timeout, $route, $filter) {
        console.log('CreateRuleANCtrl');
        $scope.currentStep = 1;

        var dbtreeUrl = $config.uri.dbtree;
        var schemaDefinitionUrl = $config.uri.schemadefinition;

        $http.get(dbtreeUrl).success(function(data) {
            var dbList = [];
            if (data && data.length > 0) {
                data.forEach(function(db) {
                    var dbNode = {
                        name: db.platform,
                        l1: true,
                        children: []
                    };
                    dbList.push(dbNode);
                    if (db.systems && db.systems.length > 0) {
                        db.systems.forEach(function(system) {
                            var dsNode = {
                                name: system.name,
                                l2: true,
                                children: []
                            };
                            dbNode.children.push(dsNode);
                            dsNode.parent = db;


                            if (system.assets && system.assets.length > 0) {
                                system.assets.sort(function(a, b){
                                  return (a.name<b.name?-1:(a.name>b.name?1:0));
                                });
                                system.assets.forEach(function(schema) {
                                    var schemaNode = {
                                        id: schema.id,
                                        name: schema.name,
                                        l3: true
                                    };
                                    schemaNode.parent = dsNode;
                                    dsNode.children.push(schemaNode);
                                });
                            }
                        });
                    }

                });
            }
            $scope.dbList = dbList;
        });


        //trigger after select schema for src
        $scope.$watch('currentNode', function(newValue) {
            // $scope.schemaCollection = null;
            if (newValue && newValue.l3) {

                var sysName = newValue.parent.name;
                $scope.form.basic.system = $filter('stridx')(sysName, 'modelsystem') + '';

                //retrieve the schema definition and display the table
                $http.get(schemaDefinitionUrl + '/' + newValue.id).success(function(data) {
                    $scope.schemaCollection = data.schema;
                });
            }
        });




        $scope.$on('$viewContentLoaded', function() {
            $scope.$emit('initReq');
            resizeWindow();
        });

        $scope.$on('resizeHandler', function(e) {
            if ($route.current.$$route.controller == "CreateRuleANCtrl") {
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
                        'max-height': $('fieldset').height() - $('.add-dataset').outerHeight()
                    });
                    //error message box position
                    $('.formula-text-mid').css({
                        'padding-top': (($('.formula-text-up').height() - $('.formula-text-mid').height()) + $('.formula-text-mid').height() / 2) + 'px'
                    });

        }

        $scope.ruleTypes = $filter('strarr')('modeltype');//['Accuracy', 'Validity', 'Anomaly Detection', 'Publish Metrics'];
        $scope.scheduleTypes = $filter('strarr')('scheduletype');//['Daily', 'Weekly', 'Monthly', 'Hourly'];
        $scope.ruleSystems = $filter('strarr')('modelsystem');//['Bullseye', 'GPS', 'Hadoop', 'PDS', 'IDLS', 'Pulsar', 'Kafka'];
        $scope.anTypes = ['', 'History Trend Detection', 'Bollinger Bands Detection', 'Deviation Detection'];
        // $scope.ruleType = function(index){
        //   var types = ['', 'Accuracy', 'Validity', 'Anomaly Detection', 'Publish Metrics'];
        //   return types[index];
        // }

        // $scope.scheduleType = function(index){
        //   var types = ['', 'Daily', 'Weekly', 'Monthly', 'Hourly'];
        //   return types[index];
        // }
        //
        // $scope.ruleSystem = function(index){
        //   var sys = ['', 'Bullseye', 'GPS', 'Hadoop', 'PDS', 'IDLS', 'Pulsar', 'Kafka'];
        //   return sys[index];
        // }

        // Initial Value
        $scope.form = {

            next: function(form) {

                if (formValidation()) {
                    // form.$setPristine();
                    nextStep();
                } else {
                    var field = null
                      , firstError = null ;
                    for (field in form) {
                        if (field[0] != '$') {
                            if (firstError === null  && !form[field].$valid) {
                                firstError = form[field].$name;
                            }

                            if (form[field].$pristine) {
                                form[field].$dirty = true;
                            }
                        }
                    }

                    //  angular.element('.ng-invalid[name=' + firstError + ']').focus();
                    errorMessage($scope.currentStep);
                }
            },
            prev: function(form) {
                //$scope.toTheTop();
                prevStep();
            },
            goTo: function(form, i) {
                if (parseInt($scope.currentStep) > parseInt(i)) {
                    // $scope.toTheTop();
                    goToStep(i);

                } else {
                    if (formValidation()) {
                        //   $scope.toTheTop();
                        if(i - parseInt($scope.currentStep) == 1){
                          goToStep(i);
                        }

                    } else {
                        errorMessage($scope.currentStep);
                    }
                }
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
                      extra: {
                        srcDb: $scope.currentNode.parent.parent.platform,
                        srcDataSet: $scope.currentNode.parent.name,
                        anType: this.anType
                      }
                    };

                    this.data.basic.dataaset = $scope.currentNode.name;
                    this.data.basic.dataasetId = $scope.currentNode.id;
                    // this.data.anType = this.anType;

                    $('#confirm-an').modal('show');
                }
            },

            save: function() {

                //::TODO: Need to save the data to backend with POST/PUT method
                console.log(JSON.stringify($scope.form.data));

                var newModel = $config.uri.newAnomalyModel;
                $http.post(newModel, this.data).success(function(data) {
                	// if(data.status=='0')
                	// {
	                  $('#confirm-an').on('hidden.bs.modal', function(e) {
	                      $('#confirm-an').off('hidden.bs.modal');
	                      $location.path('/rules');
	                      $scope.$apply();
	                  });
	                	$('#confirm-an').modal('hide');
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

        };


        var nextStep = function() {
            $scope.currentStep++;
            $timeout(function(){
                resizeWindow();
            }, 0);
        }
        ;
        var prevStep = function() {
            $scope.currentStep--;
            $timeout(function(){
                resizeWindow();
            }, 0);
        }
        ;
        var goToStep = function(i) {
            $scope.currentStep = i;
            $timeout(function(){
                resizeWindow();
            }, 0);
        }
        ;

        //validation only happens when going forward
        var formValidation = function(step) {
            //  return true;//for dev purpose
            if (step == undefined) {
                step = $scope.currentStep;
            }
            if (step == 1) {
                return $scope.currentNode?true:false;
            } else if (step == 2) {
                return true;
            } else if (step == 3) {
                return true;
                //::TODO: also need to check if duplicated mappings or empty mappings
            }

            return false;
        }

        var errorMessage = function(i, msg) {
            var errorMsgs = ['Please select one schema!','', 'please complete the form in this step before proceeding'];
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

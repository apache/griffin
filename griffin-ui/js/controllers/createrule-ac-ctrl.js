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
    controllers.controller('CreateRuleACCtrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route', '$filter', function($scope, $http, $config, $location, toaster, $timeout, $route, $filter) {
        console.log('Create rule controller');
        $scope.currentStep = 1;
        //for dev purpose
        //the selected columns for step 1
        $scope.selection = [];
        $scope.selectionTarget = [];
        $scope.selectionPK = [];
        $scope.mappings = [];
        $scope.matches = [];
        // $scope.pk = '';

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
            $scope.dbListTarget = angular.copy(dbList);
        });

        //trigger after select schema for src
        $scope.$watch('currentNode', function(newValue) {
            console.log(newValue);
            $scope.selection = [];
            $scope.selectedAll = false;

            // $scope.schemaCollection = null;
            if (newValue && newValue.l3) {

                //retrieve the schema definition and display the table
                $http.get(schemaDefinitionUrl + '/' + newValue.id).success(function(data) {

                    $scope.schemaCollection = data.schema;
                });
            }
        });

        $scope.selectNodeLabelTarget = function(selectedNode) {

            //remove highlight from previous node
            if ($scope.currentNodeTarget && $scope.currentNodeTarget.selected) {
                $scope.currentNodeTarget.selected = undefined;
            }

            if (selectedNode.children && selectedNode.children.length > 0) {
                $scope.selectNodeHead(selectedNode);
            } else {
                //set highlight to selected node
                selectedNode.selected = 'selected';
            }

            //set currentNode
            $scope.currentNodeTarget = selectedNode;
        }
        ;

        //trigger after select schema
        $scope.$watch('currentNodeTarget', function(newValue) {
            console.log(newValue);
            $scope.selectionTarget = [];
            $scope.selectedAllTarget = false;

            // $scope.schemaCollection = null;
            if (newValue) {

                //retrieve the schema definition and display the table
                if(newValue.l3){//System selected
                  var sysName = newValue.parent.name;
                  $scope.form.basic.system = $filter('stridx')(sysName, 'modelsystem') + '';
                  $http.get(schemaDefinitionUrl + '/' + newValue.id).success(function(data) {

                      $scope.schemaCollectionTarget = data.schema;
                  });
                }


            }
        });

        // $scope.$watch('selectionTarget', function(newValue) {
        //     if (newValue && newValue.length > 0) {
        //         $scope.pk = newValue[0];
        //         // console.log('-----$scope.pk: ' + $scope.pk);
        //     }
        // });

        //column selection

        $scope.toggleSelection = function toggleSelection($event) {
            var value = $event.target.value;
            var idx = $scope.selection.indexOf(value);
            // is currently selected
            if (idx > -1) {
                $scope.selection.splice(idx, 1);
                $scope.selectedAll = false;
            }
            // is newly selected

            else {
                $scope.selection.push(value);
            }
        }
        ;

        $scope.toggleAll = function() {
            if ($scope.selectedAll) {
                $scope.selectedAll = true;
            } else {
                $scope.selectedAll = false;
            }

            $scope.selection = [];
            angular.forEach($scope.schemaCollection, function(item) {
                item.selected = $scope.selectedAll;
                if ($scope.selectedAll) {
                    $scope.selection.push($scope.currentNode.name + '.' + item.name);
                }
            });
        }

        $scope.toggleSelectionTarget = function($event) {
            var value = $event.target.value;
            var idx = $scope.selectionTarget.indexOf(value);
            // is currently selected
            if (idx > -1) {
                $scope.selectionTarget.splice(idx, 1);
                $scope.selectedAllTarget = false;
            }
            // is newly selected

            else {
                $scope.selectionTarget.push(value);
            }
        }
        ;

        $scope.toggleAllTarget = function() {
            if ($scope.selectedAllTarget) {
                $scope.selectedAllTarget = true;
            } else {
                $scope.selectedAllTarget = false;
            }

            $scope.selectionTarget = [];
            angular.forEach($scope.schemaCollectionTarget, function(item) {
                item.selected = $scope.selectedAllTarget;
                if ($scope.selectedAllTarget) {
                    $scope.selectionTarget.push($scope.currentNodeTarget.name + '.' + item.name);
                }
            });
        }

        $scope.toggleSelectionPK = function($event) {
            var value = $event.target.value;
            var idx = $scope.selectionPK.indexOf(value);
            // is currently selected
            if (idx > -1) {
                $scope.selectionPK.splice(idx, 1);
                $scope.selectedAllPK = false;
            }
            // is newly selected

            else {
                $scope.selectionPK.push(value);
            }
        };

        $scope.toggleAllPK = function() {
            $scope.selectionPK = [];
            if ($scope.selectedAllPK) {
                $scope.selectedAllPK = true;
                angular.forEach($scope.selectionTarget, function(item) {
                    $scope.selectionPK.push(item);
                });
            } else {
                $scope.selectedAllPK = false;
            }
        };

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

        $scope.ruleTypes = $filter('strarr')('modeltype');//['Accuracy', 'Validity', 'Anomaly Detection', 'Publish Metrics'];
        $scope.scheduleTypes = $filter('strarr')('scheduletype');//['Daily', 'Weekly', 'Monthly', 'Hourly'];
        $scope.ruleSystems = $filter('strarr')('modelsystem');//['Bullseye', 'GPS', 'Hadoop', 'PDS', 'IDLS', 'Pulsar', 'Kafka'];
        $scope.matchFunctions = ['LENGTH', 'LOWER', 'UPPER', 'TRIM'];

        // $scope.ruleType = function(index){
        //   var types = ['', 'Accuracy', 'Validity', 'Anomaly Detection', 'Publish Metrics'];
        //   return types[index];
        // }
        //
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
                    //  $location.path('/rules');
                    form.$setPristine();
                    this.data={
                      basic: this.basic,
                      extra: {
                        srcDb: $scope.currentNode.parent.parent.platform,
                        srcDataSet: $scope.currentNode.parent.name,
                        targetDb: $scope.currentNodeTarget.parent.parent.platform,
                        targetDataSet: $scope.currentNodeTarget.parent.name
                      },
                      mappings:[]
                    };

                    this.data.basic.dataaset = $scope.currentNodeTarget.name;
                    this.data.basic.dataasetId = $scope.currentNodeTarget.id;

                    for(var i =0; i < $scope.selectionTarget.length; i ++){
                      this.data.mappings.push({target:$scope.selectionTarget[i],
                                      src:$scope.mappings[i],
                                      matchMethod: $scope.matches[i],
                                      isPk: ($scope.selectionPK.indexOf($scope.selectionTarget[i])>-1)?true:false});
                    }

                    $('#confirm').modal('show');
                }
            },

            save: function() {


                //::TODO: Need to save the data to backend with POST/PUT method
                console.log(JSON.stringify($scope.form.data));

                var newModel = $config.uri.newAccuracyModel;
                $http.post(newModel, this.data).success(function(data) {
                	// if(data.status=='0')
                	// {
	                  $('#confirm').on('hidden.bs.modal', function(e) {
	                      $('#confirm').off('hidden.bs.modal');
	                      $location.path('/rules').replace();
	                      $scope.$apply();
	                  });
	                	$('#confirm').modal('hide');
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

            // reset: function() {
            //
            // },
            //
            // ruleType: 1
        }


        var nextStep = function() {
            $scope.currentStep++;
            $timeout(function(){
                resizeWindow();
            }, 0);
        }

        var prevStep = function() {
            $scope.currentStep--;
            $timeout(function(){
                resizeWindow();
            }, 0);
        }

        var goToStep = function(i) {
            $scope.currentStep = i;

            $timeout(function(){
                resizeWindow();
            }, 0);
        }

        $scope.$watch('currentStep', function(newValue){
          if(newValue == 3){ //step 3
            if($scope.selectionTarget.length < $scope.mappings.length){
              $scope.mappings.splice($scope.selectionTarget.length);
            }
          }
        });

        var existDuplicatedElement = function(arr){

            for (var i = 0; i < arr.length; i++) {
                for (var j = i+1; j < arr.length; j++) {
                    if(arr[i] == arr[j]){
                        return true;
                    }
                };
            };
            return false;
        };

        //validation only happens when going forward
        var formValidation = function(step) {
            //  return true;//for dev purpose
            if (step == undefined) {
                step = $scope.currentStep;
            }
            if (step == 1) {
                return $scope.selection && $scope.selection.length > 0;
            } else if (step == 2) {
                return ($scope.selectionTarget && $scope.selectionTarget.length > 0)//at least one target is selected
                        && !(($scope.currentNode.name == $scope.currentNodeTarget.name)&&($scope.currentNode.parent.name == $scope.currentNodeTarget.parent.name));//target and source should be different
            } else if (step == 3) {
                return $scope.selectionTarget && $scope.selectionTarget.length == $scope.mappings.length
                        && $scope.mappings.indexOf('') == -1 && !existDuplicatedElement($scope.mappings)
                        && $scope.selectionPK && $scope.selectionPK.length>0;
            } else if (step == 4) {

            }

            return false;
        }

        var errorMessage = function(i, msg) {
            var errorMsgs = ['Please select at least one attribute!', 'Please select at least one attribute in target, make sure target is different from source!', 'Please make sure to map each target to a unique source and select at least one primary key.', 'please complete the form in this step before proceeding'];
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

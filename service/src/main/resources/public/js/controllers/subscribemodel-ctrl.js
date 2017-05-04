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
    controllers.controller('SubscribeModelCtrl', ['$scope', '$http', '$config', '$location', 'toaster', '$timeout', '$route', '$filter', function($scope, $http, $config, $location, toaster, $timeout, $route, $filter) {
        var dbtreeUrl = $config.uri.dbtree;
        var schemaDefinitionUrl = $config.uri.schemadefinition;
        var subscribeUrl = $config.uri.getsubscribe;
        var allModelsUrl = $config.uri.allModels;

        $scope.ruleSystems = $filter('strarr')('modelsystem');
        $scope.ruleTypes = $filter('strarr')('modeltype');

        $http.get(dbtreeUrl).success(function(data) {
            var dbList = [];
            if (data && data.length > 0) {
                data.forEach(function(db) {
                    var dbNode = {
                        name: db.platform,
                        fullname: db.platform,
                        l1: true,
                        children: []
                    };
                    dbList.push(dbNode);
                    if (db.systems && db.systems.length > 0) {
                        db.systems.forEach(function(system) {
                            var dsNode = {
                                name: system.name,
                                fullname: db.platform + '#' + system.name,
                                l2: true,
                                children: []
                            };
                            dbNode.children.push(dsNode);
                            dsNode.parent = dbNode;


                            if (system.assets && system.assets.length > 0) {
                                system.assets.sort(function(a, b){
                                  return (a.name<b.name?-1:(a.name>b.name?1:0));
                                });
                                system.assets.forEach(function(schema) {
                                    var schemaNode = {
                                        id: schema.id,
                                        name: schema.name,
                                        fullname: db.platform + '#' + system.name + '#' + schema.name,
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

            $http.get(subscribeUrl + $scope.ntAccount).success(function(data){
                $scope.oldSubscription = data;
                //updateModelsTable();
                initializeModelsTable();
                // $scope.changeCB();
            });





        });

        $http.get(allModelsUrl).success(function(data){
            $scope.allModels = data;
        });

        var findPlatformIndex = function(pl, subscribes){
            for (var i = 0; i < subscribes.length; i++) {
                if(subscribes[i].platform == pl){
                    return i;
                }
            };
            return -1;
        };

        var findSystemIndex = function(sys, systems){
            for (var i = 0; i < systems.length; i++) {
                if(systems[i].system == sys){
                    return i;
                }
            };
            return -1;
        };

        var getSubscribes = function(){
            var subscribes = [];
            var checkedList = angular.element('input:checked');
            angular.forEach(checkedList, function(item){
                var value = item.value;
                var res = value.split('#');
                if(res.length == 2) {
                    var pl = res[0];
                    var sys = res[1];
                    var plIndex = findPlatformIndex(pl, subscribes);
                    if(plIndex == -1) {
                        var newPlatform = {
                            "platform": pl,
                            "selectAll": false,
                            "systems": [{
                                "system": sys,
                                "selectAll": true,
                                "dataassets": []
                            }]
                        }
                        subscribes.push(newPlatform);
                    } else{
                        var sysIndex = findSystemIndex(sys, subscribes[plIndex].systems);
                        if(sysIndex == -1) {
                            var newSys = {
                                "system": sys,
                                "selectAll": true,
                                "dataassets": []
                            };
                            subscribes[plIndex].systems.push(newSys);
                        }else{
                            subscribes[plIndex].systems[sysIndex].selectAll = true;
                        }
                    }
                } else if(res.length == 3){
                    var pl = res[0];
                    var sys = res[1];
                    var asset = res[2];
                    var plIndex = findPlatformIndex(pl, subscribes);
                    if(plIndex == -1) {
                        var newPlatform = {
                            "platform": pl,
                            "selectAll": false,
                            "systems":[{
                                "system": sys,
                                "selectAll": false,
                                "dataassets": [asset]
                            }]

                        };
                        subscribes.push(newPlatform);
                    } else{
                        var sysIndex = findSystemIndex(sys, subscribes[plIndex].systems);
                        if(sysIndex == -1) {
                            var newSys = {
                                "system": sys,
                                "selectAll": false,
                                "dataassets": [asset]
                            };
                            subscribes[plIndex].systems.push(newSys);
                        }else{
                            subscribes[plIndex].systems[sysIndex].dataassets.push(asset);
                        }
                    }
                }
            })
            return subscribes;
        };

        $scope.$on('$viewContentLoaded', function() {
            $scope.$emit('initReq');
            resizeWindow();
        });

        $scope.$on('resizeHandler', function(e) {
            if ($route.current.$$route.controller == "SubscribeModelCtrl") {
                resizeWindow();
            }
        });

        function resizeWindow() {
            console.log(window.innerHeight);
            console.log($('.formStep').offset().top);
            console.log($('#footerwrap').outerHeight());
                    $('.formStep').innerHeight(window.innerHeight  -  $('.formStep').offset().top - $('#footerwrap').outerHeight());
                    $('fieldset').height(window.innerHeight  -  $('fieldset').offset().top - $('#footerwrap').outerHeight()- $('.btn-container').height() -80);
                    $('.y-scrollable').css({
                        'max-height': $('fieldset').height()
                    });

        }

        $scope.form = {
            submit: function(form) {
                var subscribes = angular.copy(getSubscribes());
                var sid = ($scope.oldSubscription=="")?null:$scope.oldSubscription._id;
                var requestData = {
                    "id": sid,
                    // "id": $scope.ntAccount,
                    "ntaccount": $scope.ntAccount,
                    "subscribes": subscribes
                };
                this.data = requestData;
                // $('#confirm').modal('show');
                var newsubscribeUrl = $config.uri.newsubscribe;
                $http.post(newsubscribeUrl, this.data).success(function(data){
                    $location.path('/mydashboard').replace();
                }).error(function(data){
                    console.log("Fail to new subscription.");
                });
            },
            save: function(form) {
                var newsubscribeUrl = $config.uri.newsubscribe;
                $http.post(newsubscribeUrl, this.data).success(function(data){
                    $('#confirm').on('hidden.bs.modal', function(e) {
                        $('#confirm').off('hidden.bs.modal');
                        $location.path('/mydashboard').replace();
                        $scope.$apply();
                    });
                    $('#confirm').modal('hide');
                }).error(function(data){
                    console.log("Fail to new subscription.");
                });
            }
        };

        var findAssetId = function(assetname, sysname, pfname, dbList){
            for (var i = 0; i < dbList.length; i++) {
                var platform = dbList[i];
                if(platform.name == pfname){
                    for (var j = 0; j < platform.children.length; j++) {
                        var sys = platform.children[j];
                        if(sys.name == sysname){
                            for (var k = 0; k < sys.children.length; k++) {
                                var asset = sys.children[k];
                                if(assetname==asset.name) {
                                    return asset.id;
                                }
                            };
                        }
                    };
                }
            };
            return -1;
        };

          var getModelsInfo = function(assetname, sysname, allModels) {
            var result = [];
            angular.forEach(allModels, function(model) {
                if(model.assetName == assetname
                    && $scope.ruleSystems[model.system] == sysname) {
                    var item = {
                        'systemName': sysname,
                        'assetName': assetname,
                        'modelName': model.name,
                        'modelType': $scope.ruleTypes[model.type],
                        'desc': model.description
                    }
                    result.push(item);
                }
            });
            return result;
          }

          var initializeModelsTable = function(){
            if($scope.oldSubscription == undefined || $scope.oldSubscription == "" || $scope.oldSubscription.subscribes == undefined){
                return;
            }
            $scope.schemaCollection = [];
            var subscribes = $scope.oldSubscription.subscribes;
            angular.forEach(subscribes, function(platform){
                angular.forEach(platform.systems, function(sys){
                    angular.forEach(sys.dataassets, function(asset){
                        var models = getModelsInfo(asset, sys.system, $scope.allModels);
                        $scope.schemaCollection = $scope.schemaCollection.concat(models);
                    })
                })
            });

          };

          var updateModelsTable = function(){
            $scope.schemaCollection = [];
            var subscribes = angular.copy(getSubscribes());
            angular.forEach(subscribes, function(platform){
                angular.forEach(platform.systems, function(sys){
                    angular.forEach(sys.dataassets, function(asset){
                        var models = getModelsInfo(asset, sys.system, $scope.allModels);
                        $scope.schemaCollection = $scope.schemaCollection.concat(models);
                    })
                })
            });

          };

        $scope.isChecked = function(node) {
            if($scope.oldSubscription == undefined || $scope.oldSubscription == "" || $scope.oldSubscription.subscribes == undefined){
                return false;
            } else {
                var oldSubscribes = $scope.oldSubscription.subscribes;
                if(node.l2){
                    for (var i = 0; i < oldSubscribes.length; i++) {
                        var subscribe = oldSubscribes[i];
                        if(subscribe.platform == node.parent.name){
                            for (var j = 0; j < subscribe.systems.length; j++) {
                                var sys = subscribe.systems[j];
                                if(sys.system == node.name && sys.selectAll){
                                    return true;
                                }
                            };
                        }
                    };
                }else if(node.l3){
                    for (var i = 0; i < oldSubscribes.length; i++) {
                        var subscribe = oldSubscribes[i];
                        if(subscribe.platform == node.parent.parent.name){
                            for (var j = 0; j < subscribe.systems.length; j++) {
                                var sys = subscribe.systems[j];
                                if(sys.system == node.parent.name){
                                    for (var k = 0; k < sys.dataassets.length; k++) {
                                        var asset = sys.dataassets[k];
                                        if(asset == node.name) {
                                            return true;
                                        }
                                    };
                                }
                            };
                        }
                    };
                }
            }
            return false;
        };

        // setTimeout(function() {
        //     updateModelsTable();
        // }, 1000);

        $scope.changeCB = function(node, evt) {
            // $('input[type="checkbox"]').change(function(e) {

                  var checked = $(evt.target).prop("checked"),
                      container = $(evt.target).parent(),
                      siblings = container.siblings();

                  container.find('input[type="checkbox"]').prop({
                    indeterminate: false,
                    checked: checked
                  });


                   function checkSiblings(el) {

                    var parent = el.parent().parent().parent(),
                        all = true;

                    el.siblings().each(function() {
                      return all = ($(this).children('input[type="checkbox"]').prop("checked") === checked);
                    });

                    if (all && checked) {

                      parent.children('input[type="checkbox"]').prop({
                        indeterminate: false,
                        checked: checked
                      });

                      checkSiblings(parent);

                    } else if (all && !checked) {

                      parent.children('input[type="checkbox"]').prop("checked", checked);
                      parent.children('input[type="checkbox"]').prop("indeterminate", (parent.find('input[type="checkbox"]:checked').length > 0));
                      checkSiblings(parent);

                    } else {

                      el.parents("li").children('input[type="checkbox"]').prop({
                        indeterminate: true,
                        checked: false
                      });

                    }

                  };

                  checkSiblings(container);

                  updateModelsTable();
            //}
            // setTimeout(function() {
            //     updateModelsTable();
            // }, 300);
            //updateModelsTable();
        }



    }]);
});

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
define(['./app'], function(app) {
    'use strict';

    return app.config(['$routeProvider', '$httpProvider', function($routeProvider, $httpProvider) {

        $routeProvider.when('/', {
            redirectTo: '/health'
        });

        $routeProvider.when('/health', {
            templateUrl: '/pages/health/health.html',
            controller: 'HealthCtrl'
        });

        $routeProvider.when('/rules', {
            templateUrl: '/pages/rules/rules.html',
            controller: 'RuleCtrl'
        });

        $routeProvider.when('/createrule0', {
            templateUrl: '/pages/rules/createrule0.html',
            controller: 'CreateRule0Ctrl'
        });

        $routeProvider.when('/createrule-ac', {
            templateUrl: '/pages/rules/createrule-ac.html',
            controller: 'CreateRuleACCtrl'
        });

        $routeProvider.when('/createrule-va', {
            templateUrl: '/pages/rules/createrule-va.html',
            controller: 'CreateRuleVACtrl'
        });

        $routeProvider.when('/createrule-an', {
            templateUrl: '/pages/rules/createrule-an.html',
            controller: 'CreateRuleANCtrl'
        });

        $routeProvider.when('/createrule-pu', {
            templateUrl: '/pages/rules/createrule-pu.html',
            controller: 'CreateRulePUCtrl'
        });

        $routeProvider.when('/viewrule/:modelname/:modeltype', {
            templateUrl: '/pages/rules/viewrule.html',
            controller: 'ViewRuleCtrl'
        });

        $routeProvider.when('/viewrule/:modelname', {
            templateUrl: '/pages/rules/viewrule.html',
            controller: 'ViewRuleCtrl'
        });

        $routeProvider.when('/metrics', {
            templateUrl: '/pages/metrics/dashboard.html',
            controller: 'MetricsCtrl'
        });

        $routeProvider.when('/metrics/:sysName', {
            templateUrl: '/pages/metrics/dashboard.html',
            controller: 'MetricsCtrl'
        });

        $routeProvider.when('/dataassets', {
            templateUrl: '/pages/dataassets/dataassets.html',
            controller: 'DataAssetsCtrl'
        });

        $routeProvider.when('/dataassets/:assetId', {
            templateUrl: '/pages/dataassets/editdataasset.html',
            controller: 'EditDataAssetCtrl'
        });

        $routeProvider.when('/createdataasset', {
            templateUrl: '/pages/dataassets/createdataasset.html',
            controller: 'CreateDataAssetCtrl'
        });

        $routeProvider.when('/mydashboard', {
            templateUrl: '/pages/mydashboard/mydashboard.html',
            controller: 'MyDashboardCtrl'
        });

        $routeProvider.when('/subscribemodel', {
            templateUrl: '/pages/mydashboard/subscribemodel.html',
            controller: 'SubscribeModelCtrl'
        });

        $routeProvider.when('/undercons', {
            templateUrl: '/pages/template/undercons.html',
            controller: null
        });

        $routeProvider.otherwise({
            redirectTo: '/'
        });

        // $routeProvider.otherwise(function(){
        //     console.log("otherwise");
        // });


        // $locationProvider.html5Mode({
        //   enabled: true,
        //   requireBase: false
        // });

        $httpProvider.interceptors.push(function($q, $location, $rootScope, $timeout, $window, $config) {
            var requestArray = {};

            return {
                "request": function(config) {
                    if (isSpinnerRequired($config, config.url)) {
                        //There's a bug in angular-spinner, the event was sent to the component too early while the directive hasn't been compiled completely so that the spinner object hasn't be created
                        //So let's delay 200 ms to browadcast the event
                        $timeout(function() {
                            if (!requestArray[config.url])
                                requestArray[config.url] = 0;

                            if (requestArray[config.url] >= 0) {
                                $rootScope.$broadcast('us-spinner:spin', false);
                                requestArray[config.url]++;
                            } else {
                                requestArray[config.url] = 0;
                            }

                            console.log(config.url + ": " + requestArray[config.url]);
                        }, 200);

                    }
                    return config || $q.when(config);
                },
                "response": function(response) {
                    if (isSpinnerRequired($config, response.config.url)) {
                        $timeout(function() {
                            if (!requestArray[response.config.url])
                                requestArray[response.config.url] = 0;

                            $rootScope.$broadcast('us-spinner:stop', false);
                            requestArray[response.config.url]--;

                            console.log(response.config.url + ": " + requestArray[response.config.url]);
                        }, 0);
                    }
                    return response || $q.when(response);
                },

                "responseError": function(response) {

                    if (isSpinnerRequired($config, response.config.url)) {
                        $timeout(function() {
                            if (!requestArray[response.config.url])
                                requestArray[response.config.url] = 0;

                            $rootScope.$broadcast('us-spinner:stop', false);
                            requestArray[response.config.url]--;

                            console.log(response.config.url + ": " + requestArray[response.config.url]);
                        }, 0);

                    }
                    //if(isApiCall(Config, response.config.url)){
                    if (isSpinnerRequired($config, response.config.url)) {
                        $rootScope.error = response.status + " " + response.statusText;
                        $rootScope.errorMsg = response.data;
                        console.error(response.data);
                        if (response.status == 0 && !response.data) {
                            $window.alert("The connection to server failed, please ensure you've configured proxy to https://c2sproxy.vip.ebay.com/wpad.dat. If still fails, please contact DL-eBay-lshao-All@ebay.com");
                            // console.error("status: " + response.status);
                            // console.error("data: " + response.data);
                        }
                        //Disable error handling temporarily
                        //$location.path("error");
                    }

                    return $q.reject(response);

                }
            }

        });

    }]);
});

function isSpinnerRequired(Config, url){
    var names = [
        "dbtree",
        "getdataasset",
        "allModels",
        "getModel",
        "heatmap",
        "metricdetail",
        "dashboard",
        "getmydashboard",
        "getsubscribe"
    ];

    var uriArray = Config.uri;
    for(var i = 0; i < names.length; i ++){
        if(url.indexOf(uriArray[names[i]])>=0)
            return true;
    }

    return false;
}

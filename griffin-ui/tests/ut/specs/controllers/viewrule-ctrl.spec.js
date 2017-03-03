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
define(['angular', 'angularMocks', 'js/controllers/viewrule-ctrl'],
  function(angular, mocks, ViewRuleCtrl) {
    describe('Test /js/controllers/viewrule-ctrl.js', function(){
      	beforeEach(function(){
	        module('app.controllers');
	        module('app.services');
      	});
    	var $scope, $rootScope, $controller, $filter, $httpBackend, $config, $location, toaster, $timeout, $compile;

	    beforeEach(inject(function(_$rootScope_ , _$controller_, _$filter_, _$httpBackend_, _$config_, _$location_, _$timeout_, _$compile_){
	    	$rootScope = _$rootScope_;
	    	$controller = _$controller_;
	    	$filter = _$filter_;
	        $httpBackend = _$httpBackend_;
	        $config = _$config_;
	        $location = _$location_;
	        $timeout = _$timeout_;
	        toaster = {};
	        $compile = _$compile_;
	    }));

        beforeEach(function(){
          	$scope =  $rootScope.$new();
	        controller = $controller('ViewRuleCtrl', {$scope: $scope, toaster: toaster});
        });

        describe("if the controller of ViewRuleCtrl exists",function(){
        	it('controller exists', function(){
	          	expect(controller).toBeDefined();
	        });
        })

        describe("check if parameters are available",function(){
	        it('$scope.value and $config.value should be right', function(){
	          	expect($config.uri.rulemetric).toBeTruthy();
	        });
      	})

      	describe("httpGet $config.uri.rulemetric test",function(){
	        beforeEach(function(){
	            $httpBackend.when('GET', $config.uri.rulemetric).respond({"age": 16,"name": "li"});
	            $httpBackend.flush();
	        });

	        it('http response', function(){
	          // expect($scope.dbList).toBeTruthy();
	        });

	      	afterEach(function() {
	        	$httpBackend.verifyNoOutstandingExpectation();
	        	$httpBackend.verifyNoOutstandingRequest();
	      	});
	    })


    });
  }
)

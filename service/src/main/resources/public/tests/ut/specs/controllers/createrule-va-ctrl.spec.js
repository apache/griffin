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
define(['angular', 'angularMocks', 'js/controllers/createrule-va-ctrl'],
  function(angular, mocks, CreateRuleVACtrl) {
    describe('Test /js/controllers/createrule-va-ctrl.js', function(){
      	beforeEach(function(){
	        module('app.controllers');
	        module('app.services');
      	});
    	var $scope, $rootScope, $controller, $httpBackend, $config, $location, toaster, $timeout, $route;
	    beforeEach(inject(function(_$rootScope_ , _$controller_, _$httpBackend_, _$config_, _$location_, _$timeout_){
	    	$rootScope = _$rootScope_;
	    	$controller = _$controller_;
	        $httpBackend = _$httpBackend_;
	        $config = _$config_;
	        $location = _$location_;
	        $timeout = _$timeout_;
	        $route = {};
	        toaster = {};
	    }));

        beforeEach(function(){
          	$scope =  $rootScope.$new();
	        controller = $controller('CreateRuleVACtrl', {$scope: $scope, $route: $route, toaster: toaster });
        });

        describe("if the controller of CreateRuleVACtrl exists",function(){
        	it('controller exists', function(){
	          	expect(controller).toBeDefined();
	        });
        })

        describe("check if parameters are available",function(){

	        it('$scope.value and $config.value should be right', function(){
	          	expect($scope.currentStep).toBe(1);
	          	expect($config.uri.dbtree).toBeTruthy();
	          	expect($config.uri.schemadefinition).toBeTruthy();
	        });

	        it('$scope.ruleTypes should be right', function(){
	          	expect($scope.ruleTypes).toEqual(['Accuracy', 'Validity', 'Anomaly Detection', 'Publish Metrics']);
	        });

      	})

      	describe("check if form function are work",function(){

	        it('$scope.form is a object', function(){
	          expect(typeof $scope.form).toBe("object");
	        });

      	})

      	describe("$scope.form test",function(){

        	it('the type of $scope.form', function(){
	          	expect(typeof $scope.form).toBe("object");
	        });

	        it('the type of $scope.form.submit', function(){
	          	expect(typeof $scope.form.submit).toBe('function');
	        });

	        it('the type of $scope.form.save', function(){
	          	expect(typeof $scope.form.save).toBe('function');
	        });

        })

      	describe("httpGet $config.uri.dbtree test",function(){
	        beforeEach(function(){
	            $httpBackend.when('GET', $config.uri.dbtree).respond({"age": 16,"name": "li"});
	            $httpBackend.when('GET', $config.uri.schemadefinition).respond({"age": 15,"name": "wei"});
	        });

	        it('http response', function(){
	        	$httpBackend.flush();
	          	expect($scope.dbList).toBeTruthy();
	        });

	        it('test watch and http request', function(){
	   			$scope.currentNode = {"name":"ha"};
		      	$scope.$digest();

		      	$httpBackend.flush();
		      	expect($scope.schemaCollection.age).toBe(15);
	   		});

	      	afterEach(function() {
	        	$httpBackend.verifyNoOutstandingExpectation();
	        	$httpBackend.verifyNoOutstandingRequest();
	      	});
	    })

    });
  }
)

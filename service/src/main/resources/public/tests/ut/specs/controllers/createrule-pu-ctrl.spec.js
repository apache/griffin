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
define(['angular', 'angularMocks', 'js/controllers/createrule-pu-ctrl'],
  function(angular, mocks, CreateRulePUCtrl) {
    describe('Test /js/controllers/createrule-pu-ctrl.js', function(){
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
	        toaster = ['error', 'Error', "hello", "lisan"];/*test data*/
	    }));

        beforeEach(function(){
          	$scope =  $rootScope.$new();
	        controller = $controller('CreateRulePUCtrl', {$scope: $scope, $route: $route, toaster: toaster });
        });

        describe("if the controller of CreateRulePUCtrl exists",function(){
        	it('controller exists', function(){
	          	expect(controller).toBeDefined();
	        });
        })

        describe("$scope.value",function(){

        	it('$scope.currentStep == 1', function(){
	          	expect($scope.currentStep).toBe(1);
	        });

	        it('$scope.scheduleTypes', function(){
	          	expect($scope.scheduleTypes).toEqual(['Daily', 'Weekly', 'Monthly', 'Hourly']);
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

        /*describe("function errorMessage() test",function(){
        	// test function
        	it('errorMessage', function(){
	          	var message = ["hello"];
	          	var i = 2;
	          	var msg = true;
	          	$scope.errorMessage(i,msg);
	          	expect(toaster).toEqual(['error', 'Error',"hello"]);
	        });

        })*/

        describe('Testing $watch expressions', function() {
		    it('test using $digest', function() {
		    	var form = {};
		    	form.basic = {"name": "li"};
		    	form.publishUrl = "publishUrl";
		    	$scope.form = form;
		      	form.basic.name = "ha";
		      	$scope.$digest();
		      	form.basic.name = "la";
		      	$scope.$digest();

		      	expect($scope.form.publishUrl).toEqual('http://dq.vip.ebay.com/api/v1/publishmetric/la');

		    });
		});



    });
  }
)

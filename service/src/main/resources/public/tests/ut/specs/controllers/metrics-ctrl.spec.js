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
define(['angular', 'angularMocks', 'js/controllers/metrics-ctrl'],
  function(angular, mocks, MetricsCtrl) {
    describe('Test /js/controllers/metrics-ctrl.js', function(){
      	beforeEach(function(){
	        module('app.controllers');
	        module('app.services');
      	});
    	var $rootScope, $controller, $httpBackend, $config, $filter, $routeParams, $timeout, $compile, $route;
	    beforeEach(inject(function(_$rootScope_, _$controller_, _$httpBackend_, _$config_, _$filter_, _$timeout_, _$compile_){
	        $rootScope = _$rootScope_;
	        $controller = _$controller_;
	        $httpBackend = _$httpBackend_;
	        $config = _$config_;
	        $filter = _$filter_;
	        $routeParams = {};
	        // $routeParams.siteId = 'abc';
	        $timeout = _$timeout_;
	        $compile = _$compile_;
	        $route = {};
	    }));

    	describe("function test",function(){
	        var $scope;

	        beforeEach(function(){
	          	$scope =  $rootScope.$new();
		        controller = $controller('MetricsCtrl', {$scope: $scope, $route: $route, $routeParams: $routeParams});
	        });

	        it('controller exists', function(){
	        	// var controller = $controller('MetricsCtrl', {$scope: $scope, $routeParams: $routeParams});
	          	expect(controller).toBeDefined();
	        });

	        it('$config.uri.dashboard', function(){
	          	expect($config.uri.dashboard).toBeTruthy();
	        });

	        it('$scope.showBig works well', function(){
	          expect($scope.showBig).toBeDefined();
	        });

	        describe("http test",function(){
		        beforeEach(function(){
	                $httpBackend.when('GET', $config.uri.dashboard).respond({"age": 16,"name": "li"});
		            $httpBackend.flush();
		        });

		        it('http response', function(){
		          expect($scope.dashboard.age).toBe(16);
		        });

	          	afterEach(function() {
	            	$httpBackend.verifyNoOutstandingExpectation();
	            	$httpBackend.verifyNoOutstandingRequest();
	          	});
	        })

      	})
    });
  }
)

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
define(['angular', 'angularMocks', 'js/controllers/createrule0-ctrl'],
  function(angular, mocks, CreateRule0Ctrl) {
    describe('Test /js/controllers/createrule0-ctrl.js', function(){
      	beforeEach(function(){
	        module('app.controllers');
	        module('app.services');
      	});
    	var $scope, $rootScope, $controller, $httpBackend, $config, $location, toaster, $timeout;

	    beforeEach(inject(function(_$rootScope_ , _$controller_, _$httpBackend_, _$config_, _$location_, _$timeout_){
	    	$rootScope = _$rootScope_;
	    	$controller = _$controller_;
	        $httpBackend = _$httpBackend_;
	        $config = _$config_;
	        $location = _$location_;
	        $timeout = _$timeout_;
	        toaster = {};
	    }));

        beforeEach(function(){
          	$scope =  $rootScope.$new();
	        controller = $controller('CreateRule0Ctrl', {$scope: $scope, toaster: toaster });
        });

        describe("if the controller of CreateRule0Ctrl exists",function(){
        	it('controller exists', function(){
	          	expect(controller).toBeDefined();
	        });
        })

        describe("if the $scope.click exists",function(){

        	it('$scope.click', function(){
	          	expect($scope.click).toBeDefined();
	        });

	        it('should change location when setting it via click function', inject(function() {
		        var url = '/index';
		        $scope.click(url);
		        spyOn($location, 'path').and.returnValue(url);
		    }));

        })


    });
  }
)

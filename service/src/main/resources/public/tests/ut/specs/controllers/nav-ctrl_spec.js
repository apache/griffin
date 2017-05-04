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
define(['angular', 'angularMocks', 'js/controllers/nav-ctrl'],
  function(angular, mocks, navCtrl) {
    describe('Test /js/controllers/nav-ctrl.js', function(){
      beforeEach(function(){
        module('app.controllers');
        module('app.services');
      });
    	var $controller, $config, $httpBackend, $location;

    	beforeEach(inject(function(_$controller_, _$config_, _$httpBackend_, _$location_){
    	    $controller = _$controller_;
    	    $config = _$config_;
    	    $httpBackend = _$httpBackend_;
          $location = _$location_;
    	}));

      describe('$scope functions are set properly', function(){
        var $scope;

        beforeEach(function(){
          $scope = {};
          $controller('NavCtrl', {$scope:$scope});
        })

        it('$scope.isActive is defined correctly', function(){
          $location.path('/home');
          expect($scope.isActive('/home')).toBeTruthy();
        });

      });
    });
  }
);

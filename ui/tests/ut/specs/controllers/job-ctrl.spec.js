define(['angular', 'angularMocks', 'js/controllers/job-ctrl'],
  function(angular, mocks, JobCtrl) {
    describe('Test /js/controllers/job-ctrl.js', function(){
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
	        toaster = {};
	        $route = {};
	    }));

        beforeEach(function(){
          	$scope =  $rootScope.$new();
	        controller = $controller('JobCtrl', {$scope: $scope, $route: $route, toaster: toaster });
        });

        describe("if the controller of JobCtrl exists",function(){
        	it('controller exists', function(){
	          	expect(controller).toBeDefined();
	        });
        })

        describe("check if createRowCollection",function(){

	        it('createRowCollection', function(){
	          	expect($scope.rowCollection).not.toEqual([]);
	        });

      	})


    });
  }
)
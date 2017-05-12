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
//for demo purpose
var allTestFiles = [];
var TEST_REGEXP = /(spec|test)\.js$/i;

// Get a list of all the test files to include
Object.keys(window.__karma__.files).forEach(function(file) {
  if (TEST_REGEXP.test(file)) {
    // Normalize paths to RequireJS module names.
    // If you require sub-dependencies of test files to be loaded as-is (requiring file extension)
    // then do not normalize the paths
    var normalizedTestModule = file.replace(/^\/base\/|\.js$/g, '');
    // var normalizedTestModule = file.replace(/^\/base\//, '').replace(/\.js$/, '');
    // console.log(normalizedTestModule);
    allTestFiles.push(normalizedTestModule);
  }
});

require.config({
  // Karma serves files under /base, which is the basePath from your config file
  baseUrl: '/base',

  waitSeconds: 200,
  // dynamically load all test files
  deps: allTestFiles,
  // dpes: ['C:/temp/Bark_UI/tests/ut/specs/my_first_spec'],


  // we have to kickoff jasmine, as it is asynchronous
  callback: window.__karma__.start,
  paths: {
    // 'domReady': '../bower_components/domReady/domReady',
    'angular': '/base/bower_components/angular/angular',
    'angularMocks': '/base/node_modules/angular-mocks/angular-mocks',
    'angularRoute': '/base/bower_components/angular-route/angular-route',

    'ngAnimate': '/base/bower_components/angular-animate/angular-animate',
    'ngToaster': '/base/bower_components/AngularJS-Toaster/toaster',

    'jquery': '/base/bower_components/jquery/dist/jquery',
    'bootstrap': '/base/bower_components/bootstrap/dist/js/bootstrap',

    
		'echarts': '/base/bower_components/echarts/dist/echarts',
  },
  shim: {
    'angular': {
      deps: ['jquery'],
      exports: 'angular'
    },
    // 'angularMocks': {
    //   exports: 'angularMocks',
    //   deps: ['angular']
    // },
    'angularMocks': {deps: ['angular'], 'exports': 'angular.mock'},
    'angularRoute': {
        deps: ['angular'],
        exports: 'angularRoute'
    },
    'ngSmartTable': {
      deps: ['angular'],
      exports: 'ngSmartTable'
    },
    'ngAnimate': {
        deps: ['angular'],
        exports: 'ngAnimate'
    },
    'ngToaster': {
      deps: ['angular', 'ngAnimate'],
      exports: 'ngToaster'
    },

    'jquery': {
      exports: 'jquery'
    },
    'bootstrap': {
      exports: 'bootstrap',
      deps: ['jquery']
    },
    'echarts': {
      exports: 'echarts'
    }
  }
});

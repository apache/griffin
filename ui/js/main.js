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

//Reference: https://www.startersquad.com/blog/angularjs-requirejs/

/*global require*/
'use strict';

require.config({
	paths: {
	    'domReady': '../bower_components/domReady/domReady',
	    'angular': '../bower_components/angular/angular',
			'angularRoute': '../bower_components/angular-route/angular-route',

			'ngSmartTable': '../bower_components/angular-smart-table/dist/smart-table',
			'ngAnimate': '../bower_components/angular-animate/angular-animate',
			'ngToaster': '../bower_components/AngularJS-Toaster/toaster',
			'ngCookies': '../bower_components/angular-cookies/angular-cookies',

	    'jquery': '../bower_components/jquery/dist/jquery',
	    'bootstrap': '../bower_components/bootstrap/dist/js/bootstrap',
	    'spin': '../bower_components/spin.js/spin',
	    'angularSpinner': '../bower_components/angular-spinner/angular-spinner',
		'echarts': '../bower_components/echarts/dist/echarts',
		'echarts-dark': '../bower_components/echarts/theme/dark'
	},
	shim: {
		'angular': {
			deps: ['jquery'],
			exports: 'angular'
		},
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
		'ngCookies': {
			deps: ['angular'],
			exports: 'ngCookies'
		},
	
	    'jquery': {
				exports: 'jquery'
			},
	    'bootstrap': {
	      exports: 'bootstrap',
	      deps: ['jquery']
	    },
	    'spin':{
	    	exports: 'spin'
	    },
	    'angularSpinner':{
	    	exports: 'angularSpinner',
	    	deps: ['angular', 'spin']
	    },
		'echarts': {
			exports: 'echarts'
		},
		'echarts-dark': {
			deps: ['echarts'],
			exports: 'echarts-dark'
		}
	},
	deps: ['bs']
});

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
define([
    'angular',
    './controllers/index',
    './directives/index',
    './filters/index',
    './services/index',
    'ngSmartTable',
    'angularRoute',
    'ngToaster',
    'ngCookies',
    'angularSpinner',
    'echarts',
    'echarts-dark'
], function (angular) {
    'use strict';

    return angular.module('app', [
        'app.services',
        'app.controllers',
        'smart-table',
        'app.filters',
        'app.directives',
        'ngRoute',
        'toaster',
        'ngCookies',
        'angularSpinner'
    ]);
});

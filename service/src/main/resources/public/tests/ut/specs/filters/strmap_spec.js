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
define(['angular', 'angularMocks', 'js/filters/strmap'],
  function(angular, mocks, filter) {

    describe('Test /js/filters/strmap.js', function() {
      beforeEach(module('app.filters'));

      // Our first test!!!!
      it('map correctly for index', mocks.inject(function($filter) {
        // console.log($filter('strShorten')('1234567890123444444'));
        var arr = ['Apple', 'Orange', 'Pale'];
        expect($filter('strmap')(0, arr)).toEqual(arr[0]);
        expect($filter('strmap')(3, arr)).toEqual(3);

      }));


    });
  }
);

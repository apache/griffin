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
define(['angular', 'angularMocks', 'js/filters/strshorten'],
  function(angular, mocks, filter) {

    describe('Test /js/filters/strshorten.js', function() {
      beforeEach(module('app.filters'));
      // Here we register the function returned by the myFilter AMD module
      // beforeEach(mocks.module(function($filterProvider) {
      //   $filterprovider.register('stringsFilter', filter);
      // }));

      // Our first test!!!!
      it('string should be shorten', mocks.inject(function($filter) {
        // console.log($filter('strShorten')('1234567890123444444'));
        expect($filter('strShorten')('abcde')).toEqual('abcde');
        expect($filter('strShorten')('1234567890123444444')).toEqual('123456789012...');
      }));

      // it('should be true', function(){
      //   expect(1).not.toBeNull();
      // });

    });
  }
);

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
define(['./module'], function (filters) {
    'use strict';

    // return filters.filter('strings', ['version', function (version) {
    return filters.filter('strShorten', function () {
        return function (text) {
            var str = String(text);
            var strLength = str.length;
            var windowWidth = 2000;
            try{
              windowWidth = window.innerWidth;
              console.log('Window Size: ' + windowWidth);
            }catch(e){
              console.error(e);
            }

            if(windowWidth < 1400 && strLength > 10){
              return str.substring(0, 10) + '...';
            }else if(windowWidth < 1600 && strLength > 20){
              return str.substring(0, 20) + '...';
            }else if(windowWidth < 1800 && strLength > 26){
              return str.substring(0, 26) + '...';
            }else if(windowWidth < 2000 && strLength > 30){
              return str.substring(0, 30) + '...';
            }else{
              return str;
            }
        }
      });
    // }]);
});

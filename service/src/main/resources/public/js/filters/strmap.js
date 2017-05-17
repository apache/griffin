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
    var constants = {
      modeltype: ['Accuracy', 'Validity', 'Anomaly Detection', 'Publish Metrics'],
      scheduletype: ['Daily', 'Weekly', 'Monthly', 'Hourly'],
      modelsystem: ['Bullseye', 'GPS', 'Hadoop', 'PDS', 'IDLS', 'Pulsar', 'Kafka', 'Sojourner', 'SiteSpeed', 'EDW'],
      matchfunction: ['LENGTH', 'LOWER', 'UPPER', 'TRIM'],
      //modelstatus: ['Testing', 'Need Verify', 'Deployed'],
      modelstatus: ['Initial', 'Validation', 'Production'],
      vatype: ['Defaut Count', 'Total Count', 'Null Count', 'Unique Count', 'Duplicate Count', 'Maximum', 'Minimum', 'Mean', 'Median', 'Regular Expression Matching', 'Pattern Frequency'],
      antype: ['Default', 'History Trend Detection', 'Bollinger Bands Detection', 'Deviation Detection(Based on MAD)']
    };

    //get tha value by index from an array
    filters.filter('strmap', function () {
        return function (index, array) {
            var result = index;
            if(!!array && (array instanceof Array)  && index < array.length){
              result = array[index];
            }else if(typeof(array)==='string'){
              if(constants[array] && index < constants[array].length){
                result = constants[array][index];
              }
            }
            return result;
        }
      });

      //get the index of the value in array
      filters.filter('stridx', function () {
          return function (value, array) {
              var result = -1;
              if(!!array && (array instanceof Array)  && index < array.length){
                result = array.indexOf(value);
              }else if(typeof(array)==='string'){
                if(constants[array]){
                  result = constants[array].indexOf(value);
                }
              }
              return result;
          }
        });

        //Get the array by name
        filters.filter('strarr', function () {
            return function (name) {
                return constants[name];
            }
          });


});

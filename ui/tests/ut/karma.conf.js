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
// Karma configuration
// Generated on Thu Apr 07 2016 15:02:00 GMT+0800 (China Standard Time)
//http://monicalent.com/blog/2015/02/11/karma-tests-angular-js-require-j/

module.exports = function(config) {
  config.set({

    // base path that will be used to resolve all patterns (eg. files, exclude)
    basePath: '../../',


    // frameworks to use
    // available frameworks: https://npmjs.org/browse/keyword/karma-adapter
    frameworks: ['jasmine', 'requirejs'],


    files: [
      'tests/ut/test-main.js',
      {pattern:'bower_components/**/*.js', included:false},
      {pattern:'node_modules/angular-mocks/angular-mocks.js', included:false},
      {pattern: 'js/**/*.js', included: false},
      {pattern: 'tests/**/*spec.js', included: false}
    ],


    // list of files to exclude
    exclude: [
      'js/main.js',
      'js/bs.js',
      'js/routes.js',
      'bower_components/**/*test*/**/*.js',
      'bower_components/**/*spec.js',
      // 'node_modules/**/*spec.js',
      // 'node_modules/**/*spec*/**/*.js',
      // 'node_modules/**/*test.js'
    ],


    // preprocess matching files before serving them to the browser
    // available preprocessors: https://npmjs.org/browse/keyword/karma-preprocessor
    preprocessors: {
      'js/**/*.js':['coverage']
    },

    coverageReporter: {
        type:'lcov',
        dir: 'tests/ut/test-coverage'
    },


    // test results reporter to use
    // possible values: 'dots', 'progress'
    // available reporters: https://npmjs.org/browse/keyword/karma-reporter
    // reporters: ['progress'],
    reporters: ['mocha', 'coverage', 'progress'],


    // web server port
    port: 9876,


    // enable / disable colors in the output (reporters and logs)
    colors: true,


    // level of logging
    // possible values: config.LOG_DISABLE || config.LOG_ERROR || config.LOG_WARN || config.LOG_INFO || config.LOG_DEBUG
    logLevel: config.LOG_DEBUG,


    // enable / disable watching file and executing tests whenever any file changes
    autoWatch: true,


    // start these browsers
    // available browser launchers: https://npmjs.org/browse/keyword/karma-launcher
    browsers: ['Chrome'],


    // Continuous Integration mode
    // if true, Karma captures browsers, runs the tests and exits
    singleRun: false,

    // Concurrency level
    // how many browser should be started simultaneous
    concurrency: Infinity
  })
}

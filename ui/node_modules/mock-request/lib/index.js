/*
 * index.js: Top-level include for the mock-request module.
 *
 * (C) 2010, Nodejitsu Inc.
 *
 */

//
// ### Export components
// Export the important components of the `MockRequest` module.
//
exports.MockRequest = require('./mock-request').MockRequest;
exports.utils       = require('./utils');

//
// ### function mock (options)
// #### @options {Object} Default mock request options (host, port, etc)
// Creates a new `MockRequest` object with the specified options.
//
exports.mock = function (options, defaults) {
  return new exports.MockRequest(options, defaults);
};
/*
 * mock-request.js: MockRequest object responsible for mocking a sequence of requests.
 *
 * (C) 2010, Nodejitsu Inc.
 *
 */
 
var assert = require('assert'),
    utils = require('./utils');

//
// ### function MockRequest (options)
// #### @options {Object} Options to use with this instance
// Constructor function for the MockRequest object responsible for
// constructing a sequence of mock `request/response` pairs and a
// function which can be used against them.
//
var MockRequest = exports.MockRequest = function (options, defaults) {  
  options = options || {};

  //
  // Setup intelligent request / response defaults
  //
  defaults = defaults || {};
  defaults.request             = defaults.request || {};
  defaults.request.headers     = defaults.request.headers || {};
  defaults.response            = defaults.response || {};
  defaults.response.headers    = defaults.response.headers || {};
  defaults.response.statusCode = defaults.response.statusCode || 200;
  
  this.mocks    = [];
  this.current  = null;
  this.protocol = options.protocol || 'http';
  this.port     = options.port || 80; 
  this.host     = this.protocol + '://' + (options.host || 'mock-request');
  this.defaults = defaults;
  
  
  if (this.port !== 433 && this.port !== 80) {
    this.host += ':' + this.port
  }
};

//
// ### function request (method, path, body, headers)
// #### @method {string} HTTP method to expect in this mock request.
// #### @path {string} Path to expect in this mock request.
// #### @body {Object} **Optional** Request body to expect in this mock request.
// #### @headers {Object} **Optional** Headers to expect in this mock request.
// Appends a new request to the sequence of mock `request/response` pairs
// managed by this instance.
//
MockRequest.prototype.request = function (method, path, body, headers) {
  if (this.current && !this.current.response) {
    this.respond();
  }
  else if (this.current) {
    this.mocks.push(this.current);
    this.current = null;
  }
  
  if (!path.slice(0) === '/') {
    path = '/' + path;
  }
  
  var current = {
    request: {
      method: method,
      uri: this.host + path
    }
  };
  
  if (body) {
    current.request.body = JSON.stringify(body);
  }
  
  function addHeaders (target) {
    current.request.headers = current.request.headers || {};
    Object.keys(target).forEach(function (header) {
      current.request.headers[header] = target[header];
    });
  }
  
  if (this.defaults.request['headers']) {
    addHeaders(this.defaults.request['headers']);
  }
  
  if (headers) {
    addHeaders(headers);
  }
  
  this.current = current;
  
  return this;
};

//
// ### function get (path, headers)
// #### @path {string} Path to expect in this mock request.
// #### @headers {Object} **Optional** Headers to expect in this mock request.
// Appends a new `GET` request to the sequence of mock `request/response` pairs
// managed by this instance.
//
MockRequest.prototype.get = function (path, headers) {
  return this.request('GET', path, null, headers);
};

//
// ### function post (path, body, headers)
// #### @path {string} Path to expect in this mock request.
// #### @body {Object} **Optional** Request body to expect in this mock request.
// #### @headers {Object} **Optional** Headers to expect in this mock request.
// Appends a new `POST` request to the sequence of mock `request/response` pairs
// managed by this instance.
//
MockRequest.prototype.post = function (path, body, headers) {
  return this.request('POST', path, body, headers);
};

//
// ### function put (path, body, headers)
// #### @path {string} Path to expect in this mock request.
// #### @body {Object} **Optional** Request body to expect in this mock request.
// #### @headers {Object} **Optional** Headers to expect in this mock request.
// Appends a new `PUT` request to the sequence of mock `request/response` pairs
// managed by this instance.
//
MockRequest.prototype.put = function (path, body, headers) {
  return this.request('PUT', path, body, headers);
};

//
// ### function del (path, body, headers)
// #### @path {string} Path to expect in this mock request.
// #### @body {Object} **Optional** Request body to expect in this mock request.
// #### @headers {Object} **Optional** Headers to expect in this mock request.
// Appends a new `DELETE` request to the sequence of mock `request/response` pairs
// managed by this instance.
//
MockRequest.prototype.del = function (path, body, headers) {
  return this.request('DELETE', path, body, headers);
};

//
// ### function respond (response)
// #### @response {Object} **Optional** HTTP response data to include in the mock response
// Sets the specified `response` data for the most recent mock request (i.e. the latest
// call to `.get()`, `.post()`, etc.) and appends it to ths list of mocks managed by this 
// instance.
//
MockRequest.prototype.respond = function (response) {
  var self = this;
  
  //
  // Setup the defaults in the mock response
  //
  response = response || {};
  response.statusCode = response.statusCode || this.defaults.response.statusCode;
  
  if (response.body) {
    response.body = JSON.stringify(response.body);
  }
  
  function addHeaders (target) {
    response.headers = response.headers || {};
    Object.keys(target).forEach(function (header) {
      response.headers[header] = target[header];
    });
  }
  
  if (this.defaults.response['headers']) {
    addHeaders(this.defaults.response['headers']);
  }
  
  if (!this.current) {
    throw new Error('Cannot mock response without a request');
  }
  
  this.current.response = response;
  this.mocks.push(this.current);
  this.current = null;
  
  return this;
};

//
// ### function run ()  
// Returns a function with the method signature to the `request` module
// which only accepts (in sequence) the `request/response` pairs that have
// been previously mocked by this instance.
//
MockRequest.prototype.run = function () {
  var self = this,
      length = this.mocks.length,
      count = 0;
  
  if (this.current && !this.current.response) {
    this.respond();
  }

  return function (actual, callback) {
    //
    // Grab the next mock request / response object.
    //
    var next = self.mocks.shift();
    
    if (!next) {
      throw new Error('Too many calls to _request. Expected: ' + length + ' Got: ' + count);
    }
    
    //
    // Increment the number of mock calls
    //
    count += 1;

    try {
      assert.equal(actual.uri, next.request.uri);
      //
      // Check that request was made with at least the required headers.
      // extra headers do not cause the test to fail.
      //
      utils.assertSubtree(actual.headers || {}, next.request.headers);
    }
    catch (ex) {
      console.log('\nmismatch in remote request :\n')
      console.dir(actual);
      console.dir(next.request);
      throw ex;
    }
    
    callback(null, { 
      statusCode: next.response.statusCode,
      headers: next.response.headers || {}
    }, next.response.body);
    
    return {
      on: function () { }, 
      emit: function () { }, 
      removeListener: function () { }, 
      end: function () { } 
    };
  }
};
/*
 * apps.js: Tests for `jitsu apps *` command(s).
 *
 * (C) 2010, Nodejitsu Inc.
 *
 */
 
var fs = require('fs'),
    path = require('path'),
    assert = require('assert'),
    vows = require('vows'),
    mockRequest = require('../lib');

var mockData = [
  {
    request: {
      method: 'get',
      path: '/tests'
    },
    response: {
      statusCode: 200
    }
  },
  {
    request: {
      method: 'post',
      path: '/tests',
      body: {
        'some': 'test is running'
      },
      headers: {
        'x-test-header': true
      }
    },
    response: {
      statusCode: 404,
      body: {
        'some': 'test has responded'
      },
      headers: {
        'x-test-complete': true
      }
    }
  }
];

function sendRequest (mockFn, request, callback) {
  var send = {
    method: request.method,
    uri: 'http://mock-request' + request.path
  };
  
  if (request.body) {
    send.body = JSON.stringify(request.body);
  }
  
  if (request.headers) {
    send.headers = request.headers;
  }
  
  return mockFn(send, callback);
}

function assertResponse (actual, expected) {
  assert.equal(actual.statusCode, expected.statusCode);
  assert.equal(actual.body, expected.body);
  assert.deepEqual(actual.headers, expected.headers || {});
}

vows.describe('mock-request').addBatch({
  "When using the mock-request module": {
    "the mock() method": {
      "configured with a sequence of HTTP requests": {
        topic: function () {
          var testMock = this.testMock = mockRequest.mock();
          
          mockData.forEach(function (pair) {
            var request = pair.request;
                
            testMock
              .request(request.method, request.path, request.body, request.headers)
              .respond(pair.response);
          });
          
          return testMock.run();
        },
        "should respond with the appropriate data at each step": function (mockFn) {
          mockData.forEach(function (pair) {
            var stream = sendRequest(mockFn, pair.request, function (err, res, body) {
              assert.isNull(err);
              res.body = body;
              assertResponse(res, pair.response);
            });
          });
        }
      }
    }
  }
}).export(module);
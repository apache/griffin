/*
 * assert-subtree.js: Custom deep object checking
 *
 * (C) 2010, Nodejitsu Inc.
 *
 */
 
var assert = require('assert');

var utils = exports;

//
// ### function assertSubtree (actual, expected, message)
// #### @actual {Object} Actual subtree to assert 
// #### @expected {Object} Expected subtree to assert against
// #### @message {string} Message to include in the error thrown (if any).
// Checks the internals of the `actual` subtree against the `expected` subtree,
// throwing an error with the specified `message` if the two do not match.
//
utils.assertSubtree = exports.assertSubtree = function assertSubtree (actual, expected, message) {
  if (!deepObjectCheck(actual, expected)) {
    //
    // Remark: getting weird error if I use the propper `assert.fail()`.
    //
    // throw new (assert.AssertionError)({actual: actual, expected: expected, opperator: 'subtree'})
    //
    throw new Error(JSON.stringify({ actual: actual, expected: expected, opperator: 'subtree' }))
  }
};

//
// ### @private function deepObjectCheck (actual, expected) 
// #### @actual {Object} Actual subtree to assert 
// #### @expected {Object} Expected subtree to assert against
// Performs a custom deep inspection of the `actual` Object against
// the `expected` Object, returning a value indicating if the two match.
//
function deepObjectCheck (actual, expected) {
  if (expected && actual && (expected.length != actual.length)
    || typeof actual != typeof expected) {
    return false;
  }

  for (var key in expected) {
    var actualType = typeof(actual[key]),
        expectedType = typeof(expected[key]);
  
    if (actualType != expectedType) {
      return false;
    }
    
    if (actualType == "function") {
      continue;
    } 
    else if (actualType == "object") {
      if (!deepObjectCheck(expected[key], actual[key])) {
        return false;
      }
    } 
    else if (actual[key] !== expected[key]) {
      return false;
    }
  }
 
  return true;
};
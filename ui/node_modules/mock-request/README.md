# mock-request

A simple testing tool for mocking HTTP sequences of request / response pairs in node.js

## Installation

### Installing npm (node package manager)
``` bash
  $ curl http://npmjs.org/install.sh | sh
```

### Installing mock-request
``` bash
  $ [sudo] npm install mock-request
```

## Purpose
The `mock-request` library is designed to easily mock HTTP endpoints in tests which fit the following boilerplate: 

1. Tests are performed by making HTTP requests against an API server
2. Assertions are made against the HTTP response (status, headers, body, etc)
3. Rinse. Repeat.

The method signature used by the `mock-request` library matches that of the popular [request][0] library widely used in the node.js community. Besides that method signature, there are no external test framework dependencies so use whatever your preference is: [vows][1], [expresso][2], [nodeunit][3], etc.

If you're curious why mocking your HTTP requests could be helpful you can [read up here][4].

## Usage
The `mock-request` library is designed for explicit mocking, it does not perform any interpolation or guessing beyond assuming a default response of `200` unless otherwise indicated. Here's a sample of how to use `mock-request`:

``` js
  var mockRequest = require('mock-request'),
      assert = require('assert');
      
  var mockFn = mockRequest.mock()
                .get('/not-here')
                .respond(404)
                .post('/tests', { 'some': 'test-youre-running' }, {
                  'x-test-header': true
                })
                .respond(200, { 'some': 'test-has-completed' }, {
                  'x-test-completed': true
                })
                .run();
  
  //
  // The mock function returned from `mockRequest.mock()` is 
  // synchronous because no HTTP actually takes place.
  //
  mockFn({
    method: 'GET',
    uri: 'http://mock-request/not-here'
  }, function (err, res, body) {
    assert.equal(res.statusCode, 404);
  });
  
  //
  // Now that we've made the first `GET` request, we have to make the 
  // `POST` request or `mock-request` will throw an `Error`
  //
  mockFn({
    method: 'POST',
    uri: 'http://mock-request/tests',
    headers: {
      'some': 'test-youre-running'
    }
  }, function (err, res, body) {
    assert.deepEqual(body, { 'some': 'test-has-completed' });
    assert.deepEqual(res.headers, { 'x-test-completed': true });
    
    //
    // We mocked this request to respond with 404, so by asserting
    // 200, this will throw an `AssertionError`.
    //
    assert.equal(res.statusCode, 200);
  });
```

## Roadmap

1. [Get feedback][5] on what else could be exposed through this library.
2. Improve it.
3. Repeat (1) + (2).

## Run Tests
<pre>
  npm test
</pre>

#### Author: [Charlie Robbins](http://blog.nodejitsu.com)
#### Contributors: [Dominic Tarr](http://github.com/dominictarr)

[0]: http://nodejs.org
[1]: http://vowsjs.org
[2]: http://tjholowaychuk.com/post/656851606/expresso-tdd-framework-for-nodejs
[3]: http://github.com/caolan/nodeunit
[4]: http://en.wikipedia.org/wiki/Mock_object
[5]: https://github.com/nodejitsu/mock-request/issues
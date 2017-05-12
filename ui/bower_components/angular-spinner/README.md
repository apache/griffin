# angular-spinner

Angular directive to show an animated spinner (using [spin.js](http://fgnass.github.io/spin.js/))

Copyright (C) 2013, 2014, 2015, Uri Shaked <uri@urish.org>.

[![Build Status](https://travis-ci.org/urish/angular-spinner.png?branch=master)](https://travis-ci.org/urish/angular-spinner)
[![Coverage Status](https://coveralls.io/repos/urish/angular-spinner/badge.png)](https://coveralls.io/r/urish/angular-spinner)

## Usage

Get both spin.js and angular-spinner

- via npm: by running ``` $ npm install angular-spinner ``` from your console
- or via Bower: by running ``` $ bower install angular-spinner ``` from your console

Include both spin.js and angular-spinner.js in your application.

```html
<script src="bower_components/spin.js/spin.js"></script>
<script src="bower_components/angular-spinner/angular-spinner.js"></script>
```

Add the module `angularSpinner` as a dependency to your app module:

```js
var myapp = angular.module('myapp', ['angularSpinner']);
```

You can now start using the us-spinner directive to display an animated
spinner. For example :

```html
<span us-spinner></span>
```

You can also pass spinner options, for example:

```html
<span us-spinner="{radius:30, width:8, length: 16}"></span>
```

Possible configuration options are described in the [spin.js homepage](http://fgnass.github.io/spin.js/).

You can direct the spinner to start and stop based on a scope expression, for example:

```html
<span us-spinner="{radius:30, width:8, length: 16}" spinner-on="showSpinner"></span>
```


### Configuring default spinner options

You can use `usSpinnerConfigProvider` to configure default options for all spinners globally. Any options passed from a directive still override these.

```js
myapp.config(['usSpinnerConfigProvider', function (usSpinnerConfigProvider) {
    usSpinnerConfigProvider.setDefaults({color: 'blue'});
}]);
```

## Themes

Themes provide named default options for spinners. Any options passed from a directive still override these.

```js
myapp.config(['usSpinnerConfigProvider', function (usSpinnerConfigProvider) {
    usSpinnerConfigProvider.setTheme('bigBlue', {color: 'blue', radius: 20});
    usSpinnerConfigProvider.setTheme('smallRed', {color: 'red', radius: 6});
}]);
```

```html
<span us-spinner spinner-theme="smallRed"></span>
```

### Using the usSpinnerService to control spinners

```html
<button ng-click="startSpin()">Start spinner</button>
<button ng-click="stopSpin()">Stop spinner</button>

<span us-spinner spinner-key="spinner-1"></span>
```

The `usSpinnerService` service let you control spin start and stop :

```js
app.controller('MyController', ['$scope', 'usSpinnerService', function($scope, usSpinnerService){
    $scope.startSpin = function(){
        usSpinnerService.spin('spinner-1');
    }
    $scope.stopSpin = function(){
        usSpinnerService.stop('spinner-1');
    }
}]);
```

Note that when you specify a key, the spinner is rendered inactive.
You can still render the spinner as active with the spinner-start-active parameter :
```html
<span us-spinner spinner-key="spinner-1" spinner-start-active="true"></span>
```

spinner-start-active is ignored if spinner-on is specified.

The spinner-key will be used as an identifier (not unique) allowing you to have several spinners controlled by the same key :

```html
<span us-spinner spinner-key="spinner-1"></span>
<span us-spinner spinner-key="spinner-2"></span>

... random html code ...

<!-- This spinner will be triggered along with the first "spinner-1" -->
<span us-spinner spinner-key="spinner-1"></span>
```

### Example

See [online example on Plunker](http://plnkr.co/edit/BGLUYcylbIVJRz6ztbhf?p=preview).

## License

Released under the terms of MIT License.

## Contributing

1. Fork repo.
2. `npm install`
3. `bower install`
4. Make your changes, add your tests.
5. `grunt test`
6. `grunt build` once all tests are passing. Commit, push, PR.

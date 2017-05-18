# Changelog

## 0.8.0 - 2015-10-29
- Improve UMD (Universal Module Definition) code, fixes ([#61](https://github.com/urish/angular-spinner/issues/61))
- Theme support ([#66](https://github.com/urish/angular-spinner/pull/66), contributed by [marknadig](https://github.com/marknadig))
- Add `spinner-on` attribute ([#71](https://github.com/urish/angular-spinner/pull/71), contributed by [marknadig](https://github.com/marknadig))

## 0.7.0 - 2015-09-09
- Add CommonJS support, improve AMD support (for use with browserify, webpack, SystemJS, etc.)

## 0.6.2 - 2015-07-02
- Relax Angular's dependency version lock ([#52](https://github.com/urish/angular-spinner/pull/52), contributed by [gottfrois](https://github.com/gottfrois))

## 0.6.1 - 2015-01-06
- Removed NBSP characters from source code ([#40](https://github.com/urish/angular-spinner/pull/40), contributed by [amolghotankar](https://github.com/amolghotankar))
- Return the created AngularJS module ([#37](https://github.com/urish/angular-spinner/pull/37), contributed by [k7sleeper](https://github.com/k7sleeper))

## 0.6.0 - 2014-12-12
- Added configurable default options ([#31](https://github.com/urish/angular-spinner/pull/31), contributed by [aleksih](https://github.com/aleksih))
- Added scope eval to allow for data binding support ([#21](https://github.com/urish/angular-spinner/pull/21), contributed by [jdamick](https://github.com/jdamick))

## 0.5.1 - 2014-08-09
- AMD / Require.js compatibility ([#11](https://github.com/urish/angular-spinner/pull/11), contributed by [floribon](https://github.com/floribon))
- Bugfix: Stop events are ignored if sent before the directive is fully initialized and `startActive` is true ([#22](https://github.com/urish/angular-spinner/pull/22), contributed by [vleborgne](https://github.com/vleborgne))

## 0.5.0 - 2014-06-03

- Add support for expressions in attributes ([#12](https://github.com/urish/angular-spinner/pull/12), contributed by [aaronroberson](https://github.com/aaronroberson))
- Generate source map for the minified version ([#14](https://github.com/urish/angular-spinner/issues/14))
- Add a `main` field to package.json ([#15](https://github.com/urish/angular-spinner/pull/15), contributed by [elfreyshira](https://github.com/elfreyshira))
- Enable support for AngularJS 1.3.x in bower.json

## 0.4.0 - 2014-03-15

- Upgrade spin.js to 2.0.0. See breaking changes [here](http://fgnass.github.io/spin.js/#v2.0.0).

## 0.3.1 - 2014-01-31

- Fixed an issue that caused the minified code to fail.

## 0.3.0 - 2014-01-26

- Add ability to control spinner state with bundled service (([#6](https://github.com/urish/angular-spinner/pull/6), contributed by [lossendae](https://github.com/lossendae))

## 0.2.1 - 2013-08-28

- Add test coverage reporting
- Stop the spinner on scope destroy
- Support for AngularJS 1.2

var pathLib = require('path')

function createPattern (path) {
  return {pattern: path, included: true, served: true, watched: false}
}

var requirejsPath = pathLib.dirname(require.resolve('requirejs')) + '/../require.js'

function initRequireJs (files) {
  files.unshift(createPattern(pathLib.join(__dirname, '/adapter.js')))
  files.unshift(createPattern(requirejsPath))
}

initRequireJs.$inject = ['config.files']

module.exports = {
  'framework:requirejs': ['factory', initRequireJs]
}

const getFile = require('./getFile');
const xmler = require('xmler');

module.exports = function(argv) {
  return () => getFile(argv.source).pipe(xmler(argv.selector, argv));
};
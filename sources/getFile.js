const fs = require('fs');
const path = require('path');

module.exports = (source, dir) => {
  if (/https?:/.exec(source))
    return require('request').get(source);
  else if (source === 'stdin')
    return process.stdin;
  else
    return fs.createReadStream(path.resolve('./',dir,source));
};
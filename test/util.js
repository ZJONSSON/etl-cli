const path = require('path');
const fs = require('fs');
const { cli } = require('..');
const split = require('split-string');

module.exports.path = filename => path.join(__dirname, filename);

module.exports.requireAll = (directory) => {
  const files = fs.readdirSync(directory);
  files.forEach(file => {
    // Filter for JavaScript files
    if (path.extname(file) === '.js') {
      const filePath = path.join(directory, file);
      require(filePath);
    }
  });
};

module.exports.cli = cmds => {
  const args = split(cmds, { quotes: '"', separator:' ' }).slice(1).map(d => d.replace(/"/g, ''));
  return cli(args);
};

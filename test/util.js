const path = require('path');
const fs = require('fs');

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

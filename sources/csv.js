const etl = require('etl');
const getFile = require('./getFile');

module.exports = function(argv) {
  return () => getFile(argv.source)
    .pipe(etl.csv(argv))
    .pipe(etl.map(function(d) {
      return Object.keys(d).reduce( (p, path) => {
        const value = d[path];
        if (value == '') return p;

        const keys = path.split(argv.separator || '>');
        let obj = p;
        keys
          .forEach( (key, pos) => {
            if (pos == keys.length - 1) {
              obj[key] = value;
            } else {
              if (obj[key] === undefined) {
                // if the key is 0 then we are probably dealing with an array
                obj[key] = (+keys[pos + 1] == 0) ? [] : {};
              }
              obj = obj[key];
            }
          });

        return p;
      }, {});
    }));
};
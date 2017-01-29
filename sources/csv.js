const etl = require('etl');
const getFile = require('./getFile');

module.exports = function(argv) {
  return () => getFile(argv.source)
    .pipe(etl.csv())
    .pipe(etl.map(function(d) {
      return Object.keys(d).reduce( (p,key) => {
        const keys = key.split(argv.separator || 'ᐅ');
        let obj = p;
        keys.slice(0,keys.length-1)
          .forEach(key => obj = obj[key] = obj[key] || {});
        obj[keys[keys.length-1]] = d[key];
        return p;
      },{});
    }));
};
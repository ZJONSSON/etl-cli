const etl = require('etl');
const getFile = require('./getFile');

module.exports = function(argv) {
    return () => getFile(argv.source)
    .pipe(etl.split())
    .pipe(etl.map(d => {
      try {
        const text = (d.text || d).replace(/,\s*$/,'');
        return JSON.parse(text);
      } catch(e) {
        if (argv.strict_json) throw e;
      }
    }));
};
const etl = require('etl');
const getFile = require('./getFile');

module.exports = function(argv) {
  return () => {
    let file = getFile(argv.source);
    if (argv.json_multiline) {
      const data = file.pipe(etl.map(d => d.toString())).promise()
      return etl.toStream(data.then(d => JSON.parse(d.join(''))));
    }
    return file
      .pipe(etl.split())
      .pipe(etl.map(d => {
        try {
          const text = (d.text || d).replace(/,\s*$/,'');
          return JSON.parse(text);
        } catch(e) {
          if (argv.strict_json) throw e;
        }
      }));
  }
};
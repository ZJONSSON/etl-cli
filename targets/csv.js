const etl = require('etl');
const csvWriter = require('csv-write-stream');

module.exports = function(stream,argv) {
  const separator = argv.separator || 'ᐅ';
  let headers;

  function getHeaders(d) {
    if (d && typeof d === 'object')
      return Object.keys(d).reduce( (p,key) => {
        p[key] = getHeaders(d[key]);
        return p;
      },{});
    else
      return true;
  }

  
  function flattenData(headers,d,obj,prefix) {
    obj = obj || {};
    prefix = prefix || '';
    Object.keys(headers).forEach(key => {
      const header = headers[key];
      if (typeof header === 'object')
        flattenData(header,d[key] || {},obj,prefix+key+separator);
      else
        obj[prefix+key] = d[key];
    });
    return obj;
  }


  return stream.pipe(etl.map(function(d) {
    headers = headers || getHeaders(d);
    return flattenData(headers,d);
  }))
  .pipe(csvWriter())
  .pipe(argv.source === 'screen' ? etl.map(d => console.log(d)) : etl.toFile(argv.dest));
  
};

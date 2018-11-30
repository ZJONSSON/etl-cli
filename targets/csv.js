const etl = require('etl');
const csvWriter = require('csv-write-stream');

module.exports = function(stream,argv) {
  const separator = argv.separator || 'ᐅ';
  let headers = {};

  function getHeaders(d,p) {
    if (d && typeof d === 'object')
      return Object.keys(d).reduce( (p,key) => {
        if (d[key] !== undefined && d[key] !== null) {
          p[key] = getHeaders(d[key], p[key]);
        }
        return p;
      },p || {});
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


  let _stream = stream
    .pipe(etl.prescan(isNaN(argv.prescan) ? 100 : argv.prescan, d =>
      d.forEach(d => headers = getHeaders(d,headers)))
    )
    .pipe(etl.map(d => flattenData(headers,d)))
    .pipe(csvWriter());

  const outStream =  stream => {
    if(argv.target_gzip){
      let gz = require('zlib').createGzip();
      return stream.pipe(gz);
    } else
      return stream;
  };

  return outStream(_stream).pipe(argv.source === 'screen' ? etl.map(d => console.log(d)) : etl.toFile(argv.dest));

};

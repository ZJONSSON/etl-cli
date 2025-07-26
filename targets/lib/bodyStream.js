const { Readable } = require("stream");

module.exports = async (d, argv) => {
  let body = await (typeof d.body === 'function' ? d.body(true) : d.body);
  if (!body) {
    if (!argv.silent) {
      console.warn(`No body for ${d.filename || 'unknown file'}`);
    }
    body = Readable.from([]);
  }
  if (typeof body.pipe !== 'function') {
    if (body && typeof body === 'object') {
      body = JSON.stringify(body);
    }
    body = Readable.from([].concat(body));
  }
  if (argv.target_gzip) {
    body = body.pipe(require('zlib').createGzip());
  }
  return body;
};

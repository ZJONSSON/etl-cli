const { Readable } = require("stream");
const zlib = require("zlib");

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

  // If source file was gzipped (marked by _isGzipped flag), decompress it
  if (d._isGzipped) {
    body = body.pipe(zlib.createGunzip());
  }

  // If target should be gzipped, compress it
  if (argv.target_gzip) {
    body = body.pipe(zlib.createGzip());
  }
  return body;
};

const csv = require('./csv');
const etl = require('etl');

module.exports = (stream, filename, argv) => {
  if (filename.endsWith('.csv')) return csv(stream, argv);
  return stream.pipe(etl.map(d => {
    if (typeof d == 'object' && !Buffer.isBuffer(d)) return JSON.stringify(d, null,  argv.json_indent || 0)+'\n';
    return d;
  }));
}
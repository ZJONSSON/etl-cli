const etl = require('etl');
const parquet = require('parquetjs-lite');

module.exports = (stream, argv, schema) => {
  schema = schema && schema.parquet && new parquet.ParquetSchema(schema.parquet);
  if (!schema) throw 'schema missing';
  const out = etl.map();

  parquet.ParquetWriter.openStream(schema, out).then(writer => {
    if (argv.rowGroupSize) writer.setRowGroupSize(argv.rowGroupSize);
    stream.pipe(etl.map(async d => {
      try {
        await writer.appendRow(d);
      } catch(e) {
        out.emit('error', e);
      }
    }))
      .on('finish', async () => {
        await writer.close();
        out.end();
      });
  });

  return out
    .pipe(etl.map(d => {
      if (typeof d === 'function') return;
      return d;
    }))
    .pipe(etl.toFile(argv.dest));
};
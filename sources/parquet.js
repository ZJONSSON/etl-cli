const Stream = require('stream');
const parquet = require('parquetjs-lite');
const path = require('path');

module.exports = function(argv) {
  let reader,cursor;
  return () => Stream.Readable({
      read : async function() {
        try {
          if (!reader) {
            reader = await parquet.ParquetReader.openFile(path.resolve('./',argv.source));
            cursor = reader.getCursor(argv.columns ? argv.columns.split(',') : undefined);
            const exp = argv.export;
            if (exp) {
              if (exp == 'metadata') this.push(reader.metadata);
              else if (exp == 'schema') this.push(reader.schema);
              else this.emit('error','unknown export');
              this.emit('end');
              return;
            }
          }

          const records = await cursor.nextRowGroup();

          if (!records || !records.length) {
            this.emit('end');
          } else {
            records.forEach(record => this.push(record));
          }
        } catch(e) {
          this.emit('error',e);
        }
      },
      objectMode:true
    });
};
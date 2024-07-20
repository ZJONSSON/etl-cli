const Stream = require('stream');
const parquet = require('parquetjs-lite');
const path = require('path');

function removeListElement(obj) {
  if (obj && typeof obj == 'object') {
    if (obj.list) {
      obj = obj.list;
    } else if (obj.element) {
      obj = obj.element;
    };
    Object.keys(obj).forEach(key => {
      obj[key] = removeListElement(obj[key]);
    });
  }
  return obj;
}

module.exports = function(argv) {
  let reader, cursor;
  return {
    stream : () => new Stream.Readable({
      read : async function() {
        try {
          if (!reader) {
            reader = await parquet.ParquetReader.openFile(path.resolve('./', argv.source));
            cursor = reader.getCursor(argv.columns ? argv.columns.split(',') : undefined);
            const exp = argv.export;
            if (exp) {
              if (exp == 'metadata') this.push(reader.metadata);
              else if (exp == 'schema') this.push(reader.schema);
              else this.emit('error', 'unknown export');
              this.emit('end');
              return;
            }
          }

          const records = await cursor.nextRowGroup();

          if (!records || !records.length) {
            this.emit('end');
          } else {
            records.forEach(record => this.push(removeListElement(record)));
          }
        } catch(e) {
          this.emit('error', e);
        }
      },
      objectMode:true
    })
  };
};
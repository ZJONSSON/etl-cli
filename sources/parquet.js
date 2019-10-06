const Stream = require('stream');
const parquet = require('parquetjs-lite');
const path = require('path');

module.exports = function(argv) {
  let reader,cursor;
  return () => Stream.Readable({
      read : async function() {
        try {
          reader = reader || await parquet.ParquetReader.openFile(path.resolve('./',argv.source));
          cursor = cursor || reader.getCursor();
          const record = await cursor.next();
          if (!record) {
            this.emit('end');
          } else {
            this.push(record);
          }
        } catch(e) {
          this.emit('error',e);
        }
      },
      objectMode:true
    });
};
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

function convert(obj) {
  const array = obj?.fields?.list?.fields?.element || obj?.list?.fields?.element;
  if (array) {
    return {
      type: 'array',
      items: convert(array)
    };
  }

  if (obj.fields) {
    return {
      type: 'object',
      properties: Object.keys(obj.fields).reduce((acc, field) => {
        acc[field] = convert(obj.fields[field]);
        return acc;
      }, {})
    };
  } else {

    if (obj.repeatable) {
      delete obj.repeatable;
      return {
        type: 'array',
        items: convert(obj),
      };
    } else if (obj.type == 'UTF8') {
      return { type: 'string', comment: obj.type };
    } else if (obj.type == 'DOUBLE' || obj.type == 'FLOAT') {
      return { type: 'number', comment: obj.type };
    } else if (obj.type.includes('INT')) {
      return { type: 'integer', comment: obj.type };
    } else if (obj.type == 'BOOLEAN') {
      return { type: 'boolean', comment: obj.type };
    } else throw `unknown type ${obj}`;

  }
}

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
    schema: async() => {
      const reader = await parquet.ParquetReader.openFile(path.resolve('./', argv.source));
      return {
        type: "object",
        '$comment': 'extracted from parquet schema',
        ...convert({ fields: reader.schema.schema })
      };
    },
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
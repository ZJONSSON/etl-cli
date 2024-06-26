const etl = require('etl');
const { createConfig } = require('../util');

module.exports = argv => {
  const mysql = require('mysql');
  ['source_database', 'source_table'].forEach(key => { if(!argv[key]) throw `${key} missing`;});

  argv.source_table = argv.source_database + '.' + argv.source_table;
  const config = createConfig(argv.source_config, argv, 'source', ['host', 'connectionLimit', 'user', 'password']);
  const pool = mysql.createPool(config);
  const p = etl.mysql.execute(pool);

  const key_query = mysql.format('SHOW KEYS FROM ?? WHERE Key_name="PRIMARY"', [argv.source_table]);
  let query = argv.source_query || mysql.format('SELECT * FROM ??', [argv.source_table]);
  if (argv.where)
    query += mysql.format(` WHERE  ${argv.where}`);
  const schema_query = argv.schema_query || mysql.format('SELECT COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?', [argv.source_database, argv.source_table]);

  const sqlTypeToElastic = (type) => {
    type = type.toLowerCase();
    if (type.includes('char'))
      return {
        type: 'text',
        fields: {
          keyword: {
            type: 'keyword',
            ignore_above: 256
          }
        }
      };
    if (type.includes('int'))
      return { type: 'long' };
    if (type.includes('float'))
      return { type: 'float' };
    if (type.includes('date'))
      return { type: 'date', ignore_malformed: true };
    return type;
  };

  const sqlToElasticSchema = function(q) {
    return p.query(q).then(columns => {
      return Promise.resolve(columns.reduce((r, c) => {
        r[c.COLUMN_NAME] = sqlTypeToElastic(c.COLUMN_TYPE);
        return r;
      }, {}));
    }).then(d => ({ properties: d }));
  };

  return {
    elastic: {
      mapping: () => sqlToElasticSchema(schema_query)
    },
    recordCount : () => p.query(mysql.format('SELECT COUNT(*) AS recordCount FROM ?? ' + (argv.where ? ' where ' + argv.where : ''), [argv.source_table])).then(d => d[0].recordCount),
    stream : () => etl.toStream(function() {
      return p.query(key_query)
        .then( keys => {
          const orderedKeys = keys
            .sort((a, b) => a.Seq_in_index - b.Seq_in_index)
            .map(k => k.Column_name);
          const stream = p.stream(query)
            .pipe(etl.map(d => {
              d._id = d._id || orderedKeys.map(f => d[f]).join('');
              return d;
            }));
          stream.promise().then(() => pool.end());
          return stream;
        });
    })
  };
};

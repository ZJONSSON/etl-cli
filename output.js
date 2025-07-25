const minimist = require('minimist');
const etl = require('etl');
const path = require('path');
const Bluebird = require('bluebird');
const nconf = require('nconf');
const fs = require('fs');
const { safeRequire } = require('./util');
const { Readable } = require('stream');

module.exports = async function(obj, argv) {
  let vm, select, remove;
  argv = Object.assign({}, argv || minimist(process.argv.slice(2)));
  argv.Σ_skipped = 0;
  let dest = argv.target || argv?._?.[0];
  dest = dest?.replace(/^s3:\//, 's3files');
  if (!argv.target_dir && dest) {
    argv.target_dir = dest.split('/').slice(1).join('/');
  }
  argv.target_params = dest?.split('/')?.slice(1) || [];

  // If a config file is specified we load it
  if (argv.config)
    nconf.load(path.resolve(argv.config));

  if (argv.select)
    select = argv.select.split(',');

  if (argv.remove)
    remove = argv.remove.split(',');

  // If dest has '/'
  if (dest && dest.match('/')) {
    dest = dest.split('/');

    const trialPath = dest.slice(0, dest.length - 1).join('/');
    // If dest is not a path, we break it up into target_index and target_collection
    if (!fs.existsSync(trialPath)) {
      argv.target_index = argv.target_index || dest[1];
      argv.target_collection = argv.target_collection || argv.target_index;
      argv.target_indextype = argv.target_indextype || dest.slice(2).join('/');

      dest = dest[0];
    } else {
      dest = dest.join('/');
    }
  }

  // Load custom config for the target_type or output
  const conf = nconf.get(dest) || {};
  for (const key in conf)
    argv['target_' + key] = argv['target_' + key] || conf[key];
  argv.target_config = conf;

  let filter;

  if (argv.filter) {
    vm = require('vm');
    filter = vm.runInNewContext(`ret = ${argv.filter}`);
  }


  //  If not silent, we write periodic updates to console
  if (obj.recordCount)
    Bluebird.try(() => obj.recordCount(argv))
      .catch(console.log)
      .then(d => total = d)
      .then(() => console.log('total', total));


  let Σ_out = 0, Σ_in = 0, last = 0, counter, total;
  if (!argv.silent) {
    counter = setInterval(() => {
      const Δ = Σ_in - last;
      last = Σ_in;
      total = argv.recordCount || total;
      const heap = Math.round(process.memoryUsage().heapUsed / 1000000);
      const skipped = argv.Σ_skipped ? ` (${argv.Σ_skipped} skipped) ` : '';
      console.log(`Σ${Σ_in} Δ${Δ} ${total && (Math.floor(Σ_in / total * 10000) / 100) + '%' || ''} (output: Σ${Σ_out}) ${skipped}- Heap: ${heap} Mb`);
    }, argv.report_interval || 1000);
  }

  const m = /\.(json|csv|parquet|raw)/.exec(dest);
  argv.target_type = argv.target_type || (m && m[1]) || (dest && dest.toLowerCase()) || 'screen';
  const type = argv.target_type;

  if (!argv.silent) {
    console.log(`Target: ${dest} - type ${type}  ${ (!!argv.upsert && 'w/upsert') || (!!argv.update && 'w/update') || ''}`);
  }

  let stream = etl.toStream(function() {
    return typeof obj.stream == 'function' ? obj.stream.call(this, argv) : obj.stream;
  });

  stream.on('error', e => {
    console.error('error', e);
    process.exit();
  });

  stream = stream.pipe(etl.map(d => {
    Σ_in++;
    if (d.body) {
      const origBody = d.body;
      d.body = async () => {
        let body = await (typeof origBody === 'function' ? origBody(true) : origBody);
        if (typeof body.pipe !== 'function') {
          if (body && typeof body === 'object') {
            body = JSON.stringify(body);
          }
          body = Readable.from([].concat(body));
        }
        return body;
      };
      d.buffer = async function() {
        const body = await (typeof d.body === 'function' ? d.body(true) : d.body);
        if (typeof body === 'string' || Buffer.isBuffer(body)) {
          return body;
        };
        return Buffer.concat(await Readable.from(body).toArray());

      };
    }
    if (d.__line !== undefined) {
      Object.defineProperty(d, '__line', { value: d.__line, enumerable: false });
    }
    return d;
  }));

  if (argv.transform && argv.transform.length) {
    obj = {};
    const transform_concurrency = argv.transform_concurrency || argv.concurrency || 1;
    try {
      vm = require('vm');
      const transform = vm.runInNewContext(`ret = ${argv.transform}`);
      stream = stream.pipe(etl.map(transform_concurrency, async function(d) {
        return transform.call(this, d, argv);
      }, {
        catch: console.log
      }));
    } catch(_e) {
      const transforms = argv.transform.split(',');
      for (const i in transforms) {
        const name = transforms[i];
        let transform = await safeRequire(path.resolve('.', name));
        transform = transform.transform || transform.default || transform;


        // If the transform should be chained, we chain instead of map
        if (transform.chain) {
          const chain = typeof transform.chain == 'function' ? transform.chain : transform;
          stream = stream.pipe(etl.chain(incoming => chain(incoming, argv)));
          continue;
        }

        if (typeof transform !== 'function') {
          console.error(`Transform ${name} is not a function`);
          process.exit();
        }

        stream = stream.pipe(etl.map(transform_concurrency, async function(d) {
          return transform.call(this, d, argv);
        }, {
          catch: transform.catch ? function(e, d) { transform.catch.call(this, e, d, argv); } : console.log,
          flush: transform.flush
        }));
      };
    }
  }

  if (argv.chain) {
    obj = {};
    let chain = await safeRequire(path.resolve('.', argv.chain));
    chain = chain.chain || chain;
    stream = stream.pipe(etl.chain(incoming => chain(incoming, argv)));
  }

  if (obj[type] && typeof obj[type].transform === 'function')
    stream = stream.pipe(etl.map(obj[type].transform));

  stream = stream.pipe(etl.map(function(d) {
    if (filter) {
      if (!filter.call(this, d, argv)) {
        return;
      }
    }

    if (argv.setid)
      d._id = d[argv.setid];

    if (argv.select)
      d = select.reduce( (p, key) => {
        if (d[key] !== undefined)
          p[key] = d[key];
        return p;
      }, {});

    if (argv.remove)
      remove.forEach(key => {
        delete d[key];
      });

    total = d.__total || total;

    if (argv.limit && Σ_out > argv.limit)
      return;

    if (!argv.limit || Σ_out <= argv.limit) {
      Σ_out += 1;
      return d;
    }
  }, { highWaterMark: argv.highWaterMark || 100 }));


  argv.dest = dest;

  let output;
  try {
    output = require(path.resolve(__dirname, 'targets', type + '.js'));
  } catch(e) {
    if (e?.code === 'MODULE_NOT_FOUND')
      throw 'target_type ' + type + ' not available';
    else
      throw e;
  }

  if (argv.collect) stream = stream.pipe(etl.collect(argv.collect));

  argv.schema_required = argv.schema_required
    || argv.export_schema
    || argv.export_glue_schema
    || (output.schema && output.schema(argv))
    ? true : false;

  if (obj && !obj.schema && argv.schema_required) {
    const { inferSchema } = require('./schema');
    let resolve;
    const schemaPromise = new Promise(r => resolve = r);
    obj.schema = () => schemaPromise;

    stream = stream.pipe(etl.prescan(argv.prescan_size || 1000, d => {
      resolve(inferSchema(d));
    }));
  };


  if (argv.export_schema) {
    const schema = await obj.schema();
    stream = etl.toStream(schema);
  }

  if (argv.export_glue_schema) {
    const schema = await obj.schema();
    const { glueSchema } = require('./schema');
    stream = glueSchema(schema);
  }

  if (argv.count) {
    if (!obj.recordCount)
      throw 'No Recordcount available';
    stream = etl.toStream([{ recordCount: await obj.recordCount(argv) }]);
  }

  if (argv.target_gzip) {
    stream = stream.pipe(etl.map(d => {
      if (d.filename && d.body) {
        d.filename += '.gz';
        const uncompressed = d.body;
        d.body = async function() {
          const body = typeof uncompressed === 'function' ? uncompressed(true) : uncompressed;
          return Readable.from(await body).pipe(require('zlib').createGzip());
        };
      }
      return d;
    }));
  }

  const o = await output(stream, argv, obj);
  if (o?.pipe) {
    await o.pipe(etl.map(d => {
      if (!argv.silent) console.log(JSON.stringify(d, null, 2));
    })).promise();
  }

  clearInterval(counter);

  if (!argv.silent) {
    let msg = `Completed ${Σ_in} records in and ${Σ_out} record out.`;
    if (argv.Σ_skipped) msg += ` (${argv.Σ_skipped} skipped)`;
    console.log(msg);
  }

  if (argv.exit) {
    setImmediate(() => process.exit());
  }
  const res = { Σ_in, Σ_out };
  if (argv.test) {
    res.data = argv.test;
  }
  Object.defineProperty(res, 'argv', { value: argv });
  if (argv.Σ_skipped) res.Σ_skipped = argv.Σ_skipped;
  return res;
};

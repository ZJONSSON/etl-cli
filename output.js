const minimist = require('minimist');
const etl = require('etl');
const path = require('path');
const Bluebird = require('bluebird');
const nconf = require('nconf');
const fs = require('fs');
const { safeRequire } = require('./util');

module.exports = async function(obj, argv) {
  let validate;
  argv = Object.assign({}, argv || minimist(process.argv.slice(2)));
  argv.Σ_skipped = 0;
  let dest = argv.target || argv?._?.[0];
  if (!argv.target_dir && dest) {
    argv.target_dir = dest.split('/').slice(1).join('/');
  }
  argv.target_params = dest?.split('/')?.slice(1) || [];

  // If a config file is specified we load it
  if (argv.config)
    nconf.load(path.resolve(argv.config));

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

  argv.target_gzip = dest && dest.match(/\.gz$/ig);

  // Load custom config for the target_type or output
  const conf = nconf.get(dest) || {};
  for (const key in conf)
    argv['target_' + key] = argv['target_' + key] || conf[key];
  argv.target_config = conf;

  // Removal should be a regex
  if (argv.remove)
    argv.remove = new RegExp(argv.remove);

  // Filter should be an array
  if (argv.filter)
    argv.filter = argv.filter.split('=');

  if (argv.jsonSchema) {
    const { Ajv } = require('ajv');
    const ajv = new Ajv({ allErrors:true, coerceTypes: true });
    validate = ajv.compile(require(path.resolve('.', argv.jsonSchema)));
    console.log('Validating jsonschema and coercing variables');
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
    if (typeof d.body == 'function') {
      d.buffer = async function() {
        let body = await d.body(true);
        body = await body.pipe(etl.map()).promise();
        return Buffer.concat(body);
      };
    }
    if (d.__line !== undefined) {
      Object.defineProperty(d, '__line', { value: d.__line, enumerable: false });
    }
    return d;
  }));

  if (argv.transform) {
    const transform_concurrency = argv.transform_concurrency || argv.concurrency || 1;
    try {
      const vm = require('vm');
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
    let chain = await safeRequire(path.resolve('.', argv.chain));
    chain = chain.chain || chain;
    stream = stream.pipe(etl.chain(incoming => chain(incoming, argv)));
  }

  if (obj[type] && typeof obj[type].transform === 'function')
    stream = stream.pipe(etl.map(obj[type].transform));

  stream = stream.pipe(etl.map(function(d) {
    if (argv.setid)
      d._id = d[argv.setid];

    if (validate) {
      const valid = validate(d);
      if (!valid) {
        console.error(validate.errors, d);
        throw 'VALIDATION_ERROR';
      }
    }

    if (argv.select)
      d = argv.select.split(',').reduce( (p, key) => {
        if (d[key] !== undefined)
          p[key] = d[key];
        return p;
      }, {});

    if (argv.remove)
      Object.keys(d).forEach(key => {
        if (argv.remove.exec(key))
          delete d[key];
      });

    Σ_out += 1;
    total = d.__total || total;

    if (argv.filter)
      if (d[argv.filter[0]] !== argv.filter[1])
        return;

    if (argv.limit && Σ_out > argv.limit)
      return;

    if (!argv.limit || Σ_out <= argv.limit)
      return d;
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
  if (argv.test) res.data = argv.test;
  if (argv.Σ_skipped) res.Σ_skipped = argv.Σ_skipped;
  return res;
};

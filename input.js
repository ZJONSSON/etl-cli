#! /usr/bin/env node

const path = require('path');
const nconf = require('nconf');
const fs = require('fs');
const { safeRequire } = require('./util');

module.exports = async function(source, argv) {
  const originalSource = source;

  // If source
  if (source && !source.match('http') && !fs.existsSync(source)) {
    source = /^[./]/.exec(source) ? [source] : source.split('/');
    argv.source_index = argv.source_index || source[1];
    argv.source_collection = argv.source_collection || source[1];
    argv.source_database = argv.source_database || argv.source_collection;
    argv.source_table = argv.source_table || source.slice(2).join('/');
    argv.source_indextype = argv.source_indextype || argv.source_table;
    argv.source_query = argv.source_query || argv.query;
    source = source[0];
  }

  // Load custom config for the source_type or source
  const conf = nconf.get(argv.source_type || source);

  for (const key in conf)
    argv['source_' + key] = argv['source_' + key] || conf[key];

  argv.source_config = conf || {
    host: argv.source_host || argv.host || 'localhost',
    port: argv.source_port || argv.port
  };

  // Custom config can redefine the source
  if (argv.source_source)
    source = argv.source_source;

  if (argv.source_query_file) {
    if (/\.(js|ts)$/.test(argv.source_query_file)) {
      argv.source_query = await safeRequire(path.resolve('.', argv.source_query_file));
      if (typeof argv.source_query === 'function') {
        argv.source_query = argv.source_query(argv);
      }
    } else {
      argv.source_query = String(fs.readFileSync(path.resolve('.', argv.source_query_file)));
    }
  }

  // Parse query into JSON
  if (typeof argv.source_query === 'string') {
    try {
      const vm = require('vm');
      argv.source_query = vm.runInNewContext(`ret = ${argv.source_query}`);
    } catch(e) {
      console.error('source_query does not parse into json');
    }
  }

  if (!source && !argv.source_type)
    return console.error('Source missing.  etl [source] [dest] ');

  argv.source = source;

  // Resolve any injections
  for (const key in argv) {
    if (argv[key] && key.indexOf('inject_') === 0) {
      const inject = await module.exports(argv[key], Object.assign({}, argv, { [key]: null }));
      argv[key] = inject.stream(argv);
    }
  }

  let obj;


  // If the file is json or csv we set the correct type
  const match = /\.(json|csv|xlsx|parquet|xml)/.exec(source);
  const type = argv.source_type || (match && match[1]) || source;

  // Find the matching source_type and execute
  const sourcePath = path.resolve(__dirname, 'sources', `${type}.js`);
  if (match || fs.existsSync(sourcePath)) {
    obj = (await safeRequire(sourcePath))(argv);
  } else {
    obj = await safeRequire(path.resolve('.', originalSource));
  }

  if (!obj.stream)
    obj.stream = obj;

  if (!argv.silent)
    console.log(`Source: ${source + (argv.inject ? ' injected' : '')} - type: ${type}`);

    return obj;
};
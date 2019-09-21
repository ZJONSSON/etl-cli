#! /usr/bin/env node

const path = require('path');
const Promise = require('bluebird');
const nconf = require('nconf');
const fs = require('fs');

module.exports = function(source,argv) {
  
  // If source
  if (source && !source.match('http') && !fs.existsSync(source) && source.match('/')) {
    source = source.split('/');
    argv.source_index = argv.source_index || source[1];
    argv.source_collection = argv.source_collection || source[1];
    argv.source_database = argv.source_database || argv.source_collection;
    argv.source_table = argv.source_table || source.slice(2).join('/');
    argv.source_indextype = argv.source_indextype || argv.source_table;
    source = source[0];
  }

  // Load custom config for the source_type or source
  let conf = nconf.get(argv.source_type || source) || {};
  
  for (let key in conf)
    argv['source_'+key] = argv['source_'+key] || conf[key];

  // Custom config can redefine the source
  if (argv.source_source)
    source = argv.source_source;

  if (argv.source_query_file) {
    if (/\.js$/.test(argv.source_query_file)) {
      argv.source_query = require(path.resolve('.',argv.source_query_file));
      if (typeof argv.source_query === 'function') {
        argv.source_query = argv.source_query(argv);
      }
    } else {
      argv.source_query = String(fs.readFileSync(path.resolve('.',argv.source_query_file)));
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

  if (!source)
    return console.error('Source missing.  etl [source] [dest] ');

  argv.source = source;

  // Resolve any injections
  for (let key in argv) {
    if (argv[key] && key.indexOf('inject_') === 0) {
      argv[key] = module.exports(argv[key], Object.assign({}, argv, {[key]: null})).stream(argv);
    }
  }

  let type,obj;


  // If the file is json or csv we set the correct type
  const match = /\.(json|csv|xlsx)/.exec(source);
  type = argv.source_type || (match && match[1]) || source;
  // Find the matching source_type and execute
  if (type) {
    obj = require(path.resolve(__dirname,'sources',type))(argv);
  } else {
    obj = require(path.resolve('./',source));
  }

  if (!obj.stream)
    obj.stream = obj;

  if (argv.schema) {
    if (!argv.silent) console.log(`Using schema ${argv.schema}`);
    Object.assign(obj,require(path.resolve('./',argv.schema)));
  }

  if (!argv.silent)
    console.log(`Source: ${source + (argv.inject ? ' injected' : '')} - type: ${type}`);

  if (argv.count) {
    if (!obj.recordCount)
      throw 'No Recordcount available';
    Promise.try(() => obj.recordCount(argv))
      .then(d => console.log(`Record count: ${d}`))
      .then(() => process.exit());
  } else
    return obj;
};
#! /usr/bin/env node

const path = require('path');
const Promise = require('bluebird');
const nconf = require('nconf');

module.exports = function(source,argv) {
  
  // If source
  if (source && !source.match('http') && source.match('/')) {
    source = source.split('/');
    argv.source_index = argv.source_index || source[1];
    argv.source_collection = argv.source_collection || source[1];
    argv.source_indextype = argv.source_indextype || source[2];
    source = source[0];
  }

  // Load custom config for the source_type or source
  let conf = nconf.get(argv.source_type || source) || {};
  console.log(argv.source_type,source,conf)
  for (let key in conf)
    argv['source_'+key] = argv['source_'+key] || conf[key];

  // Custom config can redefine the source
  if (argv.source_source)
    source = argv.source_source;

  // Parse query into JSON
  if (typeof argv.source_query === 'string') {
    try { argv.source_query = JSON.parse(argv.source_query);}
    catch(e) {}
  }

  if (!source)
    return console.log('Source missing.  etl [source] [dest] ');

  argv.source = source;

  // Resolve any injections
  for (let key in argv) {
    if (key.indexOf('inject_') === 0) {
      argv[key] = module.exports(argv[key],{inject: true}).stream();
    }
  }

  let type,obj;

  // if the source is a node file we require it (optionally defining schema)
  if (/\.js$/.exec(source)) {
    obj = require(path.resolve('./',source));
  } else {
    // If the file is json or csv we set the correct type
    const match = /\.(json|csv)/.exec(source);
    type = argv.source_type || (match && match[1]) || source;
    // Find the matching source_type and execute
    obj = require(path.resolve(__dirname,'sources',type))(argv);
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
    Promise.try(obj.recordCount)
      .then(d => console.log(`Record count: ${d}`))
      .then(() => process.exit());
  } else
    return obj;
};
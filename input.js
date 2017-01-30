#! /usr/bin/env node

const fs = require('fs');
const path = require('path');
const minimist = require('minimist');
const etl = require('etl');
const output = require('./output');
const Promise = require('bluebird');
const nconf = require('nconf')
  .file({file: process.env.ETL_CONFIG || path.resolve(process.env.HOME || process.env.USERPROFILE,'.etlconfig.json')});

module.exports = function() {
  const argv = minimist(process.argv.slice(2));
  let source = argv.source;
  
  // If source is not explicitly defined, we assume its the first argument
  if (!source) {
    source = argv._[0];
    argv._ = argv._.slice(1);
  }

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

  let stream,type,obj;

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
    console.log(`Source: ${source} - type: ${type}`);

  if (argv.count) {
    if (!obj.recordCount)
      throw 'No Recordcount available';
    return Promise.try(obj.recordCount)
      .then(d => console.log(`Record count: ${d}`))
      .then(() => process.exit());
  }

  return {obj,argv};
};
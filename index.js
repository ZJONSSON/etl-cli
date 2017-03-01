const path = require('path');
const minimist = require('minimist');
const nconf = require('nconf')
  .file({file: process.env.ETL_CONFIG || path.resolve(process.env.HOME || process.env.USERPROFILE,'.etlconfig.json')});


const input = require('./input');
const output = require('./output');

module.exports = require('./output');

if (!module.parents) {
  const argv = minimist(process.argv.slice(2));
  let source = argv.source;
  
  // If source is not explicitly defined, we assume its the first argument
  if (!source) {
    source = argv._[0];
    argv._ = argv._.slice(1);
  }

  const _input = input(source,argv);
  if (_input) output(_input,argv);
}

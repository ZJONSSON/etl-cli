#! /usr/bin/env node
const path = require('path');
const nconf = require('nconf')
  .file({file: process.env.ETL_CONFIG || path.resolve(process.env.HOME || process.env.USERPROFILE,'.etlconfig.json')});


const input = require('./input');
const output = require('./output');

module.exports = require('./output');

if (!module.parents)
  require('./input')();

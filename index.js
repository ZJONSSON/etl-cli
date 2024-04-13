#! /usr/bin/env node
const path = require('path');
const request = require('request');
const fetch = require('node-fetch');
const fetchCookie = require('fetch-cookie');
const HttpsProxyAgent = require('https-proxy-agent');
const etl = require('etl');
const minimist = require('minimist');
const nconf = require('nconf')
  .file({file: process.env.ETL_CONFIG || path.resolve(process.env.HOME || process.env.USERPROFILE,'.etlconfig.json')});


const input = require('./input');
const output = require('./output');

async function main(argv) {
  require('ts-node').register({
    transpileOnly: argv.ts_transpile === 'false' ? false : true,
    project: argv.ts_project
  });
  let source = argv.source;

  // replace proxy from config (if found)
  if (argv.proxy && nconf.get(argv.proxy))
    argv.proxy = nconf.get(argv.proxy);

  argv.userAgent = argv.userAgent || nconf.get('user-agent') || 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36';

  // expose nconf and etl in the argv
  argv.nconf = nconf;
  argv.etl = etl;

  // getProxy returns a new proxy string where {{random}} has been replaced with a random number
  argv.getProxy = () =>  argv.proxy ? argv.proxy.replace('{{random}}',String(Math.random())) : undefined;

  // inject node-fetch with proxy injection
  argv.fetch = async (url, opt) => {
    opt = opt || {};
    if (opt.proxy && !argv.proxy) throw '--proxy missing';
    opt.headers = opt.headers || {};
    opt.headers['user-agent'] = opt.headers['user-agent'] || argv.userAgent;
    if (argv.proxy) opt = Object.assign({agent: new HttpsProxyAgent(argv.getProxy())},opt);
    const fetchFn = opt.jar ? fetchCookie(fetch, opt.jar, false) : fetch;
    return fetchFn(url, opt);
  }

  // Include default request / requestAsync that use proxy (if supplied) automatically
  argv.request = d => request(Object.assign({proxy: argv.getProxy()},d));
  argv.requestAsync = d => new Promise( (resolve, reject) => {
    request(Object.assign({proxy: argv.getProxy()},d), (err, res) => err ? reject(err) : resolve(res));
  });
  
  // If source is not explicitly defined, we assume its the first argument
  if (!source) {
    source = argv?._?.[0];
    argv._ = argv?._?.slice(1);
  }

  const _input = await input(source,argv);
  if (_input) return output(_input,argv).catch(e => {
    console.error(e);
    process.exit();
  });
}

if (!module.parent) {
  console.log('parents')
  const argv = minimist(process.argv.slice(2));
  return main(argv);
}

module.exports = main;
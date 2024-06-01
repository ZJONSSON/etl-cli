module.exports.safeRequire = async function(path) {
  try {
    return require(path);
  } catch(e) {
    const mdl = await import(path);
    return mdl.default || mdl;
  }
}

module.exports.createConfig = function(config, argv, prefix, keys) {
  config = {...config};
  keys.forEach(key => {
    if (config[key] == undefined) {
      console.log(key, `${prefix}_${key}`)
      config[key] = argv[`${prefix}_${key}`] || argv[key];
    }
  })
  return config;
}
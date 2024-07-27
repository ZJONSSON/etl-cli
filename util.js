module.exports.safeRequire = async function(path) {
  try {
    return require(path);
  } catch(e) {
    if (!e.message || !e.message.includes('Cannot find module')) {
      throw e;
    }
    const mdl = await import(path);
    return mdl.default || mdl;
  }
};

module.exports.createConfig = function(config, argv, prefix, keys) {
  config = { ...config };
  if (keys) {
    keys.forEach(key => {
      if (config[key] == undefined) {
        config[key] = argv[`${prefix}_${key}`] || argv[key];
      }
    });
  } else {
    Object.keys(argv).forEach(key => {
      if (key.startsWith(prefix + '_')) {
        let value = argv[key];
        if (value === 'true') value = true;
        if (value === 'false') value = false;
        config[key.replace(prefix + '_', '')] = value;
      }
    });
  }
  return config;
};
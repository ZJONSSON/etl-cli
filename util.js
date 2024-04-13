module.exports.safeRequire = async function(path) {
  try {
    return require(path);
  } catch(e) {
    const mdl = await import(path);
    return mdl.default || mdl;
  }
}
module.exports.stream = async function* (argv) {
  yield { a: argv.n || 1 };
  yield { a: 2 };
};

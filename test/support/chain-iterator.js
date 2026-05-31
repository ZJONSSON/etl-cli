async function* chainIterator(incoming, argv) {
  for await (const d of incoming) {
    yield { ...d, chained: true, n: argv.n || 0 };
  }
}

module.exports = chainIterator;
module.exports.chain = chainIterator;

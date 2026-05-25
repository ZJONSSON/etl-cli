module.exports = async function* (d, argv) {
  const n = +argv.n || +d.a;
  for (let i = 0; i < n; i++) yield { i, parent: d.a };
};

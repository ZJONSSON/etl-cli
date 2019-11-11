const express = require('express');

express()
  .get('/', (req,res) => {
    res.end('ok');
  })
  .listen(8080);
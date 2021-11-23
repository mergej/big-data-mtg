const express = require('express');
const compression = require('compression');
const bodyParser = require('body-parser');

const port = 4000;
const ROOT_URL_API = '/api';

const app = express();

app.use(compression());
app.use(bodyParser.json());

// controller
const cardController = require('./controller/card.controller');

// use controller
app.use(ROOT_URL_API + '/cards', cardController);

// ---- START UP THE NODE SERVER  ----
app.listen(port, function() {
    console.log('Node Express server for ' + app.name + ' listening on http://localhost:' + port);
});
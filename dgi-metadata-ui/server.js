#!/usr/bin/env node

'use strict';

/**
 * Module dependencies.
 */
var express = require('express');

/**
 * Main application entry file.
 * Please note that the order of loading is important.
 */

// Initializing system variables
var config = require('./server/config/config'),
    app = express();

// Express settings
require('./server/config/express')(app);

// Start the app by listening on <port>
var port = process.env.PORT || config.port;
app.listen(port);

console.log('Environment is = "' + config.nodeEnv + '"');
console.log('Express app started on port ' + port + ' using config\n', JSON.stringify(config, null, 4));

// Expose app
module.exports = app;

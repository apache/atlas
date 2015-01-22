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
var config = require('./server/config/config');

// Load configurations
// Set the node enviornment variable if not set before
process.env.NODE_ENV = config.nodeEnv;

var app = express();

// Express settings
require('./server/config/express')(app);

// Start the app by listening on <port>
var port = process.env.PORT || config.port;
app.listen(port);

console.log('Environment is = "' + process.env.NODE_ENV + '"');
console.log('Express app started on port ' + port + ' using config\n', config);

// Expose app
module.exports = app;

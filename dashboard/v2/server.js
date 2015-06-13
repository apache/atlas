#!/usr/bin/env node

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

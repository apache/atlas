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
var express = require('express'),
    expressLoad = require('express-load'),
    consolidate = require('consolidate'),
    helpers = require('view-helpers'),
    cookieParser = require('cookie-parser'),
    compress = require('compression'),
    bodyParser = require('body-parser'),
    favicon = require('serve-favicon'),
    methodOverride = require('method-override'),
    swig = require('swig'),
    proxit = require('proxit'),
    config = require('./config');

module.exports = function(app) {
    app.set('showStackError', true);

    // Prettify HTML
    app.locals.pretty = true;
    // cache=memory or swig dies in NODE_ENV=production
    app.locals.cache = 'memory';

    // The cookieParser should be above session
    app.use(cookieParser());

    app.use(proxit(config.proxit));
    // Should be placed before express.static
    // To ensure that all assets and data are compressed (utilize bandwidth)
    app.use(compress({
        filter: function(req, res) {
            return (/json|text|javascript|css/).test(res.getHeader('Content-Type'));
        },
        // Levels are specified in a range of 0 to 9, where-as 0 is
        // no compression and 9 is best compression, but slowest
        level: 9
    }));

    // assign the template engine to .html files
    app.engine('html', consolidate[config.templateEngine]);

    // set .html as the default extension
    app.set('view engine', 'html');

    // Set views path, template engine and default layout
    app.set('views', config.root + '/public/views');

    // Enable jsonp
    app.enable('jsonp callback');

    app.use(methodOverride());

    // Dynamic helpers
    app.use(helpers(config.app.name));

    // Connect flash for flash messages
    //app.use(flash());

    // Setting the fav icon and static folder
    app.use(favicon(__dirname + '/../../public/img/favicon.ico'));

    app.use(bodyParser.urlencoded());

    /**
     * User json parser after proxied requests. If its used before proxied requests post request doesn't work
     * */
    app.use(bodyParser.json());

    /*
     * Make app details available to routes
     * */

    app.config = config;

    /**
     * System Routes
     * */
    expressLoad('../routes/system', {
        extlist: /(.*)\.(js$)/,
        cwd: __dirname
    }).into(app);

    var viewsDir = config.root + '/public';
    app.use(express.static(viewsDir));

    if (process.env.NODE_ENV === 'local') {
        // Swig will cache templates for you, but you can disable
        // that and use Express's caching instead, if you like:
        app.set('view cache', false);
        // To disable Swig's cache, do the following:
        swig.setDefaults({
            cache: false
        });
    }

    // Assume "not found" in the error message is a 404. this is somewhat
    // silly, but valid, you can do whatever you like, set properties,
    // use instanceof etc.
    app.use(function(err, req, res, next) {
        // Treat as 404
        if (~err.message.indexOf('not found')) return next();

        // Log it
        console.error(err.stack);

        // Error page
        res.status(500).send('500', {
            error: err.stack
        });
    });

    // Assume 404 since no middleware responded
    app.use(function(req, res) {
        res.status(404).send('404', {
            url: req.originalUrl,
            error: 'Not found'
        });
    });

    // Do not stop server on any unhandled error
    process.on('uncaughtException', function(err) {
        console.error('UNCAUGHT EXCEPTION\n' + err.stack || err.message);
    });

};

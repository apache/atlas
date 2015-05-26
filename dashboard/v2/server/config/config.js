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

// Extend the base configuration in all.js with environment
// specific configuration

var path = require('path'),
    oneLevelUp = path.sep + '..',
    rootPath = path.normalize(__dirname + oneLevelUp + oneLevelUp),
    packageJson = require(rootPath + path.sep + 'package.json'),
    config = require('rc')(packageJson.name, {
        app: {
            name: packageJson.name,
            title: 'DGI'
        },
        nodeEnv: 'local',
        root: rootPath,
        port: process.env.PORT || 3010,
        templateEngine: 'swig',
        proxit: {
            verbose: true,
            hosts: [{
                routes: {
                    '/api': 'http://162.249.6.39:21000/api'
                }
            }]
        }
    });
// Set the node environment variable if not set before
config.nodeEnv = process.env.NODE_ENV = process.env.NODE_ENV || config.nodeEnv;
module.exports = config;

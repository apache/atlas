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
            title: 'DGC'
        },
        nodeEnv: 'local',
        root: rootPath,
        port: process.env.PORT || 3010,
        templateEngine: 'swig',
        proxit: {
            verbose: true,
            hosts: [{
                routes: {
                    '/api': 'http://162.249.6.76:21000/api'
                }
            }]
        }
    });

module.exports = config;

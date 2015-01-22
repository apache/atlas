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
        templateEngine: 'swig'
    });

module.exports = config;

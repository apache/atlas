'use strict';

module.exports = function(grunt) {
    // Project Configuration
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),
        watch: {
            options: {
                livereload: 35730
            },
            js: {
                files: ['public/**/*.js', '!public/lib/**'],
                tasks: ['jshint']
            },
            html: {
                files: ['public/**/*.html']
            },
            css: {
                files: ['public/**/*.css']
            }
        },
        jshint: {
            all: {
                src: ['gruntfile.js', 'package.json', 'server.js', 'server/**/*.js', 'public/**/*.js', '!public/lib/**'],
                options: {
                    jshintrc: true
                }
            }
        },
        nodemon: {
            local: {
                script: 'server.js',
                options: {
                    ext: 'js,json',
                    ignore: ['public/**', 'node_modules/**'],
                    nodeArgs: ['--debug=6868']
                }
            },
            prod: {
                script: 'server.js',
                options: {
                    ignore: ['.'],
                    env: {
                        NODE_ENV: 'production'
                    }
                }
            }
        },
        concurrent: {
            tasks: ['nodemon:local', 'watch'],
            options: {
                logConcurrentOutput: true
            }
        },
        jsbeautifier: {
            'default': {
                src: ['<%= jshint.all.src %>', 'bower.json'],
                options: {
                    js: {
                        preserveNewlines: true,
                        maxPreserveNewlines: 2
                    }
                }
            },
            'build': {
                src: '<%= jsbeautifier.default.src %>',
                options: {
                    mode: 'VERIFY_ONLY',
                    js: '<%= jsbeautifier.default.options.js%>'
                }
            }
        },
        bower: {
            install: {
                options: {
                    verbose: true
                }
            }
        }
    });

    //Load NPM tasks
    require('load-grunt-tasks')(grunt);

    //Default task(s).
    grunt.registerTask('default', ['bower', 'jsbeautifier:default']);

    // Server task
    grunt.registerTask('server', ['bower', 'jshint', 'concurrent']);
    grunt.registerTask('server:prod', ['nodemon:prod']);
};

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

var git = require('git-rev');
module.exports = function(grunt) {
    var classPathSep = (process.platform === "win32") ? ';' : ':',
        gitHash = '',
        pkg = grunt.file.readJSON('package.json'),
        buildTime = new Date().getTime(),
        distPath = 'dist',
        libPath = distPath + '/js/libs',
        isDashboardDirectory = grunt.file.isDir('public'),
        modulesPath = 'public/';
    if (!isDashboardDirectory) {
        modulesPath = '../public/'
    }

    grunt.initConfig({
        watch: {
            js: {
                files: ['public/**/*.js'],
                tasks: ['copy:dist']
            },
            html: {
                files: ['public/**/*.html'],
                tasks: ['copy:dist']
            },
            css: {
                files: ['public/**/*.scss', 'public/**/*.css'],
                tasks: ['copy:dist', 'sass']
            },
            image: {
                files: ['public/**/*.{ico,gif,png}'],
                tasks: ['copy:dist']
            }
        },
        connect: {
            server: {
                options: {
                    port: 9999,
                    base: distPath,
                    // change this to '0.0.0.0' to access the server from outside
                    hostname: '0.0.0.0',
                    middleware: function(connect, options, defaultMiddleware) {
                        var proxy = require('grunt-connect-proxy/lib/utils').proxyRequest;
                        return [
                            // Include the proxy first
                            proxy
                        ].concat(defaultMiddleware);

                    }

                },
                proxies: [{
                    context: '/api', // the context of the data service
                    host: '127.0.0.1',
                    port: 21000, // the port that the data service is running on
                    ws: true,
                    changeOrigin: false,
                    https: false,
                    xforward: false,
                    //xforward: false
                }],
            },
        },
        devUpdate: {
            main: {
                options: {
                    updateType: 'force'
                }
            }
        },
        compress: {
            release: {
                options: {
                    archive: function() {
                        return [pkg.name, pkg.version, gitHash].join('_') + '.tgz';
                    }
                },
                src: ['node_modules/**', 'package.json', 'server.js', 'server/**', 'public/**', '!public/js/**', '!public/modules/**/*.js']
            }
        },
        npmcopy: {
            // Javascript 
            js: {
                options: {
                    destPrefix: libPath
                },
                files: {
                    'jquery/js': 'jquery/dist/jquery.min.js',
                    'requirejs': 'requirejs/require.js',
                    'requirejs-text': 'requirejs-text/text.js',
                    'underscore': 'underscore/underscore-min.js',
                    'bootstrap/js': 'bootstrap/dist/js/bootstrap.min.js',
                    'backbone': 'backbone/backbone-min.js',
                    'backbone-babysitter': 'backbone.babysitter/lib/backbone.babysitter.min.js',
                    'backbone-marionette': 'backbone.marionette/lib/backbone.marionette.min.js',
                    'backbone-paginator': 'backbone.paginator/lib/backbone.paginator.min.js',
                    'backbone-wreqr': 'backbone.wreqr/lib/backbone.wreqr.min.js',
                    'backgrid/js': 'backgrid/lib/backgrid.js',
                    'backgrid-filter/js': 'backgrid-filter/backgrid-filter.min.js',
                    'backgrid-orderable-columns/js': 'backgrid-orderable-columns/backgrid-orderable-columns.js',
                    'backgrid-paginator/js': 'backgrid-paginator/backgrid-paginator.min.js',
                    'backgrid-sizeable-columns/js': 'backgrid-sizeable-columns/backgrid-sizeable-columns.js',
                    'backgrid-columnmanager/js': 'backgrid-columnmanager/src/Backgrid.ColumnManager.js',
                    'jquery-asBreadcrumbs/js': 'jquery-asBreadcrumbs/dist/jquery-asBreadcrumbs.min.js',
                    'd3': 'd3/d3.min.js',
                    'd3/': 'd3-tip/index.js',
                    'dagre-d3': 'dagre-d3/dist/dagre-d3.min.js',
                    'select2': 'select2/dist/js/select2.full.min.js',
                    'backgrid-select-all': 'backgrid-select-all/backgrid-select-all.min.js',
                    'moment/js': 'moment/min/moment.min.js',
                    'jquery-placeholder/js': 'jquery-placeholder/jquery.placeholder.js',
                    'platform': 'platform/platform.js',
                    'jQueryQueryBuilder/js': 'jQuery-QueryBuilder/dist/js/query-builder.standalone.min.js',
                    'bootstrap-daterangepicker/js': 'bootstrap-daterangepicker/daterangepicker.js',
                    'nvd3': 'nvd3/build/nv.d3.min.js',
                    'sparkline': 'jquery-sparkline/jquery.sparkline.min.js'
                }
            },
            css: {
                options: {
                    destPrefix: libPath
                },
                files: {
                    'bootstrap/css': 'bootstrap/dist/css/bootstrap.min.css',
                    'bootstrap/fonts': 'bootstrap/fonts/glyphicons-halflings-regular.woff2',
                    'backgrid/css': 'backgrid/lib/backgrid.css',
                    'backgrid-filter/css': 'backgrid-filter/backgrid-filter.min.css',
                    'backgrid-orderable-columns/css': 'backgrid-orderable-columns/backgrid-orderable-columns.css',
                    'backgrid-paginator/css': 'backgrid-paginator/backgrid-paginator.css',
                    'backgrid-sizeable-columns/css': 'backgrid-sizeable-columns/backgrid-sizeable-columns.css',
                    'backgrid-columnmanager/css': 'backgrid-columnmanager/lib/Backgrid.ColumnManager.css',
                    'jquery-asBreadcrumbs/css': 'jquery-asBreadcrumbs/dist/css/asBreadcrumbs.min.css',
                    'select2/css': 'select2/dist/css/select2.min.css',
                    'backgrid-select-all': 'backgrid-select-all/backgrid-select-all.min.css',
                    'font-awesome/css': 'font-awesome/css/font-awesome.min.css',
                    'font-awesome/fonts': 'font-awesome/fonts',
                    'jQueryQueryBuilder/css': 'jQuery-QueryBuilder/dist/css/query-builder.default.min.css',
                    'bootstrap-daterangepicker/css': 'bootstrap-daterangepicker/daterangepicker.css',
                    'nvd3/css': 'nvd3/build/nv.d3.min.css'
                }

            },
            license: {
                options: {
                    destPrefix: libPath
                },
                files: {
                    'jquery': 'jquery/LICENSE.txt',
                    'requirejs-text': 'requirejs-text/LICENSE',
                    'underscore': 'underscore/LICENSE',
                    'bootstrap': 'bootstrap/LICENSE',
                    'backbone-babysitter': 'backbone.babysitter/LICENSE.md',
                    'backbone-marionette': 'backbone.marionette/license.txt',
                    'backbone-paginator': 'backbone.paginator/LICENSE-MIT',
                    'backbone-wreqr': 'backbone.wreqr/LICENSE.md',
                    'backgrid': 'backgrid/LICENSE-MIT',
                    'backgrid-filter': 'backgrid-filter/LICENSE-MIT',
                    'backgrid-orderable-columns': 'backgrid-orderable-columns/LICENSE-MIT',
                    'backgrid-paginator': 'backgrid-paginator/LICENSE-MIT',
                    'backgrid-sizeable-columns': 'backgrid-sizeable-columns/LICENSE-MIT',
                    'backgrid-columnmanager': 'backgrid-columnmanager/LICENSE',
                    'jquery-asBreadcrumbs': 'jquery-asBreadcrumbs/LICENSE',
                    'd3': 'd3/LICENSE',
                    'd3/': 'd3-tip/LICENSE',
                    'dagre-d3': 'dagre-d3/LICENSE',
                    'backgrid-select-all': 'backgrid-select-all/LICENSE-MIT',
                    'jquery-placeholder': 'jquery-placeholder/LICENSE.txt',
                    'platform/': 'platform/LICENSE',
                    'jQueryQueryBuilder/': 'jQuery-QueryBuilder/LICENSE',
                    'nvd3/': 'nvd3/LICENSE.md'
                }
            }
        },
        sass: {
            dist: {
                files: {
                    'dist/css/style.css': 'public/css/scss/style.scss'
                }
            },
            build: {
                files: {
                    'dist/css/style.css': 'dist/css/scss/style.scss'
                }
            }
        },
        copy: {
            dist: {
                expand: true,
                cwd: modulesPath,
                src: ['**', '!**/scss/**'],
                dest: distPath
            },
            build: {
                expand: true,
                cwd: modulesPath,
                src: ['**'],
                dest: distPath
            }
        },
        clean: {
            build: [distPath, libPath],
            options: {
                force: true
            }
        },
        uglify: {
            build: {
                options: {
                    sourceMap: true
                },
                files: [{
                    expand: true,
                    cwd: 'dist/js',
                    src: '**/*.js',
                    dest: 'dist/js'
                }]
            }
        },
        cssmin: {
            build: {
                files: [{
                    expand: true,
                    cwd: 'dist/css',
                    src: '*.css',
                    dest: 'dist/css'
                }]
            }

        },
        htmlmin: {
            build: {
                options: {
                    removeComments: true,
                    collapseWhitespace: true
                },
                files: [{
                    expand: true,
                    cwd: 'dist/js/templates',
                    src: '**/*.html',
                    dest: 'dist/js/templates'
                }]
            }
        },
        template: {
            build: {
                options: {
                    data: {
                        'bust': buildTime
                    }
                },
                files: {
                    [distPath + '/index.html']: [modulesPath + '/index.html.tpl']
                }
            }
        }
    });

    grunt.loadNpmTasks('grunt-connect-proxy');
    grunt.loadNpmTasks('grunt-contrib-connect');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-npmcopy');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-cssmin');
    grunt.loadNpmTasks('grunt-contrib-htmlmin');
    grunt.loadNpmTasks('grunt-template');

    require('load-grunt-tasks')(grunt);

    grunt.registerTask('default', [
        'devUpdate',
        'npmcopy:js',
        'npmcopy:css'
    ]);

    grunt.registerTask('server', ['clean', 'copy:dist', 'concurrent', 'watch']);

    grunt.registerTask('dev', [
        'clean',
        'npmcopy:js',
        'npmcopy:css',
        'npmcopy:license',
        'copy:dist',
        'sass:dist',
        'template',
        'configureProxies:server',
        'connect:server',
        'watch'
    ]);

    grunt.registerTask('build', [
        'clean',
        'npmcopy:js',
        'npmcopy:css',
        'npmcopy:license',
        'copy:build',
        'sass:build',
        'template'
    ]);

    grunt.registerTask('dev-minify', [
        'clean',
        'npmcopy:js',
        'npmcopy:css',
        'npmcopy:license',
        'copy:dist',
        'sass:dist',
        'uglify:build',
        'cssmin:build',
        'template',
        'configureProxies:server',
        'connect:server',
        'watch'
    ]);

    grunt.registerTask('build-minify', [
        'clean',
        'npmcopy:js',
        'npmcopy:css',
        'npmcopy:license',
        'copy:build',
        'sass:build',
        'uglify:build',
        'cssmin:build',
        'template'
    ]);
};
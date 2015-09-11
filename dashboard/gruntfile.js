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
        distPath = '../webapp/target/dist';

    grunt.initConfig({
        watch: {
            options: {
                livereload: 35729
            },
            js: {
                files: ['public/**/*.js', '!public/lib/**', '!public/dist/**', '!public/js/app.min.js'],
                tasks: ['shell']
            },
            html: {
                files: ['public/**/*.html'],
                tasks: ['copy:dist']
            },
            css: {
                files: ['public/**/*.css'],
                tasks: ['copy:dist']
            },
            image: {
                files: ['public/**/*.{ico,gif,png}'],
                tasks: ['copy:dist']
            }
        },
        jshint: {
            all: {
                src: ['gruntfile.js', 'package.json', 'server.js', 'server/**/*.js', 'public/**/*.js', '!public/lib/**', '!public/dist/**', '!public/**/app.min.js'],
                options: {
                    jshintrc: true
                }
            }
        },
        concurrent: {
            tasks: ['watch', 'proxitserver'],
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
                    verbose: true,
                    targetDir: '.bower-components'
                }
            }
        },
        dist: distPath + '/js/app.min.js',
        modules: grunt.file.expand(
            'public/js/app.js',
            'public/js/routes.js',
            'public/modules/**/*Module.js',
            'public/modules/**/*.js',
            'public/js/init.js'
        ).join(' '),
        shell: {
            min: {
                command: 'java ' +
                    '-cp ' + distPath + '/lib/closure-compiler/compiler.jar' + classPathSep +
                    '' + distPath + '/lib/ng-closure-runner/ngcompiler.jar ' +
                    'org.angularjs.closurerunner.NgClosureRunner ' +
                    '--compilation_level SIMPLE_OPTIMIZATIONS ' +
                    //'--formatting PRETTY_PRINT ' +
                    '--language_in ECMASCRIPT5_STRICT ' +
                    '--angular_pass ' +
                    '--manage_closure_dependencies ' +
                    '--js <%= modules %> ' +
                    '--js_output_file <%= dist %>'
            }
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
        copy: {
            dist: {
                expand: true,
                cwd: 'public/',
                src: ['**', '!js/**/*.js', '!modules/**/*.js'],
                dest: distPath
            }
        },
        clean: {
            build: [distPath],
            options: {
                force: true
            }
        },
        proxit: {
            dev: {
                options: {
                    'port': 3010,
                    'verbose': true,
                    'hosts': [{
                        'hostnames': ['*'],
                        'routes': {
                            '/': distPath,
                            '/api': 'http://162.249.6.50:21000/api'
                        }
                    }]
                }
            }
        }
    });

    require('load-grunt-tasks')(grunt);
    grunt.registerTask('default', ['devUpdate', 'bower', 'jshint', 'jsbeautifier:default']);

    grunt.registerTask('server', ['jshint', 'clean', 'bower', 'copy:dist', 'minify', 'concurrent']);
    grunt.registerTask('build', ['copy:dist', 'minify']);

    grunt.registerTask('minify', 'Minify the all js', function() {
        var done = this.async();
        grunt.task.run(['shell:min']);
        done();
    });
    grunt.loadNpmTasks('proxit');
    grunt.registerTask('proxitserver', 'Proxit', function() {
        var done = this.async();
        grunt.task.run(['proxit:dev']);
        done();
    });
    grunt.registerTask('release', 'Create release package', function() {
        var done = this.async();
        git.short(function(str) {
            gitHash = str;
            grunt.task.run(['minify', 'compress:release']);
            done();
        });
    });
};

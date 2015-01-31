'use strict';

module.exports = function(grunt) {
    // Project Configuration
    var classPathSep = (process.platform === "win32") ? ';' : ':';
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),
        watch: {
            options: {
                livereload: 35730
            },
            js: {
                files: ['public/**/*.js', '!public/lib/**', '!public/dist/**'],
                tasks: ['jshint', 'shell']
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
                src: ['gruntfile.js', 'package.json', 'server.js', 'server/**/*.js', 'public/**/*.js', '!public/lib/**', '!public/dist/**'],
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
        },
        // app
        dist: 'public/dist/app.min.js',
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
                    '-cp public/lib/closure-compiler/compiler.jar' + classPathSep +
                    'public/lib/ng-closure-runner/ngcompiler.jar ' +
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
        }
    });

    //Load NPM tasks
    require('load-grunt-tasks')(grunt);

    //Default task(s).
    grunt.registerTask('default', ['devUpdate', 'bower', 'jshint', 'jsbeautifier:default', 'shell:min']);

    // Server task
    grunt.registerTask('server', ['bower', 'jshint', 'concurrent']);
    grunt.registerTask('server:prod', ['nodemon:prod']);
};

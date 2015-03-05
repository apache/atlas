'use strict';

var git = require('git-rev');

module.exports = function(grunt) {
    var classPathSep = (process.platform === "win32") ? ';' : ':',
        gitHash = '',
        pkg = grunt.file.readJSON('package.json');

    grunt.initConfig({
        watch: {
            options: {
                livereload: 35730
            },
            js: {
                files: ['public/**/*.js', '!public/lib/**', '!public/dist/**'],
                tasks: ['shell']
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
        }
    });

    require('load-grunt-tasks')(grunt);

    grunt.registerTask('default', ['devUpdate', 'bower', 'jshint', 'jsbeautifier:default']);

    grunt.registerTask('server', ['bower', 'jshint', 'minify', 'concurrent']);
    grunt.registerTask('server:prod', ['nodemon:prod']);
    grunt.registerTask('server:prod', ['nodemon:prod']);

    grunt.registerTask('minify', 'Minify the all js', function() {
        var done = this.async();
        grunt.file.mkdir('public/dist');
        grunt.task.run(['shell:min']);
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

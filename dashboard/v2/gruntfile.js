'use strict';

var git = require('git-rev');

module.exports = function(grunt) {
    var classPathSep = (process.platform === "win32") ? ';' : ':',
        gitHash = '',
        pkg = grunt.file.readJSON('package.json');

    grunt.initConfig({
        watch: {
            options: {
                livereload: 35729
            },
            js: {
                files: ['public/**/*.js', '!public/lib/**', '!public/dist/**'],
                tasks: ['shell','copy:mainjs']
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
        concurrent: {
            tasks: ['build','watch', 'ngserver'],
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
        },
        nginx: {
            options: {
                config: 'nginx.conf',
            }
        },
        copy: {
        	dist: {
        		expand: true,
        	    cwd: 'public/',
        	    src: '**',
        	    dest: 'dist'
        	 },
        	 mainjs:{
        		expand: true,
    		    cwd: 'public/',
    		    src: 'dist/*.js',
    		    dest: 'dist/dist/',
    		    flatten: true,
    		    filter: 'isFile'
        	 }
        },
        clean: ['dist']
    });

    require('load-grunt-tasks')(grunt);
    grunt.registerTask('default', ['devUpdate', 'bower', 'jshint', 'jsbeautifier:default']);

    grunt.registerTask('server', ['bower', 'jshint', 'minify', 'concurrent']);

    grunt.registerTask('minify', 'Minify the all js', function() {
        var done = this.async();
        grunt.file.mkdir('public/dist');
        grunt.task.run(['shell:min']);
        done();
    });

    grunt.registerTask('ngserver', 'Nginx server', function() {
    	var done = this.async();
        grunt.file.mkdir('logs/log');
        grunt.file.mkdir('temp/client_body_temp');
        grunt.task.run(['nginx:start']);
        done();
    });
    grunt.registerTask('build','Build DGI',function(){
    	grunt.task.run(['clean']);
    	 grunt.task.run(['copy:dist']);
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

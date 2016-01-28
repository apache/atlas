#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import sys

import atlas_config as mc

ATLAS_LOG_OPTS="-Datlas.log.dir=%s -Datlas.log.file=quick_start.log"
ATLAS_COMMAND_OPTS="-Datlas.home=%s"
DEFAULT_JVM_OPTS="-Xmx1024m -Dlog4j.configuration=atlas-log4j.xml"

def main():

    atlas_home = mc.atlasDir()
    confdir = mc.dirMustExist(mc.confDir(atlas_home))
    mc.executeEnvSh(confdir)
    logdir = mc.dirMustExist(mc.logDir(atlas_home))
    if mc.isCygwin():
        # Pathnames that are passed to JVM must be converted to Windows format.
        jvm_atlas_home = mc.convertCygwinPath(atlas_home)
        jvm_logdir = mc.convertCygwinPath(logdir)
    else:
        jvm_atlas_home = atlas_home
        jvm_logdir = logdir

    #create sys property for conf dirs
    jvm_opts_list = (ATLAS_LOG_OPTS % jvm_logdir).split()

    cmd_opts = (ATLAS_COMMAND_OPTS % jvm_atlas_home)
    jvm_opts_list.extend(cmd_opts.split())

    default_jvm_opts = DEFAULT_JVM_OPTS
    atlas_jvm_opts = os.environ.get(mc.ATLAS_OPTS, default_jvm_opts)
    jvm_opts_list.extend(atlas_jvm_opts.split())

    #expand web app dir
    web_app_dir = mc.webAppDir(atlas_home)
    mc.expandWebApp(atlas_home)

    p = os.pathsep
    atlas_classpath = confdir + p \
                       + os.path.join(web_app_dir, "atlas", "WEB-INF", "classes" ) + p \
                       + os.path.join(web_app_dir, "atlas", "WEB-INF", "lib", "*" )  + p \
                       + os.path.join(atlas_home, "libext", "*")
    if mc.isCygwin():
        atlas_classpath = mc.convertCygwinPath(atlas_classpath, True)

    process = mc.java("org.apache.atlas.examples.QuickStart", sys.argv[1:], atlas_classpath, jvm_opts_list)
    return process.wait()

if __name__ == '__main__':
    try:
        returncode = main()
        if returncode == 0:
            print "Example data added to Apache Atlas Server!!!\n"
        else:
            print "No data was added to the Apache Atlas Server.\n"
    except Exception as e:
        print "Exception: %s " % str(e)
        returncode = -1

    sys.exit(returncode)

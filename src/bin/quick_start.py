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

METADATA_LOG_OPTS="-Datlas.log.dir=%s -Datlas.log.file=quick_start.log"
METADATA_COMMAND_OPTS="-Dmetadata.home=%s"
DEFAULT_JVM_OPTS="-Xmx1024m -Dlog4j.configuration=atlas-log4j.xml"

def main():

    metadata_home = mc.metadataDir()
    confdir = mc.dirMustExist(mc.confDir(metadata_home))
    mc.executeEnvSh(confdir)
    logdir = mc.dirMustExist(mc.logDir(metadata_home))

    #create sys property for conf dirs
    jvm_opts_list = (METADATA_LOG_OPTS % logdir).split()

    cmd_opts = (METADATA_COMMAND_OPTS % metadata_home)
    jvm_opts_list.extend(cmd_opts.split())

    default_jvm_opts = DEFAULT_JVM_OPTS
    metadata_jvm_opts = os.environ.get(mc.METADATA_OPTS, default_jvm_opts)
    jvm_opts_list.extend(metadata_jvm_opts.split())

    #expand web app dir
    web_app_dir = mc.webAppDir(metadata_home)
    mc.expandWebApp(metadata_home)

    p = os.pathsep
    metadata_classpath = confdir + p \
                       + os.path.join(web_app_dir, "atlas", "WEB-INF", "classes" ) + p \
                       + os.path.join(web_app_dir, "atlas", "WEB-INF", "lib", "*" )  + p \
                       + os.path.join(metadata_home, "libext", "*")

    process = mc.java("org.apache.atlas.examples.QuickStart", sys.argv[1:], metadata_classpath, jvm_opts_list)
    process.wait()

    print "Example data added to Apache Atlas Server!!!\n"

if __name__ == '__main__':
    try:
        returncode = main()
    except Exception as e:
        print "Exception: %s " % str(e)
        returncode = -1

    sys.exit(returncode)

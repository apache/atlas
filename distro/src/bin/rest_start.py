#!/usr/bin/env python3

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
import traceback

import rest_config as mc


REST_LOG_OPTS="-Drest.log.dir=%s -Drest.log.file=%s.log"
REST_COMMAND_OPTS="-Drest.home=%s"
REST_CONFIG_OPTS="-Drest.conf=%s"
DEFAULT_JVM_HEAP_OPTS="-Xmx1024m"
DEFAULT_JVM_OPTS="-Dlogback.configurationFile=rest-logback.xml -Djava.security.auth.login.config=conf/atlas_jaas.conf -Djava.net.preferIPv4Stack=true -Djdk.util.zip.disableZip64ExtraFieldValidation=true -server"

ATLAS_COMMAND_OPTS="-Datlas.home=%s"



def main():

    is_setup = (len(sys.argv)>1) and sys.argv[1] is not None and sys.argv[1] == '-setup'

    rest_home = mc.restDir()
    atlas_home = mc.atlasDir()
    confdir = mc.dirMustExist(mc.confDir(rest_home))
    mc.executeEnvSh(confdir)
    logdir = mc.dirMustExist(mc.logDir(rest_home))
    mc.dirMustExist(mc.dataDir(rest_home))
    if mc.isCygwin():
        # Pathnames that are passed to JVM must be converted to Windows format.
        jvm_rest_home = mc.convertCygwinPath(rest_home)
        jvm_confdir = mc.convertCygwinPath(confdir)
        jvm_logdir = mc.convertCygwinPath(logdir)
        jvm_atlas_home = mc.convertCygwinPath(atlas_home)
    else:
        jvm_rest_home = rest_home
        jvm_confdir = confdir
        jvm_logdir = logdir
        jvm_atlas_home = atlas_home

    #create sys property for conf dirs
    if not is_setup:
        jvm_opts_list = (REST_LOG_OPTS % (jvm_logdir, "application")).split()
    else:
        jvm_opts_list = (REST_LOG_OPTS % (jvm_logdir, "rest_setup")).split()

    cmd_opts = (REST_COMMAND_OPTS % jvm_rest_home)
    jvm_opts_list.extend(cmd_opts.split())

    cmd_opts2 = (ATLAS_COMMAND_OPTS % jvm_atlas_home)
    jvm_opts_list.extend(cmd_opts2.split())

    config_opts = (REST_CONFIG_OPTS % jvm_confdir)
    jvm_opts_list.extend(config_opts.split())

    rest_server_heap_opts = os.environ.get(mc.REST_SERVER_HEAP, DEFAULT_JVM_HEAP_OPTS)
    jvm_opts_list.extend(rest_server_heap_opts.split())

    rest_server_jvm_opts = os.environ.get(mc.REST_SERVER_OPTS)
    if rest_server_jvm_opts:
        jvm_opts_list.extend(rest_server_jvm_opts.split())

    rest_jvm_opts = os.environ.get(mc.REST_OPTS, DEFAULT_JVM_OPTS)
    jvm_opts_list.extend(rest_jvm_opts.split())

    #expand web app dir
    web_app_dir = mc.webAppDir(rest_home)
    mc.expandWebApp(rest_home)

    p = os.pathsep
    rest_classpath = confdir + p \
                       + os.path.join(web_app_dir,"rest-notification", "WEB-INF", "classes" ) + p \
                       + os.path.join(web_app_dir, "rest-notification","WEB-INF", "lib", "*" )  + p \
                       + os.path.join(rest_home, "libext", "*")



    if mc.isCygwin():
        rest_classpath = mc.convertCygwinPath(rest_classpath, True)

    rest_pid_file = mc.pidFile(rest_home)

    if os.path.isfile(rest_pid_file):
       #Check if process listed in rest.pid file is still running
       pf = open(rest_pid_file, 'r')
       pid = pf.read().strip()
       pf.close()
       if pid != "":
           if mc.exist_pid((int)(pid)):
               if is_setup:
                   print("Cannot run setup when server is running.")
               mc.server_already_running(pid)
           else:
               mc.server_pid_not_running(pid)


    web_app_path = os.path.join(web_app_dir, "rest-notification")

    if (mc.isCygwin()):
        web_app_path = mc.convertCygwinPath(web_app_path)
    if not is_setup:
        start_rest_server(rest_classpath, rest_pid_file, jvm_logdir, jvm_opts_list, web_app_path)
        mc.wait_for_startup(confdir, 300)
        print("Rest Server started!!!\n")
    """ else:
        process = mc.java("org.apache.atlas.web.setup.AtlasSetup", [], atlas_classpath, jvm_opts_list, jvm_logdir)
        return process.wait() """


def start_rest_server(rest_classpath, rest_pid_file, jvm_logdir, jvm_opts_list, web_app_path):
    args = ["-app", web_app_path]
    args.extend(sys.argv[1:])

    process = mc.java("org.apache.atlas.notification.rest.RestNotificationMain", args, rest_classpath, jvm_opts_list, jvm_logdir)
    mc.writePid(rest_pid_file, process)

if __name__ == '__main__':
    try:
        returncode = main()
    except Exception as e:
        print("Exception: %s " % str(e))
        print(traceback.format_exc())
        returncode = -1

    sys.exit(returncode)

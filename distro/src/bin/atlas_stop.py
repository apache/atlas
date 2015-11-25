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
from signal import SIGTERM
import sys
import traceback

import atlas_config as mc

def main():

    metadata_home = mc.metadataDir()
    confdir = mc.dirMustExist(mc.confDir(metadata_home))
    mc.executeEnvSh(confdir)
    piddir = mc.dirMustExist(mc.logDir(metadata_home))

    metadata_pid_file = mc.pidFile(metadata_home)

    try:
        pf = file(metadata_pid_file, 'r')
        pid = int(pf.read().strip())
        pf.close()
    except:
        pid = None
    if not pid:
        sys.stderr.write("No process ID file found. Server not running?\n")
        return
    if  mc.ON_POSIX:

            if not mc.unix_exist_pid(pid):
               sys.stderr.write("Server no longer running with pid %s\nImproper shutdown?\npid file deleted.\n" %pid)
               os.remove(metadata_pid_file)
               return
    else:
        if mc.IS_WINDOWS:
            if not mc.win_exist_pid((str)(pid)):
                sys.stderr.write("Server no longer running with pid %s\nImproper shutdown?\npid file deleted.\n" %pid)
                os.remove(metadata_pid_file)
                return

    os.kill(pid, SIGTERM)

    # assuming kill worked since process check on windows is more involved...
    if os.path.exists(metadata_pid_file):
        os.remove(metadata_pid_file)

if __name__ == '__main__':
    try:
        returncode = main()
    except Exception as e:
        print "Exception: %s " % str(e)
        print traceback.format_exc()
        returncode = -1

    sys.exit(returncode)

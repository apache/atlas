#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This is the script used in the job definition of our Jenkins job Manage-Install-ODF-Testenv
# The original can be foudn in get: odf-test-env/src/main/scripts/jenkins-manage-testenv.sh
#
# The Jenkins job should have the following parameters:
#
# 1. nodelabel: Label parameter. Default: odftestenv
#
# 2. action: Choice parameter with these choices: start, stop, cleanall, cleanconfig, cleanmetadata, install
# Action description:
#Available actions are:
#<ul>
#  <li>install: Remove the existing and install a new test environment build.
#    Installs the most recent successful build by default. To change which build is used
#    set the parameters <em>buildnumber</em> and <em>job</em> accordingly.</li>
#  <li>start: (re)start the test environment</li>
#  <li>stop:  stop the test environment</li>
#  <li>cleanall: (re)starts with clean configuration and clean metadata</li>
#  <li>cleanconfig   (re)starts with clean configuration</li>
#  <li>cleanmetadata (re)starts with clean metadata</li>
#</ul>
#
# 3. jenkinsjob: Choice parameter with choices: Shared-Discovery-Platform, Shared-Discovery-Platform-Parameters
#
# 4. buildnumber: String parmeter with default: lastSuccessfulBuild
#

echo Managing ODF test environment with parameters: action = $action, buildnumber = $buildnumber, jenkinsjob = $jenkinsjob

if [ "$action" = "install" ]; then
  ODFTESTENVTARGETDIR=/home/atlasadmin/odf-test-env
  OUTPUTFILE=/tmp/download-install-odf-testenv.sh

  if [ "$buildnumber" = "" ]; then
    buildnumber=lastSuccessfulBuild
  fi

  if [ "$jenkinsjob" = "" ]; then
    jenkinsjob=Shared-Discovery-Platform
  fi

  echo Downloading build number $buildnumber
  curl https://shared-discovery-platform-jenkins.swg-devops.com:8443/job/$jenkinsjob/$buildnumber/artifact/odf-test-env/src/main/scripts/download-install-odf-testenv.sh --output $OUTPUTFILE

  echo Running installer script on directory $ODFTESTENVTARGETDIR with build number $buildnumber
  chmod 755 $OUTPUTFILE
  export BUILD_ID=dontletjenkinskillme
  echo Running command $OUTPUTFILE $buildnumber $ODFTESTENVTARGETDIR $jenkinsjob
  $OUTPUTFILE $buildnumber $ODFTESTENVTARGETDIR $jenkinsjob
else
  TESTENVDIR=~/odf-test-env/odf-test-env-1.2.0-SNAPSHOT
  export BUILD_ID=dontletjenkinskillme

  $TESTENVDIR/odftestenv.sh $action
fi

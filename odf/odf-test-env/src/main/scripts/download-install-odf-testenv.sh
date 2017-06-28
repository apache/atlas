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

#
# Script to download, start, and configure the ODF test environment.
# JenkinsBuildNumber refers to the build number of the job Open-Discovery-Framework, see here:
# https://shared-discovery-platform-jenkins.swg-devops.com:8443/job/Open-Discovery-Framework
#
# Usage: download-install-odf-testenv.sh [<JenkinsBuildNumber> <Directory> ]
#        Default values:
#             <JenkinsBuildNumber>: lastSuccessfulBuild
#             <Directory>: ~/odf-test-env
#

JENKINSBUILDNUMBER=$1
if [ -z "$JENKINSBUILDNUMBER" ]; then
   JENKINSBUILDNUMBER=lastSuccessfulBuild
   echo Jenkins build number not provided, using default $JENKINSBUILDNUMBER
fi

TESTENVDIR=$2
if [ -z "$TESTENVDIR" ]; then
   TESTENVDIR=~/odf-test-env
   echo Target directory not provided, using default $TESTENVDIR
fi

# hidden third parameter taking the jenkins job name
JENKINSJOB=$3
if [ -z "$JENKINSJOB" ]; then
   JENKINSJOB=Open-Discovery-Framework
   echo Jenkins job not provided, using default $JENKINSJOB
fi

echo Downloading test env to directory $TESTENVDIR, Jenkins build number: $JENKINSBUILDNUMBER


TESTENVVERSION=1.2.0-SNAPSHOT
TESTENVZIP=/tmp/odf-test-env.zip
FULLHOSTNAME=`hostname -f`


echo Downloading ODF test env
curl https://shared-discovery-platform-jenkins.swg-devops.com:8443/job/$JENKINSJOB/$JENKINSBUILDNUMBER/artifact/odf-test-env/target/odf-test-env-$TESTENVVERSION-bin.zip --output $TESTENVZIP

echo Stopping test env if it exists...
$TESTENVDIR/odf-test-env-$TESTENVVERSION/odftestenv.sh stop
sleep 1
echo Test env stopped

echo Removing existing test env directory...
rm -rf $TESTENVDIR/odf-test-env-$TESTENVVERSION
echo Existing test env directory removed

echo Unpacking $TESTENVZIP to $TESTENVDIR
mkdir -p $TESTENVDIR
unzip -q $TESTENVZIP -d $TESTENVDIR

$TESTENVDIR/odf-test-env-$TESTENVVERSION/odftestenv.sh cleanall

echo ODF test env installed and started
echo "Point your browser to https://$FULLHOSTNAME:58081/odf-web-1.2.0-SNAPSHOT to check it out"

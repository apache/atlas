#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`

METADATA_PID_DIR="${METADATA_PID_DIR:-$BASEDIR/logs}"
# create the pid dir if its not there
[ -w "$METADATA_PID_DIR" ] ||  mkdir -p "$METADATA_PID_DIR"
export METADATA_PID_DIR
METADATA_PID_FILE=${METADATA_PID_DIR}/metadata.pid
export METADATA_PID_FILE

if [ -f $METADATA_PID_FILE ]
then
   kill -15 `cat $METADATA_PID_FILE`
   echo Metadata Server stopped
else
   echo "pid file $METADATA_PID_FILE not present"
fi

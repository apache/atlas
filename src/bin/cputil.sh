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

if [ -z "$METADATA_CONF" ]; then
  METADATA_CONF=${BASEDIR}/conf
fi
export METADATA_CONF

if [ -f "${METADATA_CONF}/metadata-env.sh" ]; then
  . "${METADATA_CONF}/metadata-env.sh"
fi

if test -z ${JAVA_HOME}
then
    JAVA_BIN=`which java`
    JAR_BIN=`which jar`
else
    JAVA_BIN=${JAVA_HOME}/bin/java
    JAR_BIN=${JAVA_HOME}/bin/jar
fi
export JAVA_BIN

if [ ! -e $JAVA_BIN ] || [ ! -e $JAR_BIN ]; then
  echo "$JAVA_BIN and/or $JAR_BIN not found on the system. Please make sure java and jar commands are available."
  exit 1
fi

# default the heap size to 1GB
DEFAULT_JAVA_HEAP_MAX=-Xmx1024m
METADATA_OPTS="$DEFAULT_JAVA_HEAP_MAX $METADATA_OPTS"

METADATACPPATH="$METADATA_CONF"

METADATA_EXPANDED_WEBAPP_DIR=${METADATA_EXPANDED_WEBAPP_DIR:-${BASEDIR}/server/webapp}
export METADATA_EXPANDED_WEBAPP_DIR
# set the server classpath
if [ ! -d ${METADATA_EXPANDED_WEBAPP_DIR}/metadata/WEB-INF ]; then
  mkdir -p ${METADATA_EXPANDED_WEBAPP_DIR}/metadata
  cd ${METADATA_EXPANDED_WEBAPP_DIR}/metadata
  $JAR_BIN -xf ${BASEDIR}/server/webapp/metadata.war
  cd -
fi

METADATACPPATH="${METADATACPPATH}:${METADATA_EXPANDED_WEBAPP_DIR}/metadata/WEB-INF/classes"
METADATACPPATH="${METADATACPPATH}:${METADATA_EXPANDED_WEBAPP_DIR}/metadata/WEB-INF/lib/*:${BASEDIR}/libext/*"

# log and pid dirs for applications
METADATA_LOG_DIR="${METADATA_LOG_DIR:-$BASEDIR/logs}"
export METADATA_LOG_DIR

METADATA_PID_DIR="${METADATA_PID_DIR:-$BASEDIR/logs}"
# create the pid dir if its not there
[ -w "$METADATA_PID_DIR" ] ||  mkdir -p "$METADATA_PID_DIR"
export METADATA_PID_DIR
METADATA_PID_FILE=${METADATA_PID_DIR}/metadata.pid
export METADATA_PID_FILE

METADATA_DATA_DIR=${METADATA_DATA_DIR:-${BASEDIR}/data}
METADATA_HOME_DIR="${METADATA_HOME_DIR:-$BASEDIR}"
export METADATA_HOME_DIR

# make sure the process is not running
if [ -f $METADATA_PID_FILE ]; then
  if kill -0 `cat $METADATA_PID_FILE` > /dev/null 2>&1; then
    echo metadata running as process `cat $METADATA_PID_FILE`.  Stop it first.
    exit 1
  fi
fi

mkdir -p $METADATA_LOG_DIR

pushd ${BASEDIR} > /dev/null

JAVA_PROPERTIES="$METADATA_OPTS $METADATA_PROPERTIES -Dmetadata.log.dir=$METADATA_LOG_DIR -Dmetadata.home=${METADATA_HOME_DIR}"
shift

while [[ ${1} =~ ^\-D ]]; do
  JAVA_PROPERTIES="${JAVA_PROPERTIES} ${1}"
  shift
done
TIME=`date +%Y%m%d%H%M%s`

${JAVA_BIN} ${JAVA_PROPERTIES} -cp ${METADATACPPATH} org.apache.hadoop.metadata.CredentialProviderUtility

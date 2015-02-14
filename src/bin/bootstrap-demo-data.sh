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

METADATACPPATH="${METADATACPPATH}:${METADATA_EXPANDED_WEBAPP_DIR}/metadata/WEB-INF/classes"
METADATACPPATH="${METADATACPPATH}:${METADATA_EXPANDED_WEBAPP_DIR}/metadata/WEB-INF/lib/*:${BASEDIR}/libext/*"

# log and pid dirs for applications
METADATA_LOG_DIR="${METADATA_LOG_DIR:-$BASEDIR/logs}"
export METADATA_LOG_DIR

METADATA_HOME_DIR="${METADATA_HOME_DIR:-$BASEDIR}"
export METADATA_HOME_DIR

JAVA_PROPERTIES="$METADATA_OPTS $METADATA_PROPERTIES -Dmetadata.log.dir=$METADATA_LOG_DIR -Dmetadata.home=${METADATA_HOME_DIR}"

${JAVA_BIN} ${JAVA_PROPERTIES} -cp ${METADATACPPATH} org.apache.hadoop.metadata.DemoDataDriver

echo Test data added to Metadata Server!!!

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
#  See the License for the specific language governing limitations under
#  the License. See accompanying LICENSE file.
#

PRG="${0}"

[[ `uname -s` == *"CYGWIN"* ]] && CYGWIN=true

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

allargs=$@

if test -z "${JAVA_HOME}"
then
    JAVA_BIN=`which java`
    JAR_BIN=`which jar`
else
    JAVA_BIN="${JAVA_HOME}/bin/java"
    JAR_BIN="${JAVA_HOME}/bin/jar"
fi
export JAVA_BIN

if [ ! -e "${JAVA_BIN}" ] || [ ! -e "${JAR_BIN}" ]; then
  echo "$JAVA_BIN and/or $JAR_BIN not found on the system. Please make sure java and jar commands are available."
  exit 1
fi

for i in "${BASEDIR}/hook/nutch/atlas-nutch-plugin-impl/"*.jar; do
  ATLASCPPATH="${ATLASCPPATH}:$i"
done

if [ -z "${ATLAS_CONF_DIR}" ] && [ -e /etc/atlas/conf ];then
    ATLAS_CONF_DIR=/etc/atlas/conf
fi
ATLASCPPATH=${ATLASCPPATH}:${ATLAS_CONF_DIR}

if [ -d "${ATLAS_CONF_DIR}" ] && [ -f "${ATLAS_CONF_DIR}/atlas-env.sh" ]; then
  source ${ATLAS_CONF_DIR}/atlas-env.sh
fi

ATLAS_LOG_DIR="${ATLAS_LOG_DIR:-/var/log/atlas}"
export ATLAS_LOG_DIR
LOGFILE="$ATLAS_LOG_DIR/import-nutch.log"

CP="${ATLASCPPATH}"

if [ "${CYGWIN}" == "true" ]
then
   ATLAS_LOG_DIR=`cygpath -w ${ATLAS_LOG_DIR}`
   LOGFILE=`cygpath -w ${LOGFILE}`
   CP=`cygpath -w -p ${CP}`
fi

JAVA_PROPERTIES="$ATLAS_OPTS -Datlas.log.dir=$ATLAS_LOG_DIR -Datlas.log.file=import-nutch.log -Dlogback.configurationFile=atlas-nutch-import-logback.xml"

echo "Log file for import is $LOGFILE"

"${JAVA_BIN}" ${JAVA_PROPERTIES} -cp "${CP}" org.apache.atlas.nutch.bridge.NutchBridge $allargs

RETVAL=$?
[ $RETVAL -eq 0 ] && echo Nutch Data Model imported successfully!!!
[ $RETVAL -ne 0 ] && echo Failed to import Nutch Data Model!!!

exit $RETVAL

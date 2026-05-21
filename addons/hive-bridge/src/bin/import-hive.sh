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

# Construct Atlas classpath using jars from hook/hive/atlas-hive-plugin-impl/ directory.
for i in "${BASEDIR}/hook/hive/atlas-hive-plugin-impl/"*.jar; do
  ATLASCPPATH="${ATLASCPPATH}:$i"
done

if [ -z "${ATLAS_CONF_DIR}" ] && [ -e /etc/atlas/conf ];then
    ATLAS_CONF_DIR=/etc/atlas/conf
fi
ATLASCPPATH=${ATLASCPPATH}:${ATLAS_CONF_DIR}

if [ -d "${ATLAS_CONF_DIR}" ] && [ -f "${ATLAS_CONF_DIR}/atlas-env.sh" ]; then
  #source the atlas-env.sh file, use the exported argument into atlas-jvm
  source ${ATLAS_CONF_DIR}/atlas-env.sh
fi

# log dir for applications
ATLAS_LOG_DIR="${ATLAS_LOG_DIR:-/var/log/atlas}"
export ATLAS_LOG_DIR
LOGFILE="$ATLAS_LOG_DIR/import-hive.log"

TIME=`date +%Y%m%d%H%M%s`

#Add hive conf in classpath
if [ ! -z "$HIVE_CONF_DIR" ]; then
    HIVE_CONF=$HIVE_CONF_DIR
elif [ ! -z "$HIVE_HOME" ]; then
    HIVE_CONF="$HIVE_HOME/conf"
elif [ -e /etc/hive/conf ]; then
    HIVE_CONF="/etc/hive/conf"
else
    echo "Could not find a valid HIVE configuration"
    exit 1
fi

echo Using Hive configuration directory ["$HIVE_CONF"]


if [ -f "${HIVE_CONF}/hive-env.sh" ]; then
  . "${HIVE_CONF}/hive-env.sh"
fi

if [ -z "$HIVE_HOME" ]; then
    if [ -d "${BASEDIR}/../hive" ]; then
        HIVE_HOME=${BASEDIR}/../hive
    else
        echo "Please set HIVE_HOME to the root of Hive installation"
        exit 1
    fi
fi

HIVE_CP="${HIVE_CONF}"
# Multiple jars in HIVE_CP_EXCLUDE_LIST can be added using "\|" separator
# Ex: HIVE_CP_EXCLUDE_LIST="jersey-multipart"
# exclude log4j libs from hive classpath to avoid conflict
HIVE_CP_EXCLUDE_LIST="javax.ws.rs-api\|log4j-slf4j-impl\|log4j-1.2-api\|log4j-api\|log4j-core\|log4j-web\|jsr311-api"

for i in $(find "${HIVE_HOME}/lib/" -name  "*.jar" | grep -v "$HIVE_CP_EXCLUDE_LIST"); do
    HIVE_CP="${HIVE_CP}:$i"
done

#Add hadoop conf in classpath
if [ ! -z "$HADOOP_CLASSPATH" ]; then
    HADOOP_CP=$HADOOP_CLASSPATH
elif [ ! -z "$HADOOP_HOME" ]; then
    HADOOP_CP=`$HADOOP_HOME/bin/hadoop classpath`
elif [ $(command -v hadoop) ]; then
    HADOOP_CP=`hadoop classpath`
    echo $HADOOP_CP
else
    echo "Environment variable HADOOP_CLASSPATH or HADOOP_HOME need to be set"
    exit 1
fi

#Exclude libs from Hadoop classpath which are conflicting with Atlas
HADOOP_CP_EXCLUDE_LIST=("jersey-core-1.19.jar" "jersey-json-1.19.jar" "jersey-server-1.19.jar" "jersey-servlet-1.19.jar" "jersey-client-1.19.jar" "jsr311-api-1.1.1.jar")

# Construct the classpath excluding the specified JARs
ORIGINAL_HADOOP_CLASSPATH=$(echo "$HADOOP_CP" | tr ':' '\n')
for jar in $ORIGINAL_HADOOP_CLASSPATH
do
    excluded=false
    for exclude_jar in "${HADOOP_CP_EXCLUDE_LIST[@]}"
    do
        if [[ "$jar" == *"$exclude_jar" ]]; then
            excluded=true
            break
        fi
    done

    if [ "$excluded" = false ]; then
        HADOOP_CLASSPATH_NEW="$HADOOP_CLASSPATH_NEW:$jar"
    fi
done

# Remove leading colon
HADOOP_CP=$(echo "$HADOOP_CLASSPATH_NEW" | sed 's/^://')

CP="${HIVE_CP}:${HADOOP_CP}:${ATLASCPPATH}"

# If running in cygwin, convert pathnames and classpath to Windows format.
if [ "${CYGWIN}" == "true" ]
then
   ATLAS_LOG_DIR=`cygpath -w ${ATLAS_LOG_DIR}`
   LOGFILE=`cygpath -w ${LOGFILE}`
   HIVE_CP=`cygpath -w ${HIVE_CP}`
   HADOOP_CP=`cygpath -w ${HADOOP_CP}`
   CP=`cygpath -w -p ${CP}`
fi

JAVA_PROPERTIES="$ATLAS_OPTS -Datlas.log.dir=$ATLAS_LOG_DIR -Datlas.log.file=import-hive.log
-Dlogback.configurationFile=atlas-hive-import-logback.xml"

IMPORT_ARGS=
JVM_ARGS=

while true
do
  option=$1
  shift

  case "$option" in
    -d) IMPORT_ARGS="$IMPORT_ARGS -d $1"; shift;;
    -t) IMPORT_ARGS="$IMPORT_ARGS -t $1"; shift;;
    -f) IMPORT_ARGS="$IMPORT_ARGS -f $1"; shift;;
    -o) IMPORT_ARGS="$IMPORT_ARGS -o $1"; shift;;
    -i) IMPORT_ARGS="$IMPORT_ARGS -i";;
    -h) export HELP_OPTION="true"; IMPORT_ARGS="$IMPORT_ARGS -h";;
    --database) IMPORT_ARGS="$IMPORT_ARGS --database $1"; shift;;
    --table) IMPORT_ARGS="$IMPORT_ARGS --table $1"; shift;;
    --filename) IMPORT_ARGS="$IMPORT_ARGS --filename $1"; shift;;
    --output) IMPORT_ARGS="$IMPORT_ARGS --output $1"; shift;;
    --ignoreBulkImport) IMPORT_ARGS="$IMPORT_ARGS --ignoreBulkImport";;
    --help) export HELP_OPTION="true"; IMPORT_ARGS="$IMPORT_ARGS --help";;
    -deleteNonExisting) IMPORT_ARGS="$IMPORT_ARGS -deleteNonExisting";;
    "") break;;
    *) IMPORT_ARGS="$IMPORT_ARGS $option"
  esac
done

JAVA_PROPERTIES="${JAVA_PROPERTIES} ${JVM_ARGS}"

if [ -z ${HELP_OPTION} ]; then
  echo "Log file for import is $LOGFILE"
fi

"${JAVA_BIN}" ${JAVA_PROPERTIES} -cp "${CP}" org.apache.atlas.hive.bridge.HiveMetaStoreBridge $IMPORT_ARGS

RETVAL=$?
if [ -z ${HELP_OPTION} ]; then
  [ $RETVAL -eq 0 ] && echo Hive Meta Data imported successfully!
  [ $RETVAL -eq 1 ] && echo Failed to import Hive Meta Data! Check logs at: $LOGFILE for details.
fi

exit $RETVAL


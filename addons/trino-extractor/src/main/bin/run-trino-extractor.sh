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
for i in "${BASEDIR}/lib/"*.jar; do
  ATLASCPPATH="${ATLASCPPATH}:$i"
done

if [ -z "${ATLAS_CONF_DIR}" ] && [ -e "${BASEDIR}/conf/" ];then
    ATLAS_CONF_DIR="${BASEDIR}/conf/"
fi
ATLASCPPATH=${ATLASCPPATH}:${ATLAS_CONF_DIR}

# log dir for applications
ATLAS_LOG_DIR="${BASEDIR}/log"
export ATLAS_LOG_DIR
LOGFILE="$ATLAS_LOG_DIR/atlas-trino-extractor.log"

TIME=`date +%Y%m%d%H%M%s`

CP="${ATLASCPPATH}"

# If running in cygwin, convert pathnames and classpath to Windows format.
if [ "${CYGWIN}" == "true" ]
then
   ATLAS_LOG_DIR=`cygpath -w ${ATLAS_LOG_DIR}`
   LOGFILE=`cygpath -w ${LOGFILE}`
   HIVE_CP=`cygpath -w ${HIVE_CP}`
   HADOOP_CP=`cygpath -w ${HADOOP_CP}`
   CP=`cygpath -w -p ${CP}`
fi

JAVA_PROPERTIES="$ATLAS_OPTS -Datlas.log.dir=$ATLAS_LOG_DIR -Datlas.log.file=atlas-trino-extractor.log
-Dlog4j.configuration=atlas-log4j.xml -Djdk.httpclient.HttpClient.log=requests -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006"

IMPORT_ARGS=()
JVM_ARGS=

set -f
while true
do
  option=${1}
  shift

  case "${option}" in
    -c) IMPORT_ARGS+=("-c" "$1"); shift;;
    -s) IMPORT_ARGS+=("-s" "$1"); shift;;
    -t) IMPORT_ARGS+=("-t" "$1"); shift;;
    -cx)
        CRON_EXPR="$1"
        shift
        while [[ "$1" != "" && "$1" != -* ]]; do
          CRON_EXPR="$CRON_EXPR $1"
          shift
        done
	      IMPORT_ARGS+=("-cx" "$CRON_EXPR");;
    -h) export HELP_OPTION="true"; IMPORT_ARGS+=("-h");;
    --catalog) IMPORT_ARGS+=("--catalog" "$1"); shift;;
    --table) IMPORT_ARGS+=("--table" "$1"); shift;;
    --schema) IMPORT_ARGS+=("--schema" "$1"); shift;;
    --cronExpression)
          CRON_EXPR="$1"
          shift
          while [[ "$1" != "" && "$1" != -* ]]; do
            CRON_EXPR="$CRON_EXPR $1"
            shift
          done
  	      IMPORT_ARGS+=("--cronExpression" "$CRON_EXPR");;
    --help) export HELP_OPTION="true"; IMPORT_ARGS+=("--help");;
    -*)
	    echo "Invalid argument found"
	    export HELP_OPTION="true"; IMPORT_ARGS+=("--help")
	    break;;
    "") break;;
  esac
done

JAVA_PROPERTIES="${JAVA_PROPERTIES} ${JVM_ARGS}"

if [ -z ${HELP_OPTION} ]; then
  echo "Log file for import is $LOGFILE"
fi

"${JAVA_BIN}" ${JAVA_PROPERTIES} -cp "${CP}" org.apache.atlas.trino.cli.TrinoExtractor "${IMPORT_ARGS[@]}"

set +f

RETVAL=$?
if [ -z ${HELP_OPTION} ]; then
  [ $RETVAL -eq 0 ] && echo Trino Meta Data imported successfully!
  [ $RETVAL -eq 1 ] && echo Failed to import Trino Meta Data! Check logs at: $LOGFILE for details.
fi

exit $RETVAL

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

# You should not have to change anything below this line ;-)
###############################################################

#############################################
## Check that java and python are available


if [ "x$JAVA_HOME" == "x" ]; then
  echo "JAVA_HOME is not set, using standard java on path"
  JAVAEXE=$(which java)
else
  echo "JAVA_HOME is set to $JAVA_HOME"
  JAVAEXE=$JAVA_HOME/bin/java
fi

if [ ! -x $JAVAEXE ]; then
   echo "Java executable $JAVAEXE could not be found. Set JAVA_HOME accordingly or make sure that java is in your path".
   exit 1
fi

echo "Using java: $JAVAEXE"


PYTHON27EXE=python
PYTHONVERSION=`$PYTHON27EXE --version 2>&1`
if [[ ! $PYTHONVERSION == *2.7.* ]]; then
   echo "Warning: Python command is not version 2.7. Starting / stopping Atlas might not work properly"
fi


###############################################
## Set some variables

BASEDIR="$( cd "$(dirname "$0")" ; pwd -P )"
FULLHOSTNAME=`hostname -f`

ATLAS_HOME=$BASEDIR/apache-atlas-0.7-incubating-release
ATLAS_PORT=21453
ATLAS_URL=https://localhost:$ATLAS_PORT
ATLAS_USER=admin
ATLAS_PASSWORD=UR0+HOiApXG9B8SNpKN5ww==

ZK_DATADIR=/tmp/odftestenv-zookeeper
KAFKA_DATADIR=/tmp/odftestenv-kafka-logs

# export KAFKA_OPTS so that is picked up by the kafka and zookeeper start scripts. This can be used as a marker to search for those processes
KILLMARKER=thisisanodftestenvprocess
export KAFKA_OPTS="-D$KILLMARKER=true"
KAFKA_HOME=$BASEDIR/kafka_2.11-0.10.0.0
SPARK_HOME=$BASEDIR/spark-2.1.0-bin-hadoop2.7

JETTY_BASE=$BASEDIR/odfjettybase
JETTY_HOME=$BASEDIR/jetty-distribution-9.2.10.v20150310

##########################################
## Copy required files

if [ "$(uname)" == "Darwin" ]; then
	cp $ATLAS_HOME/conf/atlas-application.properties_mac $ATLAS_HOME/conf/atlas-application.properties
else
	cp $ATLAS_HOME/conf/atlas-application.properties_linux $ATLAS_HOME/conf/atlas-application.properties
fi

##########################################
## Functions

function waitSeconds {
   echo "     Waiting for $1 seconds..."
   sleep $1
}

function cleanMetadata {
	echo Removing Atlas data...
	rm -rf $ATLAS_HOME/data
	rm -rf $ATLAS_HOME/logs
	echo Atlas data removed
}

function cleanConfig {
	echo Removing Zookeeper and Kafka data...
	rm -rf $KAFKA_DATADIR
    rm -rf $ZK_DATADIR
	echo Zookeeper and Kafka data removed.
}

function reconfigureODF {
	echo Configuring ODF...
    JSON='{ "sparkConfig": { "clusterMasterUrl": "'$SPARK_MASTER'" } }'
    echo Updating config to $JSON
    curl -H "Content-Type: application/json" -X PUT -d "$JSON" -k -u sdp:admin4sdp https://$FULLHOSTNAME:58081/odf-web-1.2.0-SNAPSHOT/odf/api/v1/settings
    echo ODF configured.
}

function healthCheck {
    echo Running ODF health check
    curl -X GET -k -u sdp:admin4sdp https://$FULLHOSTNAME:58081/odf-web-1.2.0-SNAPSHOT/odf/api/v1/engine/health
    echo Health check finished
}

function startTestEnv {
   echo Starting ODF test env
   if [ -f "$ZKDATADIR" ]; then
      echo zookeeper data exists
   fi

   echo "Starting Zookeeper"
   nohup $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &> $BASEDIR/nohupzookeeper.out &
   waitSeconds 5
   echo "Starting Kafka"
   nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &> $BASEDIR/nohupkafka.out &
   waitSeconds 5
   if [[ $(unzip -v $JETTY_BASE/webapps/odf-web-1.2.0-SNAPSHOT.war | grep odf-atlas-) ]]; then
     echo "Starting Atlas"
     nohup $PYTHON27EXE $ATLAS_HOME/bin/atlas_start.py -port $ATLAS_PORT &> $BASEDIR/nohupatlas.out &
     waitSeconds 30
   else
       echo "Do not start Atlas because ODF was built without it."
   fi
   echo "Starting Spark master"
   cd $SPARK_HOME
   nohup sbin/start-master.sh &> $BASEDIR/nohupspark.out &
   waitSeconds 5
   SPARK_MASTER=$(curl http://localhost:8080 | awk '/ Spark Master at/{print $NF}')
   echo "Spark master URL: $SPARK_MASTER"
   echo "Starting Spark slave"
   nohup sbin/start-slave.sh $SPARK_MASTER &> $BASEDIR/nohupspark.out &
   waitSeconds 5
   echo "Starting ODF on Jetty"
   cd $JETTY_BASE
   nohup $JAVAEXE -Dodf.zookeeper.connect=localhost:52181 -Datlas.url=$ATLAS_URL -Datlas.user=$ATLAS_USER -Datlas.password=$ATLAS_PASSWORD -Dorg.eclipse.jetty.servlet.LEVEL=ALL -jar $JETTY_HOME/start.jar STOP.PORT=53000 STOP.KEY=STOP &> $BASEDIR/nohupjetty.out &
   waitSeconds 10

   healthCheck
   reconfigureODF

   echo "ODF test env started on https://$FULLHOSTNAME:58081/odf-web-1.2.0-SNAPSHOT"
}

function stopTestEnv {
   echo Stopping ODF test env ...
   echo Stopping kafka and zookeeper...
   PROCESSNUM=`ps aux | grep $KILLMARKER | grep -v grep | wc | awk '{print $1}'`
   if [ $PROCESSNUM -gt 0 ]; then
      echo Killing $PROCESSNUM Kafka / ZK processes
      kill -9 $(ps aux | grep $KILLMARKER | grep -v grep | awk '{print $2}')
   else
      echo No Kafka / Zookeeper processes found
   fi
   waitSeconds 3
   echo Kafka and Zookeeper stopped
   echo Stopping Atlas...
   $PYTHON27EXE $ATLAS_HOME/bin/atlas_stop.py
   waitSeconds 5
   echo Atlas stopped
   echo Stopping Spark...
   cd $SPARK_HOME
   SPARK_MASTER=$(curl http://localhost:8080 | awk '/ Spark Master at/{print $NF}')
   sbin/stop-slave.sh $SPARK_MASTER
   sbin/stop-master.sh
   waitSeconds 5
   echo Spark stopped
   echo Stopping Jetty...
   cd $JETTY_BASE
   $JAVAEXE -jar $JETTY_HOME/start.jar STOP.PORT=53000 STOP.KEY=STOP --stop
   waitSeconds 5
   echo Jetty stopped
   echo ODF test env stopped
}


function usageAndExit {
  echo "Usage: $0 start|stop|cleanconfig|cleanmetadata|cleanall"
  echo "Manage the ODF test environment"
  echo "Options:"
  echo "         start         (re)start"
  echo "         stop          stop"
  echo "         cleanall      (re)starts with clean configuration and clean metadata"
  echo "         cleanconfig   (re)starts with clean configuration"
  echo "         cleanmetadata (re)starts with clean metadata"
  exit 1;
}

###############################################
## main script

if [ -z "$1" ]; then
   usageAndExit
elif [ "$1" = "start" ]; then
   echo "(Re) starting test env..."
   stopTestEnv
   echo "-------------------------------------"
   startTestEnv
   echo "Test env restarted"
elif [ "$1" = "stop" ]; then
   stopTestEnv
elif [ "$1" = "cleanconfig" ]; then
   echo "(Re) starting test env with clean configuration..."
   stopTestEnv
   cleanConfig
   startTestEnv
   echo "(Re)started test env with clean configuration"
elif [ "$1" = "cleanmetadata" ]; then
   echo "(Re) starting test env with clean metadata..."
   stopTestEnv
   cleanMetadata
   startTestEnv
   echo "(Re)started test env with clean metadata"
elif [ "$1" = "cleanall" ]; then
   echo "(Re) starting test env with clean configuration and metadata..."
   stopTestEnv
   cleanConfig
   cleanMetadata
   startTestEnv
   echo "(Re)started test env with clean configuration and metadata"
else
   usageAndExit
fi

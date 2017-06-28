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
export JAVAEXE=java
export PYTHON27EXE=python

# You should not have to change anything below this line ;-)
export BASEDIR="$( cd "$(dirname "$0")" ; pwd -P )"

export JETTY_HOME=$BASEDIR/jetty-distribution-9.2.10.v20150310
export KAFKA_PACKAGE_DIR=$BASEDIR/kafka_2.11-0.10.0.0
export ATLAS_HOME=$BASEDIR/apache-atlas-0.7-incubating-release

echo Delete logs
rm -rf /tmp/odftestenv-kafka-logs
rm -rf /tmp/odftestenv-zookeeper

echo Copy required files
if [ "$(uname)" == "Darwin" ]; then
	cp $ATLAS_HOME/conf/atlas-application.properties_mac $ATLAS_HOME/conf/atlas-application.properties
else
	cp $ATLAS_HOME/conf/atlas-application.properties_linux $ATLAS_HOME/conf/atlas-application.properties
fi

echo Start zookeeper:
$KAFKA_PACKAGE_DIR/bin/zookeeper-server-start.sh $KAFKA_PACKAGE_DIR/config/zookeeper.properties &

sleep 5

echo Start kafka:
$KAFKA_PACKAGE_DIR/bin/kafka-server-start.sh $KAFKA_PACKAGE_DIR/config/server.properties &

sleep 5

echo Stop and restart Atlas
$PYTHON27EXE $ATLAS_HOME/bin/atlas_stop.py
$PYTHON27EXE $ATLAS_HOME/bin/atlas_start.py -port 21443

echo Start jetty
export JETTY_BASE=$BASEDIR/odfjettybase
cd $JETTY_BASE
$JAVAEXE -Dodf.zookeeper.connect=localhost:52181 -Dorg.eclipse.jetty.servlet.LEVEL=ALL -jar $JETTY_HOME/start.jar &

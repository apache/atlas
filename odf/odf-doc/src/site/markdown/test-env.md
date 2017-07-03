<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# Test environment

The odf-test-env archive contains a simple test environment for ODF.
It contains all components to run a simple ODF installation, namely

- Apache Kafka
- Apache Atlas
- Jetty (to host the ODF web app)
- Apache Spark

The test environment is available on Linux and Windows.

## Before you start

Make sure that

- The python executable of Python 2.7 is in your path
- The environment variable JAVA_HOME is set and points to a proper JDK (not just a JRE!)


## *Fast path*: Download and install ODF test environment

If you are running on Linux you can download and install the latest ODF test environment by
downloading the script `download-install-odf-testenv.sh` from
<a href="https://shared-discovery-platform-jenkins.swg-devops.com:8443/view/1-ODF/job/Open-Discovery-Framework/lastSuccessfulBuild/artifact/odf-test-env/src/main/scripts/download-install-odf-testenv.sh">
here</a>.

If you call the script with no parameters, it will download, install and start the latest version of the test env.
The default unpack directory is `~/odf-test-env`.

## Download the test environment manually

You can get the latest version of the test environment from the Jenkins
<a href="https://shared-discovery-platform-jenkins.swg-devops.com:8443/view/1-ODF/job/Open-Discovery-Framework/lastSuccessfulBuild/artifact/odf-test-env/target/odf-test-env-0.1.0-SNAPSHOT-bin.zip">
here</a>.

## Running the test environment

To start the test environment on Linux, run the script ``odftestenv.sh start`` . The script will start four background processes (Zookeeper, Kafka, Atlas, Jetty). To stop the test env, use the script ``odftestenv.sh stop``.

To start the test environment on Windows, run the script ``start-odf-testenv.bat``.
This will open four command windows (Zookeeper, Kafka, Atlas, Jetty) with respective window titles. To stop the test environment close all these windows. Note that the `HADOOP_HOME` environment variable needs to be set on Windows as described in the [build documentation](build.md).


Once the servers are up and running you will reach the ODF console at
[https://localhost:58081/odf-web-0.1.0-SNAPSHOT](https://localhost:58081/odf-web-0.1.0-SNAPSHOT).

*Note*: The test environment scripts clean the Zookeeper and Kafka data before it starts.
This means in particular that the configuration will be reset every time you restart it!

Have fun!

## Restart / cleanup

On Linux, the `odftestenv.sh` script has these additional options

- `cleanconfig`: Restart the test env with a clean configuration and clean Kafka topics
- `cleanmetadata`: Restart with empty metadata
- `cleanall`: Both `cleanconfig`and `cleanmetadata`.


## Additional Information

### Deploying a new version of the ODF war
Once started you can hot-deploy a new version of the ODF war file simply by copying it
to the ``odfjettybase/webapps`` folder even while the test environment's Jetty instance is running.
Note that it may take a couple of seconds before the new app is available.

If you have the ODF build set up you may want to use the ``deploy-odf-war.bat/.sh`` for this.
You must edit the environment variable ``ODF_GIT_DIR`` in this script first to point to your local build directory.

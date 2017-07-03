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

# Troubleshooting

## ODF

### Debugging using eclipse

You can run Jetty inside Eclipse using the “Eclipse Jetty Feature” (Eclipse -> Help -> Install New Software…).
Then, create a new debug configuration (Run -> Debug Configurations…). Specify

WebApp Tab
Project: odf-web
WebApp Folder: ../../../../../odf-web/src/main/webapp
Context Path: /odf-web-0.1.0-SNAPSHOT
HTTP / HTTPs Port: 58081

Arguments Tab
VM Arguments: -Dodf.zookeeper.connect=localhost:52181

As the Eclipse Jetty plugin does not support secure connections nor basic authentication, remove the `<security-constraint>`
and `<login-config>`
sections from the web.xml.
The URL of the ODF Webapp the needs to be prefixed with http:// rather than https://.

Then start Atlas and Kafka via the test-env (just comment out the line that starts jetty or stop it after being started).
Now you can use the debug configuration in eclipse to start ODF.

See also (https://ibm-analytics.slack.com/archives/shared-discovery-pltf/p1467365155000009)


### Logs and trace
ODF uses ``java.util.logging`` APIs so if your runtime environment does support
direct setting, use the respective mechanism.

For runtimes that don't support this out-of-the-box (like Jetty) you can set the JVM system property
``odf.logspec`` with a value like ``<Level>,<Path>`` which advises ODF to
write the log with logging level ``<Level>`` to the file under ``<Path>``.

Example:

	-Dodf.logspec=ALL,/tmp/myodflogfile.log

Availabel log levels are the ones for java.util.logging, namely SEVERE, WARNING, INFO, FINE, FINER, FINEST,
and ALL.


## Atlas

### Logs

The logs directory contains a bunch of logfiles, together with a file called ``atlas.pid`` which
contains the process ID of the Atlas server that is currently running.
In case of issues the file ``logs/application.log`` should be checked first.

### Restarting Atlas

Run these commands (from the atlas installation directory) to restart Atlas

	bin/atlas_stop.py
	bin/atlas_start.py

### Clean all data

To clean the Atlas repository, simply remove the directories ``data`` and ``logs`` before starting.


### Issues

#### Service unavailable (Error 503)

Sometimes, calling any Atlas REST API (and the UI) doesn't work and an HTTP error 503 is returned.
We see this error occasionally and don't know any way to fix it except cleaning all data and restarting Atlas


### Creating Atlas object take a long time

It takes a long time to create an Atlas object and after about a minute you see a message like this in the log

	Unable to update metadata after 60000ms

This is the result of the kafka queues (which are used for notifications) being in error.
To fix this restart Atlas (no data cleaning required).

## Kafka / Zookeeper

If there is a problem starting Kafka / Zookeeper check if there might be a port conflict due to other instances of Kafka / Zookeeper using the default port.
This might be the case if a more recent version of the IS suite is installed on the system on which you want to run ODF.

Example: If another instance of Zookeeper uses the default port 52181 you need to switch the Zookeeper port used by replacing 52181 with a free port number in:
- start-odf-testenv.bat
- kafka_2.10-0.8.2.1\config\zookeeper.properties
- kafka_2.10-0.8.2.1\config\server.properties

### Reset

To reset your Zookeeper / Kafka installation, you will first have to stop the servers:

	bin/kafka-server-stop
	bin/zookeeper-server-stop

Next remove the zookeeper data directory and the Kafka logs directory. Note that "logs"
in Kafka mean the actual data in the topics not the logfiles.
You can find which directories to clean in the the properties ``dataDir`` in the ``zookeeper.properties``
file and ``log.dirs`` in ``server.properties`` respectively.
The defaults are ``/tmp/zookeeper`` and ``/tmp/kafka-logs``.

Restart the servers with

	bin/zookeeper-server-start config/zookeeper.properties
	bin/kafka-server-start config/server.properties

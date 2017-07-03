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

# Install ODF manually

This section describes how to manually install ODF and its prerequisites.

## Install ODF locally

ODF is installed on an application server like jetty. Its prereqs (Kafka and Atlas)
can run on separate machines, they simple must be reachable over the network.

ODF's configuration is stored in Zookeeper which is a prereq for Kafka.


### Prerequisites

ODF has two prerequisites:

1. Apache Atlas (only tested with 0.6 but no hard dependency required here)
2. Apache Kafka 0.8.2.1 which, in turn, requires Zookeper 3.4

#### Apache Atlas

[Apache Atlas](http://atlas.incubator.apache.org/) is an open
metadata infrastructure that is currently in incubator status.
There is currently no binary download available, you have to [build it yourself](http://atlas.incubator.apache.org/InstallationSteps.html).
Alternatively, you can download a version that we built [here](https://ibm.box.com/shared/static/of1tdea7465iaen8ywt7l1h761j0fplt.zip).

After you built the distribution, simply unpack the tar ball, and run
``bin/atlas_start.py``. Atlas will be started on port 21443 as default, so point
your browser to [https://localhost:21443](https://localhost:21443) to look at the
Atlas UI.
Note that starting Atlas can take up to a minute.

To stop, run ``bin/atlas_stop.py``.

See the Atlas section in the [Troubleshooting guide](troubleshooting.html)
for common issues and how to workaround them.

#### Apache Kafka

[Apache Kafka](http://kafka.apache.org/) is an open source project that implements
a messaging infrastructure. ODF uses Kafka for notifications and queueing up requests to
discovery services.

To install Kafka, download the version 0.8.2.1 with Scala 2.10 (which is the version we used in
our tests) from the Kafka website, see [here](https://www.apache.org/dyn/closer.cgi?path=/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz).

After unpacking the tar ball these steps should get you going:

1. CD to the distribution directory.
2. Start zookeeper first by running ``bin/zookeeper-server-start.sh config/zookeeper.properties``.
3. Start Kafka: ``bin/kafka-server-start.sh config/server.properties``

By default, Zookeeper is running on port 2181 and Kafka runs on port 9092. You can change the Zookeeper
port by changing the properties ``clientPort`` in ``config/zookeeper.properties`` and
``zookeeper.connect`` in ``config/server.properties``. Change the Kafka port by changing
``port`` in ``config/server.properties``.

For Windows, run the respective .bat commands in the ``bin\windows`` directory.


### Deploy ODF

The only ODF artifact you need for deployment is the war file built by the odf-web maven project, which can typically
be found here:

	odf-web/target/odf-web-0.1.0-SNAPSHOT.war

To tell ODF which Zookeeper / Kafka to use you will need to set the
Java system property ``odf.zookeeper.connect`` to point
to the Zookeeper host and port. The value is typically the same string as the ``zookeeper.connect`` property
in the Kafka installation ``config/server.properties`` file:

	-Dodf.zookeeper.connect=zkserver.example.org:2181

Note that if this property is not set, the default is ``localhost:52181``.


#### Application Server

ODF should run on any application server. As of now we have done most of our testing on Jetty.

##### Jetty

[Jetty](https://eclipse.org/jetty/) is an open source web and application server.
We have used version 9.2.x for our testing (the most current one that supports Java 7).
Download it from the web site [https://eclipse.org/jetty/](https://eclipse.org/jetty/).

Here are some quick start instructions for creating a new Jetty base. Compare
the respective Jetty documentation section [here](http://www.eclipse.org/jetty/documentation/9.2.10.v20150310/quickstart-running-jetty.html#creating-jetty-base).

First, for in order to enable basic authentication, the following configuration needs to be added to the `etc/jetty.xml` file, right before the closing `</Configure>` tag at the end of the file:

```
<Call name="addBean">
	<Arg>
		<New class="org.eclipse.jetty.security.HashLoginService">
			<Set name="name">ODF Realm</Set>
			<Set name="config"><SystemProperty name="jetty.home" default="."/>/etc/realm.properties</Set>
		</New>
	</Arg>
</Call>
```

Secondly, a `etc/realm.properties` file needs to be added that contains the credentials of the ODF users in the following [format](http://www.eclipse.org/jetty/documentation/9.2.10.v20150310/configuring-security-authentication.html#security-realms):

```
<username>: <password>[,<rolename> ...]
```

Then, you will have to create and initialize new directory where you deploy your web apps and
copy the ODF war there. These commands should do the trick:

	mkdir myjettybase
	cd myjettybase
	java -jar $JETTY_HOME\start.jar --add-to-startd=https,ssl,deploy
	cp $ODFDIR/odf-web-0.1.0-SNAPSHOT.jar webapps
	java -Dodf.zookeeper.connect=zkserver.example.org:2181 -jar $JETTY_HOME\start.jar

The first java command initializes the jetty base directory by creating a directory ``start.d`` which
contains some config files (e.g. http.ini contains the port the server runs on) and the
empty ``webapps`` directory.
The copy command copies the ODF war file to the webapps folder.
The last command starts Jetty (on default port 8443). You can stop it by hitting Ctrl-C.

You should see a message like this one indicating that the app was found and started.

	2016-02-26 08:28:24.033:INFO:oejsh.ContextHandler:Scanner-0: Started o.e.j.w.WebAppContext@-545d793e{/odf-web-0.1.0-SNAPSHOT,file:/C:/temp/jetty-0.0.0.0-8443-odf-web-0.1.0-SNAPSHOT.war-_odf-web-0.1.0-SNAPSHOT-any-8485458047819836926.dir/webapp/,AVAILABLE}{myjettybase\webapps\odf-web-0.1.0-SNAPSHOT.war}

Point your browser to [https://localhost:8443/odf-web-0.1.0-SNAPSHOT](https://localhost:8443/odf-web-0.1.0-SNAPSHOT) to see the ODF console.



##### Websphere Liberty Profile

Stay tuned

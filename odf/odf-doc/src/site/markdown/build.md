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

# Build

This page describes how to build ODF.  

## Prerequisites

You need git, Maven, and Python (2.7 (not 3!)) available on the command line.
If you run these commands and you see similar output you should be all set:

	$ mvn -v
	Apache Maven 3.3.9 (bb52d8502b132ec0a5a3f4c09453c07478323dc5; 2015-11-10T17:41:47+01:00)
	...

	$ python -V
	Python 2.7.10

	$ git --version
	git version 2.7.4

### Additional Prerequisites on Windows

- For the build: The directory C:\tmp needs to exist
- For the tests and the test environment to run properly: The `HADOOP_HOME` environment variable must be set to a location where the Hadoop [winutils.exe](http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe) file is available in a the bin folder. For example, if the environment variable is set to `HADOOP_HOME=c:\hadoop`, the file needs to be available at `c:\hadoop\bin\winutils.exe`.
- In your Maven install directory go to bin and copy mvn.cmd to mvn.bat

## Building

To build, clone the repository and perform a maven build in the toplevel directory. These commands should do the trick:

	git clone https://github.com/Analytics/open-discovery-framework.git
	cd open-discovery-framework
	mvn clean install

Add the `-Dreduced-build` option to build and test only the core components and services of ODF:

	mvn clean install -Dreduced-build

## Fast build without tests or with reduced tests

To build without running tests run maven with the following options (The second one prevents the test Atlas instance from being started and stopped):

	mvn clean install -DskipTests -Duse.running.atlas

Use the `-Dreduced-tests` option to run only a reduced set of tests:

	mvn clean install -Dreduced-tests

This will skip all integration tests (i.e. all tests that involve Atlas) and also some of the long running tests. The option may be combined with the `-Dreduced-build` option introduced above.

## Building the test environment

You can build a test environment that contains Atlas and
Kafka, and Jetty by running these commands:

	cd odf-test-env
	mvn package

This will create a zip file with the standalone test environment under
``odf-test-env/target/odf-test-env-0.1.0-SNAPSHOT-bin.zip``.
See the contents of this zip file or the [documentation section on the test environment](test-env.html)
for details.

Congrats! You have just built ODF.
This should be enough to get you going. See below for additional information
on different aspects of the build.

## Additional Information

### Working with Eclipse

To build with Eclipse you must have the maven m2e plugin and EGit installed (e.g., search for "m2e maven integration for eclipse" and "egit", respectively, on the Eclipse marketplace).

- Clone the repository into some directory as above, e.g., /home/code/odf.
- Open Eclipse with a workspace in a different directory.
- Go to File -> Import -> Maven -> Existing Maven projects.
- Enter /home/code/odf as the root directory.
- Select all projects and click Finish.
- Internally, Eclipse will now perform Maven builds but you can work with the code as usual.

If you want to build via Run configurations be aware that this will not work with the embedded
maven provided by the m2e plugin. Instead you will have to do this:

- Open Windows -> Preferences -> Maven -> Installations
- Add a new installation pointing to your external Maven installation
- For each run configuration you use, select the new installation in the Maven runtime dropdown
(you might also have to set JAVA_HOME in the environment tab).

  [1]: http://iis-repo.swg.usma.ibm.com:8080/archiva/repository/all/
  [2]: https://ips-rtc.swg.usma.ibm.com/jazz/web/projects

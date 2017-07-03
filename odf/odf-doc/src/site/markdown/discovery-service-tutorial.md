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

# Tutorial: Build and run your first Discovery Service
This tutorial shows how you can create your first discovery service in Java that analyzes a data set and creates a single annotation of a new type.
This tutorial requires that you have [Maven](http://maven.apache.org/) installed.


## Create a discovery service
Follow these steps to create and package a Java implementation of the simplest discovery service.

#### Step 1: Create ODF discovery service maven project
Create a new Java Maven project from the ODF provided archetype ``odf-archetype-discoveryservice`` (group ID ``org.apache.atlas.odf``).
Choose the following values for the respective parameters:

| Parameter | Value                    |
|-----------|--------------------------|
|groupId    | odftutorials             |
|artifactId | discoveryservicetutorial |
|version    | 0.1                      |


From the command line, your command may look like this:

	mvn archetype:generate -DarchetypeGroupId=org.apache.atlas.odf -DarchetypeArtifactId=odf-archetype-discoveryservice -DarchetypeVersion=0.1.0-SNAPSHOT -DgroupId=odftutorials -DartifactId=discoveryservicetutorial -Dversion=0.1

This will create a new Maven project with a pom that has dependencies on ODF.
It will also create two Java classes ``MyDiscoveryService`` and ``MyAnnotation``
that you may want to use as a basis for the following steps.

If you use Eclipse to create your project, be sure to enable the checkbox "Include snapshot archetypes" in the
archetype selection page of the New Maven Project wizard.

If you are not interested in the actual code at this point, you may skip Steps 2 through 4 and go directly
to step 5.

#### Step 2 (optional): Check the discovery service implementation class
Create a new Java class named ``odftutorials.MyDiscoveryService`` that inherits from `org.apache.atlas.odf.core.discoveryservice.SyncDiscoveryServiceBase`.
As the interface name indicates, our service will be synchronous, i.e., it will have a simple method ``runAnalysis()`` that returns
the analysis result. For the implementation of long-running, asynchronous services, see TODO.
The archetype creation has already filled in some code here that we will use. Your class
should look something like this:

	public class MyDiscoveryService extends SyncDiscoveryServiceBase {

		@Override
		public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request) {
			// 1. create an annotation that annotates the data set object passed in the request
			MyAnnotation annotation = new MyAnnotation();
			annotation.setAnnotatedObject(request.getDataSetContainer().getDataSet().getReference());
			// set a new property called "tutorialProperty" to some string
			annotation.setMyProperty("My property was created on " + new Date());

			// 2. create a response with our annotation created above
			return createSyncResponse( //
						ResponseCode.OK, // Everything works OK
						"Everything worked", // human-readable message
						Collections.singletonList(annotation) // new annotations
			);
		}
	}

What does the code do?
The code basically consists of two parts:

1. Create a new ``MyAnnotation`` object and annotate the data set that is passed into
the discovery service with it.
2. Create the discovery service response with the new annotation and return it.


#### Step 3 (optional): Check the new annotation class
The project also contains a new Java class called ``odftutorials.MyAnnotation``
which extends the class ``org.apache.atlas.odf.core.metadata.ProfilingAnnotation``.
It is a new annotation type that contains a property called ``myProperty`` of type ``String``.
In the code you can see that there is a Java-Bean style getter and a setter method, i.e., ``getTutorialProperty()`` and
``setTutorialProperty(String value)``.

	public class MyAnnotation extends ProfilingAnnotation {

		private String myProperty;

		public String getMyProperty() {
			return myProperty;
		}

		public void setMyProperty(String myValue) {
			this.myProperty = myValue;
		}
	}

When we return these annotations, ODF will take care that these annotations are stored
appropriately in the metadata store.


#### Step 4 (optional): Check the discovery service descriptor
Lastly, the project contains a file called ``META-INF/odf/odf-services.json``
in the ``main/resources`` folder. This file which always have to have the same
name contains a JSON list of the the descriptions of all services defined in our project.
The descriptions contain an ID, a name, a short human-readable description, together
with the Java class name implementing the service. Here is how it looks like:

	[
	  {
		"id": "odftutorials.discoveryservicetutorial.MyDiscoveryService",
		"name": "My service",
		"description": "My service creates my annotation for a data set",
		"type": "Java",
		"endpoint": "odftutorials.MyDiscoveryService"
	  }
	]

Note that most of this information can be changed but ``type`` (this is a Java implementation)
and ``endpoint`` (the Java class is called ``odftutorials.MyDiscoveryService``)
should remain as the are.

#### Step 5: Build the service JAR
The service jar is a standard jar file so you can build it with a standard Maven command like

	mvn clean install

You can find the output jar as per the Maven convention in
``target/discoveryservicetutorial-0.1.jar``


## Deploy the discovery service
Once you've built your service JAR as described in the previous section, there are two ways
how you can deploy it.

### Classpath deployment
The simplest way to make an ODF instance pickup your new service is add the service JAR (and
any dependent JARs) to the ODF classpath. A simple way to do this is to package the JARs into
ODF war file. Once you (re-)start ODF, your new service should be available.

## Run the discovery service

Perform these steps to run your new service and inspect the results.

1. Go to the Analysis tab in the ODF console
2. Select the Data Sets tab and click on Start Analysis next to any data set
3. Select "My Service" as the discovery service and click Submit.
4. Select the Requests tab and click Refresh
5. You should see a new entry showing the data set and the "My Service" discovery service.
6. Click on Annotations. A new page will open that opens the Atlas UI with a list of the new
annotation that was created.
7. Click on the annotation and check the value of the "myProperty" property. It should contain
a value like ``My property was created on  Mon Feb 01 18:31:51 CET 2016``.

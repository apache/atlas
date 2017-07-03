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

# Tutorial: Creating Spark discovery services

This tutorial shows how to turn an existing [Apache Spark][1] application into an ODF discovery service of type *Spark*. The tutorial is based the Spark *summary statistics* example application provided with ODF in project `odf-spark-example-application`. It implements the Spark `describe()` method of the Spark [DataSet][2] class that calculates basic summary statistics on a Spark data frame.

## Introduction

ODF supports Spark applications implemented in Java or Scala. In order to be used as ODF discovery services, a Spark application must implement one of the following two interfaces:

* **DataFrame** - intended for Spark applications that process relational tables by using Spark data frames internally.
* **Generic** - intended for applications that need the full flexibility of ODF.

Both interfaces requires a specific method (or multiple methods) to be implemented by the Spark application that is called by ODF to run the discovery service. This method takes the current Spark context and the data set to be processed as input parameters and returns the annotations to be created. The two interface types are described in detail in separate sections below.

Spark discovery services must be packaged into a single application jar file that contains all required dependencies. Spark libraries, drivers for data access, and the required ODF jar files are implicitly provided by ODF and do not need to be packaged into the application jar file. The jar file may be renamed into *zip* by replacing its extension (not by zipping the jar file) in order to avoid possible security issues when making the file available trough tools like [box](https://box.com).

### Configure an ODF Spark cluster

ODF supports access to a local spark cluster which can be can be configured in the `sparkConfig` section of the ODF settings using the ODF REST API or the ODF web application. The parameter `clusterMasterUrl` must point to the master URL of your Spark cluster, e.g. `spark://dyn-9-152-202-64.boeblingen.de.ibm.com:7077`. An optional set of [Spark configuration options](http://spark.apache.org/docs/latest/configuration.html) can be set in the `configs` parameter by providing appropriate name value pairs. The ODF test environment comes with a ready-to-use local Spark cluser running on your local system. It can monitored on the URL `http://localhost:8080/`.

### Registering a Spark service

A Spark discovery service can be registered using the *Services* tab of the admin Web application or the `/services` endpoint of the [ODF REST API](../swagger/ext-services.html), the following parameters need to be specified to register a service. You may use the following example values to register your own instance of the *summary statistics* discovery service:

* Name of the discovery service: `Spark Summary Statistics`
* Description: `Calculates summary statistics for a given table or data file.`
* Unique service ID: `spark-summary-statistics`
* URL of application jar file (may be renamed to zip): `file:///tmp/odf-spark/odf-spark-example-application-1.2.0-SNAPSHOT.jar` (Update link to point to correct location of the file)
* Name of entry point to be called: `org.apache.atlas.odf.core.spark.SummaryStatistics`
* Service interface type: `DataFrame`

For trying out the *generic* interface, entry point `org.apache.atlas.odf.spark.SparkDiscoveryServiceExample` and service interface type `Generic` may be specified.   

### Testing the Spark service

In order to test the Spark service, you can use the *DataSets* tab of the ODF admin Web application. Click on *START ANALYSIS* right to a relational data set (data file or relational table), then select the newly registered Spark discovery service and click *SUBMIT*. You can browse the resulting annotations by searching for the name of the annotation type in the Atlas metadata repository. The example services creates two types of annotations, *SummaryStatisticsAnnotation* and *SparkTableAnnotation*. *SummaryStatisticsAnnotation* annotates data set columns with the five attributes `count`, `mean`, `stddev`, `min`, and `max`, that represent basic statistics of the data set. *SparkTableAnnotation* annotates the data set with a single attribute `count` that represents the number of columns of the data set.

### Developing Spark discovery services

When developing a new discovery service, you may use project `odf-spark-example-application` as a template. Rather than testing your service interactively using the ODF admin web application it is recommended to create a new test case in class `SparkDiscoveryServiceTest` of project `odf-core`. Two methods need to be added, one for describing the service, the other for running the actual test.

The method that describes the service basically contains the same parameters that need to be specified when adding a service through the admin webapp. The jar file must be an URL that pay point to a local file:  

	public static DiscoveryServiceRegistrationInfo getSparkSummaryStatisticsService() {
		DiscoveryServiceRegistrationInfo regInfo = new DiscoveryServiceRegistrationInfo();
		regInfo.setId("spark-summary-statistics-example-service");
		regInfo.setName("Spark summary statistics service");
		regInfo.setDescription("Example discovery service calling summary statistics Spark application");
		regInfo.setIconUrl("spark.png");
		regInfo.setLink("http://www.spark.apache.org");
		regInfo.setParallelismCount(2);
		DiscoveryServiceSparkEndpoint endpoint = new DiscoveryServiceSparkEndpoint();
		endpoint.setJar("file:/tmp/odf-spark-example-application-1.2.0-SNAPSHOT.jar");
		endpoint.setClassName("org.apache.atlas.odf.core.spark.SummaryStatistics");
		endpoint.setInputMethod(SERVICE_INTERFACE_TYPE.DataFrame);
		regInfo.setEndpoint(endpoint);
		return regInfo;
	}

The method that runs the actual test retrieves the service description from the above method and specifies what type of data set should be used for testing (data file vs. relational table) and what types of annotations are created by the discovery service. The test automatically applies the required configurations, runs the service, and checks whether new annotations of the respective types have been created. In order to speed up processing, the existing test can be temporarily commented out.  

	@Test
	public void testLocalSparkClusterWithLocalDataFile() throws Exception{
		runSparkServiceTest(
			getLocalSparkConfig(),
			DATASET_TYPE.DataFile,
			getSparkSummaryStatisticsService(),
			new String[] { "SparkSummaryStatisticsAnnotation", "SparkTableAnnotation" }
		);
	}

For compiling the test case, the `odf-core` project needs to be built:

	cd ~/git/shared-discovery-platform/odf-core
	mvn clean install -DskipTests

The test is started implicitly when building the  `odf-spark` project.

	cd ~/git/shared-discovery-platform/odf-spark
	mvn clean install

If something goes wrong, debugging information will be printed to stdout during the test. For speeding up the build and test process, option `-Duse.running.atlas` may be added to the two `mvn` commands. This way, a running Atlas instance will be used instead of starting a new instance every time.

#### Test run method example

### Troubleshooting

Before registering a Spark application in ODF as a new discovery service, it is highly recommended to test the application interactively using the `spark-submit` tool and to check whether the application implements the requested interfaces and produces the expected output format. If the execution of a Spark discovery service fails, you can browse the ODF log for additional information.

## DataFrame interface

The ODF *DataFrame* interface for Spark discovery services has a number of advantages that makes it easy to turn an existing Spark application into an ODF discovery service:

* No dependencies to the ODF code, except that a specific method needs to be implemented.
* No need to care about data access because the data set to be analyzed is provided as Spark data frame.
* Easy creation of annotations by returning "annotation data frames".   

The simplicity of the DataFrame interface leads to a number of restrictions:

* Only relational data sets can be processed, i.e. data files (OMDataFile) and relational tables (OMTable).
* Annotations may only consist of a flat list of attributes that represent simple data types, i.e. data structures and references to other data sets are not supported.  
* Annotations may only be attached to the analyzed relational data set as well as to its columns.

### Method to be implemented

In order to implement the DataFrame interface, the Spark application must implement the following method:

	public static Map<String,Dataset<Row>> processDataFrame(JavaSparkContext sc, DataFrame df, String[] args)

The parameters to be provided to the Spark application are:

* **sc**: The Spark context to be used by the Spark application for performing all Spark operations.
* **df**: The data set to be analyzed represented by a Spark data frame.
* **args**: Optional arguments for future use.

### Expected output

The result to be provided by the Spark application must be of type `Map<String,Dataset<Row>>` where `String` represents the type of the annotation to be created and `Dataset<Row>` represents the *annotation data frame* that defines the annotations to be created. If the annotation type does not yet exist, a new annotation type will be dynamically created based on the attributes of the annotation data frame.

The following example describes the format of the annotation data frame. The example uses the BankClientsShort data file provided with ODF. In contains 16 columns with numeric values that represent characteristics of bank clients:

CUST_ID | ACQUIRED | FIRST_PURCHASE_VALUE | CUST_VALUE_SCORE | DURATION_OF_ACQUIRED | CENSOR | ACQ_EXPENSE | ACQ_EXPENSE_SQ | IN_B2B_INDUSTRY | ANNUAL_REVENUE_MIL | TOTAL_EMPLOYEES | RETAIN_EXPENSE | RETAIN_EXPENSE_SQ CROSSBUY | PURCHASE_FREQ | PURCHASE_FREQ_SQ
---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---
481 | 0 | 0.0 | 0.0000 | 0 | 0 | 382.32 | 146168.58 | 0 | 56.51 | 264 | 0.00 | 0.0 | 0 | 0 | 0
482 | 1 | 249.51 | 59.248 | 730 | 1 | 586.61 | 344111.29 | 1 | 35.66 | 355 | 1508.16 | 2274546.59 | 2 | 3 | 9
483 | 0 | 0.0 | 0.0000 | 0 | 0 | 444.61 | 197678.05 | 1 | 40.42 | 452 | 0.00 | 0.0 | 0 | 0 | 0
484 | 1 | 351.41 | 77.629 | 730 | 1 | 523.10 | 273633.61 | 1 | 56.36 | 320 | 2526.72 | 6384313.96 | 3 | 12 | 144
485 | 1 | 460.04 | 76.718 | 730 | 1 | 357.78 | 128006.53 | 1 | 23.53 | 1027 | 2712.48 | 7357547.75 | 2 | 13 | 169
486 | 1 | 648.6 | 0.0000 | 701 | 0 | 719.61 | 517838.55 | 0 | 59.97 | 1731 | 1460.64 | 2133469.21 | 5 | 11 | 121
487 | 1 | 352.84 | 63.370 | 730 | 1 | 593.44 | 352171.03 | 1 | 45.08 | 379 | 1324.62 | 1754618.14 | 4 | 8 | 64
488 | 1 | 193.18 | 0.0000 | 289 | 0 | 840.30 | 706104.09 | 0 | 35.95 | 337 | 1683.83 | 2835283.47 | 6 | 12 | 144
489 | 1 | 385.14 | 0.0000 | 315 | 0 | 753.13 | 567204.80 | 0 | 58.85 | 745 | 1214.99 | 1476200.7 | 1 | 12 | 144

When applying the *Spark Summary Statistics* service to the table, two annotation data frames will be returned by the service, one for the *SparkSummaryStatistics* and one for the *SparkTableAnnotation* annotation type. The data frame returned for the *SparkSummaryStatistics* annotation type consists of one column for each attribute of the annotation. In the example, the attributes are `count`, `mean`, `stddev`, `min`, and `max` standing for the the column count, the mean value, the standard deviation, the minimum and the maximum value of each column. Each row represents one annotation to be created. The first column `ODF_ANNOTATED_COLUMN` stands for the column of the input data frame to which the annotation should be assigned.

ODF_ANNOTATED_COLUMN    |count   |                mean |              stddev |       min |       max
------------------------|--------|---------------------|---------------------|-----------|----------
              CLIENT_ID |  499.0 |   1764.374749498998 |  108.14436025195488 |    1578.0 |    1951.0
                    AGE |  499.0 |   54.65130260521042 |  19.924220223453258 |      17.0 |      91.0
          NBR_YEARS_CLI |  499.0 |  16.847695390781563 |  10.279080097460023 |       0.0 |      48.0
        AVERAGE_BALANCE |  499.0 |   17267.25809619238 |   30099.68272689043 |  -77716.0 |  294296.0
             ACCOUNT_ID |  499.0 |   126814.4749498998 |  43373.557241804665 |  101578.0 |  201950.0

If there is no (first) column named `ODF_ANNOTATED_COLUMN`, the annotations will be assigned to the data set rather than to its columns. The following example annotation data frame of type *SparkTableAnnotation* assigns a single attribute `count` to the data set:

| count |
|-------|
| 499   |

### Example implementation

The implementation of the The *summary statistics*  discovery service may be used as a reference implementation for the DataFrame interface. It is available in class `SummaryStatistics` of project `odf-spark-example-application`.

## Generic interface

The *generic* interface provides the full flexibility of ODF discovery services implemented in Java (or Scala):

* No restrictions regarding the types of data sets to be analyzed.
* Arbitrary objects may be annotated because references to arbitrary objects may be retrieved from the meta data catalog.
* Annotations may contain nested structures of data types and references to arbitrary objects.

On the downside, the generic interface may be slightly more difficult to use than the DataFrame interface:

* Discovery service must implement a specific ODF interface.
* Spark RDDs, data frames etc. must be explicitly constructed (Helper methods are available in class `SparkUtils`).
* Resulting annotations must be explicitly constructed and linked to the annotated objects.

### Methods to be implemented

The Spark application must implement the `SparkDiscoveryService` interface available in ODF project `odf-core-api`:

	public class SparkDiscoveryServiceExample extends SparkDiscoveryServiceBase implements SparkDiscoveryService

The interface consists of the following two methods that are described in detail in the [Java Docs for ODF services](./apidocs/index.html). The `SparkDiscoveryServiceBase` can be extended for convenience as the `SparkDiscoveryService` interface has much more methods.

#### Actual discovery service logic

This method is called to run the actual discovery service.

	DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request)

#### Validation whether data set can be accessed

This method is called internally before running the actual discovery service.

	DataSetCheckResult checkDataSet(DataSetContainer dataSetContainer)

### Example implementation

Class class `SparkDiscoveryServiceExample` in project `odf-spark-example-application` provides an example implementation of a *generic* discovery service. It provides an alternative implementation of the *summary statistics*  discovery service.

  [1]: http://spark.apache.org/
  [2]: http://spark.apache.org/docs/latest/api/java/index.html

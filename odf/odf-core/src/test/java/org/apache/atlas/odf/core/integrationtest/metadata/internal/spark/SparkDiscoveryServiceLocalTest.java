/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.odf.core.integrationtest.metadata.internal.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceManager;
import org.apache.atlas.odf.api.metadata.MetadataQueryBuilder;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.api.metadata.models.DataSet;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.metadata.models.Table;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceSparkEndpoint;
import org.apache.atlas.odf.api.discoveryservice.ServiceNotFoundException;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceSparkEndpoint.SERVICE_INTERFACE_TYPE;
import org.apache.atlas.odf.api.settings.SparkConfig;
import org.apache.atlas.odf.core.test.ODFTestBase;

public class SparkDiscoveryServiceLocalTest extends ODFTestBase {
	protected static Logger logger = Logger.getLogger(SparkDiscoveryServiceLocalTest.class.getName());
	public static int WAIT_MS_BETWEEN_POLLING = 2000;
	public static int MAX_NUMBER_OF_POLLS = 400;
	public static String DISCOVERY_SERVICE_ID = "spark-summary-statistics-example-service";
	public static String DASHDB_DB = "BLUDB";
	public static String DASHDB_SCHEMA = "SAMPLES";
	public static String DASHDB_TABLE = "CUST_RETENTION_LIFE_DURATION";
	public static enum DATASET_TYPE {
		FILE, TABLE
	}

	@BeforeClass
	public static void createSampleData() throws Exception {
		MetadataStore mds = new ODFFactory().create().getMetadataStore();
		if (mds.search(mds.newQueryBuilder().objectType("DataFile").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "BankClientsShort").build()).size() == 0) {
			mds.createSampleData();
		}
	}

	public static SparkConfig getLocalSparkConfig() {
		SparkConfig config = new SparkConfig();
		config.setClusterMasterUrl("local");
		return config;
	}

	public static DiscoveryServiceProperties getSparkSummaryStatisticsService() throws JSONException {
		DiscoveryServiceProperties dsProperties = new DiscoveryServiceProperties();
		dsProperties.setId(DISCOVERY_SERVICE_ID);
		dsProperties.setName("Spark summary statistics service");
		dsProperties.setDescription("Example discovery service calling summary statistics Spark application");
		dsProperties.setCustomDescription("");
		dsProperties.setIconUrl("spark.png");
		dsProperties.setLink("http://www.spark.apache.org");
		dsProperties.setPrerequisiteAnnotationTypes(null);
		dsProperties.setResultingAnnotationTypes(null);
		dsProperties.setSupportedObjectTypes(null);
		dsProperties.setAssignedObjectTypes(null);
		dsProperties.setAssignedObjectCandidates(null);
		dsProperties.setParallelismCount(2);
		DiscoveryServiceSparkEndpoint endpoint = new DiscoveryServiceSparkEndpoint();
		endpoint.setJar("META-INF/spark/odf-spark-example-application-1.2.0-SNAPSHOT.jar");
		endpoint.setClassName("org.apache.atlas.odf.core.spark.SummaryStatistics");
		endpoint.setInputMethod(SERVICE_INTERFACE_TYPE.DataFrame);
		dsProperties.setEndpoint(JSONUtils.convert(endpoint, DiscoveryServiceEndpoint.class));
		return dsProperties;
	}

	public static DiscoveryServiceProperties getSparkDiscoveryServiceExample() throws JSONException {
		DiscoveryServiceProperties dsProperties = new DiscoveryServiceProperties();
		dsProperties.setId(DISCOVERY_SERVICE_ID);
		dsProperties.setName("Spark summary statistics service");
		dsProperties.setDescription("Example discovery service calling summary statistics Spark application");
		dsProperties.setCustomDescription("");
		dsProperties.setIconUrl("spark.png");
		dsProperties.setLink("http://www.spark.apache.org");
		dsProperties.setPrerequisiteAnnotationTypes(null);
		dsProperties.setResultingAnnotationTypes(null);
		dsProperties.setSupportedObjectTypes(null);
		dsProperties.setAssignedObjectTypes(null);
		dsProperties.setAssignedObjectCandidates(null);
		dsProperties.setParallelismCount(2);
		DiscoveryServiceSparkEndpoint endpoint = new DiscoveryServiceSparkEndpoint();
		endpoint.setJar("META-INF/spark/odf-spark-example-application-1.2.0-SNAPSHOT.jar");
		endpoint.setClassName("org.apache.atlas.odf.core.spark.SparkDiscoveryServiceExample");
		endpoint.setInputMethod(SERVICE_INTERFACE_TYPE.Generic);
		dsProperties.setEndpoint(JSONUtils.convert(endpoint, DiscoveryServiceEndpoint.class));
		return dsProperties;
	}

	public static DataFile getTestDataFile(MetadataStore mds) {
		DataFile dataSet = null;
		List<MetaDataObjectReference> refs = mds.search(mds.newQueryBuilder().objectType("DataFile").build());
		for (MetaDataObjectReference ref : refs) {
			DataFile file = (DataFile) mds.retrieve(ref);
			if (file.getName().equals("BankClientsShort")) {
				dataSet = file;
				break;
			}
		}
		Assert.assertNotNull(dataSet);
		logger.log(Level.INFO, "Testing Spark discovery service on metadata object {0} (ref: {1})", new Object[] { dataSet.getName(), dataSet.getReference() });
		return dataSet;
	}

	public static Table getTestTable(MetadataStore mds) {
		Table dataSet = null;
		List<MetaDataObjectReference> refs = mds.search(mds.newQueryBuilder().objectType("Table").build());
		for (MetaDataObjectReference ref : refs) {
			Table table = (Table) mds.retrieve(ref);
			if (table.getName().equals(DASHDB_TABLE)) {
				dataSet = table;
				break;
			}
		}
		Assert.assertNotNull(dataSet);
		logger.log(Level.INFO, "Testing Spark discovery service on metadata object {0} (ref: {1})", new Object[] { dataSet.getName(), dataSet.getReference() });
		return dataSet;
	}

	public static AnalysisRequest getSparkAnalysisRequest(DataSet dataSet) {
		AnalysisRequest request = new AnalysisRequest();
		List<MetaDataObjectReference> dataSetRefs = new ArrayList<>();
		dataSetRefs.add(dataSet.getReference());
		request.setDataSets(dataSetRefs);
		List<String> serviceIds = Arrays.asList(new String[]{DISCOVERY_SERVICE_ID});
		request.setDiscoveryServiceSequence(serviceIds);
		return request;
	}

	public void runSparkServiceTest(SparkConfig sparkConfig, DATASET_TYPE dataSetType, DiscoveryServiceProperties regInfo, String[] annotationNames) throws Exception{
		logger.info("Using Spark configuration: " + JSONUtils.toJSON(sparkConfig));
		SettingsManager config = new ODFFactory().create().getSettingsManager();
		ODFSettings settings = config.getODFSettings();
		settings.setSparkConfig(sparkConfig);
		config.updateODFSettings(settings);

		logger.info("Using discovery service: " + JSONUtils.toJSON(regInfo));
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();

		try {
			discoveryServicesManager.deleteDiscoveryService(DISCOVERY_SERVICE_ID);
		} catch(ServiceNotFoundException e) {
			// Ignore exception because service may not exist
		}
		discoveryServicesManager.createDiscoveryService(regInfo);

		MetadataStore mds = new ODFFactory().create().getMetadataStore();
		Assert.assertNotNull(mds);
		AnnotationStore as = new ODFFactory().create().getAnnotationStore();
		Assert.assertNotNull(as);

		RelationalDataSet dataSet = null;
		if (dataSetType == DATASET_TYPE.FILE) {
			dataSet = getTestDataFile(mds);
		} else if (dataSetType == DATASET_TYPE.TABLE) {
			dataSet = getTestTable(mds);
		} else {
			Assert.fail();
		}

		logger.info("Using dataset: " + JSONUtils.toJSON(dataSet));

		AnalysisRequest request = getSparkAnalysisRequest(dataSet);
		logger.info("Using analysis request: " + JSONUtils.toJSON(request));

		logger.info("Starting analysis...");
		AnalysisResponse response = analysisManager.runAnalysis(request);
		Assert.assertNotNull(response);
		String requestId = response.getId();
		Assert.assertNotNull(requestId);
		logger.info("Request id is " + requestId + ".");

		logger.info("Waiting for request to finish");
		AnalysisRequestStatus status = null;
		int maxPolls = MAX_NUMBER_OF_POLLS;
		do {
			status = analysisManager.getAnalysisRequestStatus(requestId);
			logger.log(Level.INFO, "Poll request for request ID ''{0}'', state: ''{1}'', details: ''{2}''", new Object[] { requestId, status.getState(), status.getDetails() });
			maxPolls--;
			try {
				Thread.sleep(WAIT_MS_BETWEEN_POLLING);
			} catch (InterruptedException e) {
				logger.log(Level.INFO, "Exception thrown: ", e);
			}
		} while (maxPolls > 0 && (status.getState() == AnalysisRequestStatus.State.ACTIVE || status.getState() == AnalysisRequestStatus.State.QUEUED || status.getState() == AnalysisRequestStatus.State.NOT_FOUND));
		if (maxPolls == 0) {
			logger.log(Level.INFO, "Request ''{0}'' is not finished yet, don't wait for it", requestId);
		}
		Assert.assertEquals(AnalysisRequestStatus.State.FINISHED, status.getState());

		List<Annotation> annots = as.getAnnotations(null, status.getRequest().getId());
		logger.info("Number of annotations created: " + annots.size());
		Assert.assertTrue("No annotations have been created.", annots.size() > 0);

		logger.log(Level.INFO, "Request ''{0}'' is finished.", requestId);

		discoveryServicesManager.deleteDiscoveryService(DISCOVERY_SERVICE_ID);
	}

	@Test
	public void testLocalSparkClusterWithLocalDataFile() throws Exception{
		runSparkServiceTest(getLocalSparkConfig(), DATASET_TYPE.FILE, getSparkSummaryStatisticsService(), new String[] { "SparkSummaryStatisticsAnnotation", "SparkTableAnnotation" });
	}

	@Test
	public void testLocalSparkClusterWithLocalDataFileAndDiscoveryServiceRequest() throws Exception{
		runSparkServiceTest(getLocalSparkConfig(), DATASET_TYPE.FILE, getSparkDiscoveryServiceExample(), new String[] { "SparkSummaryStatisticsAnnotation", "SparkTableAnnotation" });
	}
}

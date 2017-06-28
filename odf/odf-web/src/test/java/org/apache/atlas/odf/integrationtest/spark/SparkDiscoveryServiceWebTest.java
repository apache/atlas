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
package org.apache.atlas.odf.integrationtest.spark;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.rest.test.RestTestBase;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.wink.json4j.JSONException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.RemoteMetadataStore;
import org.apache.atlas.odf.core.Encryption;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus.State;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceSparkEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceSparkEndpoint.SERVICE_INTERFACE_TYPE;
import org.apache.atlas.odf.core.integrationtest.metadata.internal.spark.SparkDiscoveryServiceLocalTest;
import org.apache.atlas.odf.core.integrationtest.metadata.internal.spark.SparkDiscoveryServiceLocalTest.DATASET_TYPE;
import org.apache.atlas.odf.api.settings.SparkConfig;
import org.apache.atlas.odf.json.JSONUtils;

public class SparkDiscoveryServiceWebTest extends RestTestBase {
	protected static Logger logger = Logger.getLogger(SparkDiscoveryServiceWebTest.class.getName());

	@Before
	public void createSampleData() throws Exception {
		Executor exec = getRestClientManager().getAuthenticatedExecutor();
		Request req = Request.Get(getBaseURI() + "/metadata/sampledata");
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		checkResult(httpResp, HttpStatus.SC_OK);
	}

	public static DiscoveryServiceProperties getSparkSummaryStatisticsService() throws JSONException {
		DiscoveryServiceProperties dsProperties = new DiscoveryServiceProperties();
		dsProperties.setId(SparkDiscoveryServiceLocalTest.DISCOVERY_SERVICE_ID);
		dsProperties.setName("Spark summary statistics service");
		dsProperties.setDescription("Example discovery service calling summary statistics Spark application");
		dsProperties.setIconUrl("spark.png");
		dsProperties.setLink("http://www.spark.apache.org");
		dsProperties.setPrerequisiteAnnotationTypes(null);
		dsProperties.setResultingAnnotationTypes(null);
		dsProperties.setSupportedObjectTypes(null);
		dsProperties.setAssignedObjectTypes(null);
		dsProperties.setAssignedObjectCandidates(null);
		dsProperties.setParallelismCount(2);
		DiscoveryServiceSparkEndpoint endpoint = new DiscoveryServiceSparkEndpoint();
		endpoint.setJar("file:///tmp/odf-spark/odf-spark-example-application-1.2.0-SNAPSHOT.jar");
		endpoint.setInputMethod(SERVICE_INTERFACE_TYPE.DataFrame);
		endpoint.setClassName("org.apache.atlas.odf.core.spark.SummaryStatistics");
		dsProperties.setEndpoint(JSONUtils.convert(endpoint, DiscoveryServiceEndpoint.class));
		return dsProperties;
	}

	public void runSparkServiceTest(SparkConfig sparkConfig, DATASET_TYPE dataSetType, DiscoveryServiceProperties regInfo, String[] annotationNames) throws Exception{
		logger.log(Level.INFO, "Testing spark application on ODF webapp url {0}.", getOdfBaseUrl());

		logger.info("Using Spark configuration: " + JSONUtils.toJSON(sparkConfig));
		ODFSettings settings = settingsRead();
		settings.setSparkConfig(sparkConfig);
		settings.setOdfUrl(Utils.getSystemPropertyExceptionIfMissing("odf.test.webapp.url"));
		settingsWrite(JSONUtils.toJSON(settings), HttpStatus.SC_OK);

		logger.log(Level.INFO, "Trying to delete existing discovery service: " + SparkDiscoveryServiceLocalTest.DISCOVERY_SERVICE_ID);
		deleteService(SparkDiscoveryServiceLocalTest.DISCOVERY_SERVICE_ID);

		logger.info("Using discovery service: " + JSONUtils.toJSON(regInfo));
		createService(JSONUtils.toJSON(regInfo), HttpStatus.SC_OK);

		checkServiceExists(regInfo.getId());

		MetadataStore mds = new RemoteMetadataStore(getOdfBaseUrl(), getOdfUser(), Encryption.decryptText(getOdfPassword()), true);
		Assert.assertNotNull(mds);


		RelationalDataSet dataSet = null;
		if (dataSetType == DATASET_TYPE.FILE) {
			dataSet = SparkDiscoveryServiceLocalTest.getTestDataFile(mds);
		} else if (dataSetType == DATASET_TYPE.TABLE) {
			dataSet = SparkDiscoveryServiceLocalTest.getTestTable(mds);
		} else {
			Assert.assertTrue(false);
		}
		logger.info("Using dataset: " + JSONUtils.toJSON(dataSet));

		AnnotationStore as = new ODFFactory().create().getAnnotationStore();

		AnalysisRequest request = SparkDiscoveryServiceLocalTest.getSparkAnalysisRequest(dataSet);
		logger.info("Using analysis request: " + JSONUtils.toJSON(request));

		logger.info("Starting analysis...");
		String requestId = runAnalysis(request, State.FINISHED);

		List<Annotation> annots = as.getAnnotations(null, requestId);
		logger.info("Number of annotations created: " + annots.size());
		Assert.assertTrue("No annotations have been created.", annots.size() > 0);
	}

	@Test
	public void testSparkServiceRESTAPI() throws Exception{
		runSparkServiceTest(SparkDiscoveryServiceLocalTest.getLocalSparkConfig(), DATASET_TYPE.FILE, getSparkSummaryStatisticsService(), new String[] { "SparkSummaryStatisticsAnnotation", "SparkTableAnnotation" });
	}

}

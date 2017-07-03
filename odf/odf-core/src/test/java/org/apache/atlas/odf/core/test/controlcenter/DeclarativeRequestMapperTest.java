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
package org.apache.atlas.odf.core.test.controlcenter;

import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceJavaEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceManager;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.core.controlcenter.DeclarativeRequestMapper;
import org.apache.atlas.odf.core.test.ODFTestBase;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;
import org.junit.Assert;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.core.controlcenter.ControlCenter;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.ServiceNotFoundException;

public class DeclarativeRequestMapperTest extends ODFTestBase {
	final private static String SERVICE_CLASSNAME = "org.apache.atlas.odf.core.test.discoveryservice.TestAsyncDiscoveryService1";
	final private static String[] EXPECTED_SERVICE_SEQUENCES = new String[] { "pre3,ser1", "alt1,ser1", "pre4,pre1,ser1", 
			"pre3,ser1,ser3", "pre3,ser1,ser5", "alt1,ser1,ser3", "alt1,ser1,ser5", "pre3,pre2,ser4", "alt1,pre2,ser4", 
			"pre4,pre1,ser1,ser3", "pre4,pre1,ser1,ser5", "pre3,ser1,alt1,ser3", "pre3,ser1,pre2,ser4", "pre3,ser1,alt1,ser5" };
	private Logger logger = Logger.getLogger(ControlCenter.class.getName());

	private static void createDiscoveryService(String serviceId, String[] resultingAnnotationTypes, String[] prerequisiteAnnotationTypes, String[] supportedObjectTypes) throws ValidationException, JSONException {
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		DiscoveryServiceProperties dsProperties = new DiscoveryServiceProperties();
		DiscoveryServiceJavaEndpoint dse = new DiscoveryServiceJavaEndpoint();
		dse.setClassName(SERVICE_CLASSNAME);
		dsProperties.setEndpoint(JSONUtils.convert(dse, DiscoveryServiceEndpoint.class));
		dsProperties.setId(serviceId);
		dsProperties.setName(serviceId + " Discovery Service");
		dsProperties.setPrerequisiteAnnotationTypes(Arrays.asList(prerequisiteAnnotationTypes));
		dsProperties.setResultingAnnotationTypes(Arrays.asList(resultingAnnotationTypes));
		dsProperties.setSupportedObjectTypes(Arrays.asList(supportedObjectTypes));
		discoveryServicesManager.createDiscoveryService(dsProperties);
	}

	private void deleteDiscoveryService(String serviceId, boolean failOnError) throws ValidationException {
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		try {
			discoveryServicesManager.deleteDiscoveryService(serviceId);
		}
		catch (ServiceNotFoundException e) {
			if (failOnError) {
				Assert.fail("Error deleting discovery services.");
			}
		}		
	}

	private void deleteDiscoveryServices(boolean failOnError) throws ValidationException {
		List<String> serviceIds = Arrays.asList(new String[] { "ser1", "ser2", "ser3", "ser4", "ser5", "pre1", "pre2", "pre3", "pre4", "alt1" });
		for (String serviceId : serviceIds) {
			deleteDiscoveryService(serviceId, failOnError);
		}
	}

	private void createDiscoveryServices() throws ValidationException, JSONException {
		createDiscoveryService("ser1", new String[] { "an1", "com1", "com2" }, new String[] { "pre1"         }, new String[] { "Table", "DataFile" });
		createDiscoveryService("ser2", new String[] { "an2", "com1"         }, new String[] { "pre2"         }, new String[] { "Table", "DataFile" });
		createDiscoveryService("ser3", new String[] {                "com2" }, new String[] { "pre1"         }, new String[] { "Table", "DataFile" });
		createDiscoveryService("ser4", new String[] { "an1", "com1", "com2" }, new String[] { "pre1", "pre2" }, new String[] { "Table", "DataFile" });
		createDiscoveryService("ser5", new String[] {        "com1", "com2" }, new String[] { "pre1"         }, new String[] { "Table", "DataFile" });

		createDiscoveryService("pre1", new String[] { "pre1"                }, new String[] { "pre4"         }, new String[] { "Table", "DataFile" });
		createDiscoveryService("pre2", new String[] { "pre2"                }, new String[] {                }, new String[] { "Table", "DataFile" });
		createDiscoveryService("pre3", new String[] { "pre1"                }, new String[] {                }, new String[] { "Table", "DataFile" });
		createDiscoveryService("pre4", new String[] { "pre4"                }, new String[] {                }, new String[] { "Table", "DataFile" });

		createDiscoveryService("alt1", new String[] { "pre1"                }, new String[] {                }, new String[] { "Table", "DataFile" });
	}

	@Test
	public void testDiscoveryServiceSequences() throws Exception {
		deleteDiscoveryServices(false);
		createDiscoveryServices();

		AnalysisRequest request = new AnalysisRequest();
		request.setAnnotationTypes(Arrays.asList( new String[] { "an1", "com2" }));
		DeclarativeRequestMapper mapper = new DeclarativeRequestMapper(request);
		logger.log(Level.INFO, "Printing list of mapper result to stdout.");
		int i = 0;
		for (DeclarativeRequestMapper.DiscoveryServiceSequence discoveryApproach : mapper.getDiscoveryServiceSequences()) {
			String sequence = Utils.joinStrings(new ArrayList<String>(discoveryApproach.getServiceSequence()), ',');
			System.out.println(sequence);
			if (i < EXPECTED_SERVICE_SEQUENCES.length) {
				Assert.assertTrue(sequence.equals(EXPECTED_SERVICE_SEQUENCES[i++]));
			}
		}
		Assert.assertEquals("Number of calculated discovery service sequences does not match expected value.", 36, mapper.getDiscoveryServiceSequences().size());

		deleteDiscoveryServices(true);
	}

	@Test
	public void testRecommendedDiscoveryServiceSequence() throws Exception {
		deleteDiscoveryServices(false);
		createDiscoveryServices();

		AnalysisRequest request = new AnalysisRequest();
		request.setAnnotationTypes(Arrays.asList( new String[] { "com2", "pre4" }));
		DeclarativeRequestMapper mapper = new DeclarativeRequestMapper(request);
		Assert.assertEquals("Recommended sequence does not match expected string.", "pre4,pre1,ser1", Utils.joinStrings(mapper.getRecommendedDiscoveryServiceSequence(), ','));

		deleteDiscoveryServices(true);
	}

	@Test
	public void testRemoveFailingService() throws Exception {
		deleteDiscoveryServices(false);
		createDiscoveryServices();

		AnalysisRequest request = new AnalysisRequest();
		request.setAnnotationTypes(Arrays.asList(new String[] { "an1", "com2" }));
		DeclarativeRequestMapper mapper = new DeclarativeRequestMapper(request);
		Assert.assertEquals("Original sequence does not match expected string.", EXPECTED_SERVICE_SEQUENCES[0], Utils.joinStrings(mapper.getRecommendedDiscoveryServiceSequence(), ','));

		mapper.removeDiscoveryServiceSequences("ser1");
		Assert.assertEquals("Updated sequence does not match expected string.", "pre3,pre2,ser4", Utils.joinStrings(mapper.getRecommendedDiscoveryServiceSequence(), ','));

		deleteDiscoveryServices(true);
	}

	@Test
	public void testRequestWithManyAnnotationTypes() throws Exception {
		deleteDiscoveryServices(false);
		createDiscoveryServices();

		AnalysisRequest request = new AnalysisRequest();
		request.setAnnotationTypes(Arrays.asList(new String[] {  "an1", "an2", "com1", "com2", "pre1", "pre2", "pre4" }));
		DeclarativeRequestMapper mapper = new DeclarativeRequestMapper(request);
		Assert.assertEquals("Number of calculated discovery service sequences does not match expected value.", 75, mapper.getDiscoveryServiceSequences().size());

		deleteDiscoveryServices(true);
	}
}

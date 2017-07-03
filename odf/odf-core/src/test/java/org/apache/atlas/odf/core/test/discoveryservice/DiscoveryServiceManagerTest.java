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
package org.apache.atlas.odf.core.test.discoveryservice;

import java.io.InputStream;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceManager;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceJavaEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRuntimeStatistics;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceStatus;
import org.apache.atlas.odf.api.discoveryservice.ServiceNotFoundException;

public class DiscoveryServiceManagerTest {
	
	final private static String ASYNCTESTWA_SERVICE_ID = "asynctestservice-with-annotations";

	final private static String NEW_SERVICE_ID = "New_Service";
	final private static String NEW_SERVICE_NAME = "Name of New Service";
	final private static String NEW_SERVICE_DESCRIPTION = "Description of the New Service";
	final private static String NEW_SERVICE_CLASSNAME = "org.apache.atlas.odf.core.test.discoveryservice.TestAsyncDiscoveryService1";
	
	final private static String UPDATED_SERVICE_DESCRIPTION = "Updated description of the New Service";
	final private static String UPDATED_SERVICE_CLASSNAME = "org.apache.atlas.odf.core.test.discoveryservice.TestSyncDiscoveryService1";
	
	private void registerDiscoveryService(DiscoveryServiceProperties dsProperties) throws ValidationException {
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		discoveryServicesManager.createDiscoveryService(dsProperties);
	}
	
	private void replaceDiscoveryService(DiscoveryServiceProperties dsProperties) throws ValidationException {
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		discoveryServicesManager.replaceDiscoveryService(dsProperties);
	}
	
	private void unregisterDiscoveryService(String serviceId) throws ServiceNotFoundException, ValidationException {
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		discoveryServicesManager.deleteDiscoveryService(serviceId);
	}
		
	@Test
	public void testGetDiscoveryServiceProperties() throws ServiceNotFoundException {
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		DiscoveryServiceProperties dsProperties = discoveryServicesManager.getDiscoveryServiceProperties(ASYNCTESTWA_SERVICE_ID);
		Assert.assertNotNull(dsProperties);
	}
	
		
	@Ignore @Test    // Ignoring testcase due to problem on Mac (issue #56)
	public void testGetDiscoveryServiceStatus() throws ServiceNotFoundException {
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		DiscoveryServiceStatus dsStatus = discoveryServicesManager.getDiscoveryServiceStatus(ASYNCTESTWA_SERVICE_ID);
		Assert.assertNotNull(dsStatus);
	}
	
	@Test  // TODO: need to adjust as soon as runtime statistics are available
	public void testGetDiscoveryServiceRuntimeStatistics() throws ServiceNotFoundException {
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		DiscoveryServiceRuntimeStatistics dsRuntimeStats = discoveryServicesManager.getDiscoveryServiceRuntimeStatistics(ASYNCTESTWA_SERVICE_ID);
		Assert.assertNotNull(dsRuntimeStats);
		long avgProcTime = dsRuntimeStats.getAverageProcessingTimePerItemInMillis();
		Assert.assertEquals(0, avgProcTime);
	}

	@Test
	public void testDeleteDiscoveryServiceRuntimeStatistics() throws ServiceNotFoundException {
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		discoveryServicesManager.deleteDiscoveryServiceRuntimeStatistics(ASYNCTESTWA_SERVICE_ID);
	}

	@Test
	public void testGetDiscoveryServiceImage() throws ServiceNotFoundException {
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		InputStream is = discoveryServicesManager.getDiscoveryServiceImage(ASYNCTESTWA_SERVICE_ID);
		Assert.assertNull(is);
	}

	@Test
	public void testCreateUpdateDelete() throws ServiceNotFoundException, ValidationException, JSONException {
		DiscoveryServiceJavaEndpoint dse = new DiscoveryServiceJavaEndpoint();
		dse.setClassName(NEW_SERVICE_CLASSNAME);
		DiscoveryServiceProperties dsProperties = new DiscoveryServiceProperties();
		dsProperties.setId(NEW_SERVICE_ID);
		dsProperties.setName(NEW_SERVICE_NAME);
		dsProperties.setDescription(NEW_SERVICE_DESCRIPTION);
		dsProperties.setLink(null);
		dsProperties.setPrerequisiteAnnotationTypes(null);
		dsProperties.setResultingAnnotationTypes(null);
		dsProperties.setSupportedObjectTypes(null);
		dsProperties.setAssignedObjectTypes(null);
		dsProperties.setAssignedObjectCandidates(null);
		dsProperties.setEndpoint(JSONUtils.convert(dse, DiscoveryServiceEndpoint.class));
		dsProperties.setParallelismCount(2);
		registerDiscoveryService(dsProperties);

		DiscoveryServiceJavaEndpoint dse2 = new DiscoveryServiceJavaEndpoint();
		dse2.setClassName(UPDATED_SERVICE_CLASSNAME);
		DiscoveryServiceProperties dsProperties2 = new DiscoveryServiceProperties();
		dsProperties2.setId(NEW_SERVICE_ID);
		dsProperties2.setName(NEW_SERVICE_NAME);
		dsProperties2.setDescription(UPDATED_SERVICE_DESCRIPTION);
		dsProperties2.setLink(null);
		dsProperties.setPrerequisiteAnnotationTypes(null);
		dsProperties.setResultingAnnotationTypes(null);
		dsProperties.setSupportedObjectTypes(null);
		dsProperties.setAssignedObjectTypes(null);
		dsProperties.setAssignedObjectCandidates(null);
		dsProperties2.setEndpoint(JSONUtils.convert(dse2, DiscoveryServiceEndpoint.class));
		dsProperties2.setParallelismCount(2);
		replaceDiscoveryService(dsProperties2);

		unregisterDiscoveryService(NEW_SERVICE_ID);
	}
	
}

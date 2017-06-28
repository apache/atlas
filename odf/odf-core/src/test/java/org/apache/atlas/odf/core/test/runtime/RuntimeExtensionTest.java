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
package org.apache.atlas.odf.core.test.runtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.OpenDiscoveryFramework;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus.State;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntime;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntimes;
import org.apache.atlas.odf.core.test.ODFTestBase;
import org.apache.atlas.odf.core.test.controlcenter.ODFAPITest;

public class RuntimeExtensionTest extends ODFTestBase {

	static final String SERVICE_ON_TEST_RUNTIME = "testruntimeservice";

	List<String> getNames(List<ServiceRuntime> rts) {
		List<String> result = new ArrayList<>();
		for (ServiceRuntime rt : rts) {
			result.add(rt.getName());
		}
		return result;
	}

	@Test
	public void testActiveRuntimes() {
		List<String> allNames = getNames(ServiceRuntimes.getAllRuntimes());
		Assert.assertTrue(allNames.contains(TestServiceRuntime.TESTSERVICERUNTIME_NAME));

		List<String> activeNames = getNames(ServiceRuntimes.getActiveRuntimes());
		Assert.assertTrue(activeNames.contains(TestServiceRuntime.TESTSERVICERUNTIME_NAME));
	}

	@Test
	public void testRuntimeForNewService() {
		ServiceRuntime rt = ServiceRuntimes.getRuntimeForDiscoveryService(SERVICE_ON_TEST_RUNTIME);
		Assert.assertNotNull(rt);
		Assert.assertEquals(TestServiceRuntime.TESTSERVICERUNTIME_NAME, rt.getName());
	}

	static Object lock = new Object();

	@Test
	public void testRuntimeExtensionSimple() throws Exception {
		synchronized (lock) {
			OpenDiscoveryFramework odf = new ODFFactory().create();
			TestServiceRuntime.runtimeBlocked = false;
			AnalysisRequest request = ODFAPITest.createAnalysisRequest(Collections.singletonList(ODFAPITest.DUMMY_SUCCESS_ID));
			request.setDiscoveryServiceSequence(Collections.singletonList(SERVICE_ON_TEST_RUNTIME));
			log.info("Starting service for test runtime");
			AnalysisResponse resp = odf.getAnalysisManager().runAnalysis(request);
			String requestId = resp.getId();
			Assert.assertTrue(ODFAPITest.waitForRequest(requestId, odf.getAnalysisManager(), 40, State.FINISHED));
			Assert.assertTrue(TestServiceRuntime.requests.contains(requestId));
			log.info("testRuntimeExtensionSimple finished");

			// block runtime again to restore state before testcase
			TestServiceRuntime.runtimeBlocked = true;
			Thread.sleep(5000);
		}
	}

	@Test
	public void testBlockedRuntimeExtension() throws Exception {
		synchronized (lock) {
			OpenDiscoveryFramework odf = new ODFFactory().create();
			TestServiceRuntime.runtimeBlocked = true;
			AnalysisRequest request = ODFAPITest.createAnalysisRequest(Collections.singletonList(ODFAPITest.DUMMY_SUCCESS_ID));
			request.setDiscoveryServiceSequence(Collections.singletonList(SERVICE_ON_TEST_RUNTIME));
			log.info("Starting service for test runtime");
			AnalysisResponse resp = odf.getAnalysisManager().runAnalysis(request);
			String requestId = resp.getId();
			Assert.assertFalse(resp.isInvalidRequest());
			log.info("Checking that service is not called");
			for (int i = 0; i < 5; i++) {
				Assert.assertFalse(TestServiceRuntime.requests.contains(requestId));
				Thread.sleep(1000);
			}
			log.info("Unblocking runtime...");
			TestServiceRuntime.runtimeBlocked = false;
			Thread.sleep(5000); // give service time to start consumption
			log.info("Checking that request has finished");
			Assert.assertTrue(ODFAPITest.waitForRequest(requestId, odf.getAnalysisManager(), 40, State.FINISHED));
			log.info("Checking that service was called");
			Assert.assertTrue(TestServiceRuntime.requests.contains(requestId));
			log.info("testBlockedRuntimeExtension finished");
			
			// block runtime again to restore state before testcase
			TestServiceRuntime.runtimeBlocked = true;
			Thread.sleep(5000);
		}
	}

}

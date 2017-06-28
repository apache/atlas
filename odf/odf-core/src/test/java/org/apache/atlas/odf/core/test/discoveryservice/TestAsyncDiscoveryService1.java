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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceBase;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;
import org.apache.atlas.odf.api.discoveryservice.async.DiscoveryServiceAsyncRunStatus;
import org.junit.Assert;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.async.AsyncDiscoveryService;
import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.api.discoveryservice.async.DiscoveryServiceAsyncStartResponse;
import org.apache.atlas.odf.json.JSONUtils;

public class TestAsyncDiscoveryService1 extends DiscoveryServiceBase implements AsyncDiscoveryService {

	static int unavailableCounter = 0;

	static Logger logger = ODFTestLogger.get();

	public static void checkUserAndAdditionalProperties(DiscoveryServiceRequest request) {
		String user = request.getUser();
		
		String defaultUser = System.getProperty("user.name");
		Assert.assertEquals(defaultUser, user);

		Map<String, Object> additionalProperties = request.getAdditionalProperties();
		logger.info("TestAsyncDiscoveryService1.startAnalysis additional properties: " + additionalProperties);
		Assert.assertNotNull(additionalProperties);
		
		// check that environment entries are also available additional properties
		Environment ev = new ODFInternalFactory().create(Environment.class);
		String dsId = request.getDiscoveryServiceId();
		Map<String, String> serviceEnvProps = ev.getPropertiesWithPrefix(dsId);
		if (!serviceEnvProps.isEmpty()) {
			Assert.assertTrue(!additionalProperties.isEmpty());
			for (Map.Entry<String, String> serviceEnvProp : serviceEnvProps.entrySet()) {
				String key = serviceEnvProp.getKey();
				String val = serviceEnvProp.getValue();
				logger.info("Found discoveryservice configuration parameter: " + key + " with value " + val);
				Assert.assertTrue(key.startsWith(dsId));
				Assert.assertTrue(additionalProperties.containsKey(key) );
				Assert.assertEquals(val, additionalProperties.get(key));
			}
		}
		
		if (!additionalProperties.isEmpty()) {
			Assert.assertTrue(additionalProperties.containsKey("aaa"));
			Assert.assertTrue("bbb".equals(additionalProperties.get("aaa")));
			Assert.assertTrue(additionalProperties.containsKey("jo"));
			@SuppressWarnings("unchecked")
			Map<String, Object> m = (Map<String, Object>) additionalProperties.get("jo");
			Assert.assertTrue("v1".equals(m.get("p1")));
			Assert.assertTrue("v2".equals(m.get("p2")));
			/*
			if (!additionalProperties.containsKey("aaa")) {
				response.setCode(ResponseCode.UNKNOWN_ERROR);
				response.setDetails("Additional property value 'aaa' doesn't exist");
				return;
			}
			if (!"bbb".equals(additionalProperties.get("aaa"))) {
				response.setCode(ResponseCode.UNKNOWN_ERROR);
				response.setDetails("Additional properties 'aaa' has wrong value");
				return;
			}
			if (!additionalProperties.containsKey("jo")) {
				response.setCode(ResponseCode.UNKNOWN_ERROR);
				response.setDetails("Additional property value 'jo' doesn't exist");
				return;
			}
			Map m = (Map) additionalProperties.get("jo");
			if (!"v1".equals(m.get("p1"))) {
				response.setCode(ResponseCode.UNKNOWN_ERROR);
				response.setDetails("Additional property value 'jo.p1' doesn't exist");
				return;

			}
			if (!"v2".equals(m.get("p2"))) {
				response.setCode(ResponseCode.UNKNOWN_ERROR);
				response.setDetails("Additional property value 'jo.p2' doesn't exist");
				return;
			}
			*/
		}
	}
	
	@Override
	public DiscoveryServiceAsyncStartResponse startAnalysis(DiscoveryServiceRequest request) {
		try {
			DiscoveryServiceResponse.ResponseCode code = DiscoveryServiceResponse.ResponseCode.TEMPORARILY_UNAVAILABLE;
			String details = "Cannot answer right now";
			if (unavailableCounter % 2 == 0) {
				code = DiscoveryServiceResponse.ResponseCode.OK;
				details = "Everything's peachy";
			}
			unavailableCounter++;
			/*
			if (unavailableCounter % 3 == 0) {
				code = CODE.NOT_AUTHORIZED;
				details = "You have no power here!";
			}
			*/
			DiscoveryServiceAsyncStartResponse response = new DiscoveryServiceAsyncStartResponse();
			response.setCode(code);
			response.setDetails(details);
			if (code == DiscoveryServiceResponse.ResponseCode.OK) {
				String runid = "TestAsyncService1" + UUID.randomUUID().toString();
				synchronized (lock) {
					runIDsRunning.put(runid, 4); // return status "running" 4 times before finishing
				}
				response.setRunId(runid);
				String dataSetId = request.getDataSetContainer().getDataSet().getReference().getId();
				if (dataSetId.startsWith("error")) {
					logger.info("TestAsync Discovery Service run " + runid + " will fail");
					runIDsWithError.add(runid);
				} else {
					logger.info("TestAsync Discovery Service run " + runid + " will succeed");
				}
			}
			logger.info("TestAsyncDiscoveryService1.startAnalysis returns: " + JSONUtils.lazyJSONSerializer(response));
			checkUserAndAdditionalProperties(request);
			/*
			String user = request.getUser();
			Assert.assertEquals(TestControlCenter.TEST_USER_ID, user);

			Map<String, Object> additionalProperties = request.getAdditionalProperties();
			logger.info("TestAsyncDiscoveryService1.startAnalysis additional properties: " + additionalProperties);
			Assert.assertNotNull(additionalProperties);
			if (!additionalProperties.isEmpty()) {
				if (!additionalProperties.containsKey("aaa")) {
					response.setCode(ResponseCode.UNKNOWN_ERROR);
					response.setDetails("Additional property value 'aaa' doesn't exist");
					return response;
				}
				if (!"bbb".equals(additionalProperties.get("aaa"))) {
					response.setCode(ResponseCode.UNKNOWN_ERROR);
					response.setDetails("Additional properties 'aaa' has wrong value");
					return response;
				}
				if (!additionalProperties.containsKey("jo")) {
					response.setCode(ResponseCode.UNKNOWN_ERROR);
					response.setDetails("Additional property value 'jo' doesn't exist");
					return response;
				}
				Map m = (Map) additionalProperties.get("jo");
				if (!"v1".equals(m.get("p1"))) {
					response.setCode(ResponseCode.UNKNOWN_ERROR);
					response.setDetails("Additional property value 'jo.p1' doesn't exist");
					return response;

				}
				if (!"v2".equals(m.get("p2"))) {
					response.setCode(ResponseCode.UNKNOWN_ERROR);
					response.setDetails("Additional property value 'jo.p2' doesn't exist");
					return response;
				}
			}
			*/
			return response;
		} catch (Throwable t) {
			DiscoveryServiceAsyncStartResponse response = new DiscoveryServiceAsyncStartResponse();
			response.setCode(DiscoveryServiceResponse.ResponseCode.UNKNOWN_ERROR);
			response.setDetails(Utils.getExceptionAsString(t));
			return response;
		}
	}

	static Object lock = new Object();
	static Map<String, Integer> runIDsRunning = new HashMap<String, Integer>();
	static Set<String> runIDsWithError = Collections.synchronizedSet(new HashSet<String>());

	//	static Map<String, Integer> requestIDUnavailable = new HashMap<>();

	@Override
	public DiscoveryServiceAsyncRunStatus getStatus(String runId) {
		String details = "Run like the wind";
		DiscoveryServiceAsyncRunStatus.State state = DiscoveryServiceAsyncRunStatus.State.RUNNING;
		synchronized (lock) {
			Integer i = runIDsRunning.get(runId);
			Assert.assertNotNull(i);
			if (i.intValue() == 0) {
				if (runIDsWithError.contains(runId)) {
					state = DiscoveryServiceAsyncRunStatus.State.ERROR;
					details = "This was a mistake";
				} else {
					state = DiscoveryServiceAsyncRunStatus.State.FINISHED;
					details = "Finish him!";
				}
			} else {
				runIDsRunning.put(runId, i - 1);
			}
		}

		DiscoveryServiceAsyncRunStatus status = new DiscoveryServiceAsyncRunStatus();
		status.setRunId(runId);
		status.setDetails(details);
		status.setState(state);
		logger.info("TestAsyncDiscoveryService1.getStatus returns: " + JSONUtils.lazyJSONSerializer(status));

		return status;
	}


}

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

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryService;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.SyncDiscoveryServiceBase;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntime;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;

public class TestServiceRuntime implements ServiceRuntime {

	static Logger logger = ODFTestLogger.get();

	public static final String TESTSERVICERUNTIME_NAME = "TestServiceRuntime";
	
	public static boolean runtimeBlocked = true;

	@Override
	public String getName() {
		return TESTSERVICERUNTIME_NAME;
	}

	@Override
	public long getWaitTimeUntilAvailable() {
		if (runtimeBlocked) {
			return 1000;
		}
		return 0;
	}

	public static Set<String> requests = new HashSet<>();

	public static class DSProxy extends SyncDiscoveryServiceBase {

		@Override
		public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request) {
			logger.info("Running test runtime service");
			requests.add(request.getOdfRequestId());
			DiscoveryServiceSyncResponse resp = new DiscoveryServiceSyncResponse();
			resp.setCode(DiscoveryServiceResponse.ResponseCode.OK);
			resp.setDetails("Test success");
			return resp;
		}
	}

	@Override
	public DiscoveryService createDiscoveryServiceProxy(DiscoveryServiceProperties props) {
		return new DSProxy();
	}

	@Override
	public String getDescription() {
		return "TestServiceRuntime description";
	}

	@Override
	public void validate(DiscoveryServiceProperties props) throws ValidationException {
	}

}

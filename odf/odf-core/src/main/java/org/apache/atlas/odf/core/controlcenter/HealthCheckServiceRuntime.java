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
package org.apache.atlas.odf.core.controlcenter;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryService;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.SyncDiscoveryServiceBase;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.api.discoveryservice.*;

public class HealthCheckServiceRuntime implements ServiceRuntime {
	public static final String HEALTH_CHECK_RUNTIME_NAME = "HealthCheck";

	@Override
	public String getName() {
		return HEALTH_CHECK_RUNTIME_NAME;
	}

	@Override
	public long getWaitTimeUntilAvailable() {
		return 0;
	}

	@Override
	public DiscoveryService createDiscoveryServiceProxy(DiscoveryServiceProperties props) {
		return new SyncDiscoveryServiceBase() {
			
			@Override
			public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request) {
				DiscoveryServiceSyncResponse response = new DiscoveryServiceSyncResponse();
				response.setCode(DiscoveryServiceResponse.ResponseCode.OK);
				response.setDetails("Health check service finished successfully");
				return response;
			}
		};
	}
	
	public static DiscoveryServiceProperties getHealthCheckServiceProperties() {		
		DiscoveryServiceProperties props = new DiscoveryServiceProperties();
		props.setId(ControlCenter.HEALTH_TEST_DISCOVERY_SERVICE_ID);
		props.setDescription("Health check service");
		
		DiscoveryServiceEndpoint ep = new DiscoveryServiceEndpoint();
		ep.setRuntimeName(HEALTH_CHECK_RUNTIME_NAME);
		
		props.setEndpoint(ep);
		return props;
	}

	@Override
	public String getDescription() {
		return "Internal runtime dedicated to health checks";
	}

	@Override
	public void validate(DiscoveryServiceProperties props) throws ValidationException {
	}

}

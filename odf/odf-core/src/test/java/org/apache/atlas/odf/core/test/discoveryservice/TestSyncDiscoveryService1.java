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

import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceBase;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.discoveryservice.sync.SyncDiscoveryService;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;

public class TestSyncDiscoveryService1 extends DiscoveryServiceBase implements SyncDiscoveryService {
	static int unavailableCounter = 0;

	Logger logger = ODFTestLogger.get();

	@Override
	public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request) {
		try {
			DiscoveryServiceResponse.ResponseCode code = DiscoveryServiceResponse.ResponseCode.TEMPORARILY_UNAVAILABLE;
			String details = "Cannot answer right now synchronously";
			if (unavailableCounter % 2 == 0) {
				code = DiscoveryServiceResponse.ResponseCode.OK;
				details = "Everything's peachy and synchronous";
			}
			unavailableCounter++;
			DiscoveryServiceSyncResponse response = new DiscoveryServiceSyncResponse();
			response.setDetails(details);
			response.setCode(code);
			if (code == DiscoveryServiceResponse.ResponseCode.OK) {
				String dataSetId = request.getDataSetContainer().getDataSet().getReference().getId();
				if (dataSetId.startsWith("error")) {
					response.setCode(DiscoveryServiceResponse.ResponseCode.UNKNOWN_ERROR);
					response.setDetails("Something went synchronously wrong!");
				} else {
					response.setDetails("All is synchronously fine!");
				}
				TestAsyncDiscoveryService1.checkUserAndAdditionalProperties(request);
			}
			logger.info(this.getClass().getSimpleName() + " service returned with code: " + response.getCode());
			return response;
		} catch (Throwable t) {
			t.printStackTrace();
			return null;
		}
	}

}

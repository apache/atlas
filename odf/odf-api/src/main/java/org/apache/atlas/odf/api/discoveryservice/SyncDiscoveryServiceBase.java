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
package org.apache.atlas.odf.api.discoveryservice;

import java.util.List;

import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse.ResponseCode;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.discoveryservice.sync.SyncDiscoveryService;

/**
 * 
 * This is an abstract class to extend when creating a synchronous discovery service
 *
 */
public abstract class SyncDiscoveryServiceBase extends DiscoveryServiceBase implements SyncDiscoveryService {
	
	protected DiscoveryServiceSyncResponse createSyncResponse(ResponseCode code, String detailsMessage, List<? extends Annotation> annotations) {
		try {
			DiscoveryServiceSyncResponse response = new DiscoveryServiceSyncResponse();
			response.setCode(code);
			response.setDetails(detailsMessage);
			DiscoveryServiceResult result = new DiscoveryServiceResult();
			if (annotations != null) {
				result.setAnnotations((List<Annotation>) annotations);
			}
			response.setResult(result);
			return response;
		} catch (Exception exc) {
			throw new RuntimeException(exc);
		}
	}
	
}

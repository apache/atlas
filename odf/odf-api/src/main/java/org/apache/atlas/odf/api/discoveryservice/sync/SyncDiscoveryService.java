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
package org.apache.atlas.odf.api.discoveryservice.sync;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryService;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;

/**
 * 
 * Synchronous discovery services must implement this interface
 *
 */
public interface SyncDiscoveryService extends DiscoveryService {

    /**
     * Runs the actual discovery service.
     * 
     * @param request Request parameter that includes a reference to the data set to be analyzed
     * @return Response object that includes the annotations to be created along with status information
     */
	DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request);
}

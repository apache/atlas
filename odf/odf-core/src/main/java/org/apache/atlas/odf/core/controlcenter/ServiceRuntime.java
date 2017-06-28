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
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.settings.validation.ValidationException;

public interface ServiceRuntime {
	
	String getName();
	
	/**
	 * Check if the runtime is currently available for processing.
	 * Returns <= 0 if the runtime is available immediately. A number > 0
	 * indicates how many seconds to wait until retrying.
	 * 
	 * Note: If this method returns > 0 the Kafka consumer will be shut down and only be 
	 * started again when it returns <= 0. Shutting down and restarting the consumer is
	 * rather costly so this should only be done if the runtime won't be accepting requests
	 * for a foreseeable period of time.
	 */
	long getWaitTimeUntilAvailable();

	DiscoveryService createDiscoveryServiceProxy(DiscoveryServiceProperties props);

	String getDescription();
	
	void validate(DiscoveryServiceProperties props) throws ValidationException;
	
}

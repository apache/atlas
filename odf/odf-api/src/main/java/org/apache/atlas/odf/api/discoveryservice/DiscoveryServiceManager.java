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

import java.io.InputStream;
import java.util.List;

import org.apache.atlas.odf.api.settings.validation.ValidationException;

/**
 *
 * External Java API for creating and managing discovery services
 *
 */
public interface DiscoveryServiceManager {

	/**
	 * Retrieve list of discovery services registered in ODF
	 * @return List of registered ODF discovery services
	 */
	public List<DiscoveryServiceProperties> getDiscoveryServicesProperties();

	/**
	 * Register a new service in ODF
	 * @param dsProperties Properties of the discovery service to register
	 * @throws ValidationException Validation of a property failed
	 */
	public void createDiscoveryService(DiscoveryServiceProperties dsProperties) throws ValidationException;

	/**
	 * Update configuration of an ODF discovery service
	 * @param dsProperties Properties of the discovery service to update
	 */
	public void replaceDiscoveryService(DiscoveryServiceProperties dsProperties) throws ServiceNotFoundException, ValidationException;

	/**
	 * Remove a registered service from ODF
	 * @param serviceId Discovery service ID
	 */
	public void deleteDiscoveryService(String serviceId) throws ServiceNotFoundException, ValidationException;

	/**
	 * Retrieve current configuration of a discovery services registered in ODF
	 * @param serviceId Discovery Service ID
	 * @return Properties of the service with this ID
	 * @throws ServiceNotFoundException A service with this ID is not registered
	 */
	public DiscoveryServiceProperties getDiscoveryServiceProperties(String serviceId) throws ServiceNotFoundException;

	/**
	 * Retrieve status overview of all discovery services registered in ODF
	 * @return List of status count maps for all discovery services
	 */
	public List<ServiceStatusCount> getDiscoveryServiceStatusOverview();

	/**
	 * Retrieve status of a specific discovery service. Returns null if no service info can be obtained
	 * @param serviceId Discovery Service ID
	 * @return Status of the service with this ID
	 */
	public DiscoveryServiceStatus getDiscoveryServiceStatus(String serviceId) throws ServiceNotFoundException;

	/**
	 * Retrieve runtime statistics of a specific discovery service
	 * @param serviceId Discovery Service ID
	 * @return Runtime statistics of the service with this ID
	 */
	public DiscoveryServiceRuntimeStatistics getDiscoveryServiceRuntimeStatistics(String serviceId) throws ServiceNotFoundException;

	/**
	 * Delete runtime statistics of a specific discovery service
	 * @param serviceId Discovery Service ID
	 */
	public void deleteDiscoveryServiceRuntimeStatistics(String serviceId) throws ServiceNotFoundException;

	/**
	 * Retrieve picture representing a discovery service
	 * @param serviceId Discovery Service ID
	 * @return Input stream for image
	 */
	public InputStream getDiscoveryServiceImage(String serviceId) throws ServiceNotFoundException;
}

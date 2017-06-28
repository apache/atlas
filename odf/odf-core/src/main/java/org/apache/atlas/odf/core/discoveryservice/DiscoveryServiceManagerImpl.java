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
package org.apache.atlas.odf.core.discoveryservice;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryService;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceManager;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRuntimeStatistics;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceStatus;
import org.apache.atlas.odf.api.discoveryservice.ServiceNotFoundException;
import org.apache.atlas.odf.api.discoveryservice.ServiceStatusCount;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.configuration.ConfigContainer;
import org.apache.atlas.odf.core.configuration.ConfigManager;
import org.apache.atlas.odf.core.controlcenter.AnalysisRequestTrackerStore;
import org.apache.atlas.odf.core.controlcenter.ControlCenter;

/**
 *
 * External Java API for creating and managing discovery services
 *
 */
public class DiscoveryServiceManagerImpl implements DiscoveryServiceManager {
	private Logger logger = Logger.getLogger(DiscoveryServiceManagerImpl.class.getName());
	public ConfigManager configManager;

	public DiscoveryServiceManagerImpl() {
		configManager = new ODFInternalFactory().create(ConfigManager.class);
	}

	/**
	 * Retrieve list of discovery services registered in ODF
	 * @return List of registered ODF discovery services
	 */
	public List<DiscoveryServiceProperties> getDiscoveryServicesProperties() {
		logger.entering(DiscoveryServiceManager.class.getName(), "getDiscoveryServicesProperties");
		List<DiscoveryServiceProperties> dsProperties = configManager.getConfigContainer().getRegisteredServices();
		return dsProperties;
	};

	/**
	 * Register a new service in ODF
	 * @param dsProperties Properties of the discovery service to register
	 * @throws ValidationException Validation of a property failed
	 */
	public void createDiscoveryService(DiscoveryServiceProperties dsProperties) throws ValidationException {
		logger.entering(DiscoveryServiceManager.class.getName(), "createDiscoveryService");
		ConfigContainer update = new ConfigContainer();
		List<DiscoveryServiceProperties> registeredServices = configManager.getConfigContainer().getRegisteredServices();
		registeredServices.addAll(Collections.singletonList(dsProperties));
		update.setRegisteredServices(registeredServices);
		configManager.updateConfigContainer(update);


	};

	/**
	 * Update configuration of an ODF discovery service
	 * @param dsProperties Properties of the discovery service to update
	 */
	public void replaceDiscoveryService(DiscoveryServiceProperties dsProperties) throws ServiceNotFoundException, ValidationException {
		logger.entering(DiscoveryServiceManager.class.getName(), "updateDiscoveryService");
		String serviceId = dsProperties.getId();
		deleteDiscoveryService(serviceId);
		createDiscoveryService(dsProperties);
	};

	/**
	 * Remove a registered service from ODF
	 * @param serviceId Discovery service ID
	 */
	public void deleteDiscoveryService(String serviceId) throws ServiceNotFoundException, ValidationException {
		logger.entering(DiscoveryServiceManager.class.getName(), "deleteDiscoveryService");
		ConfigContainer cc = configManager.getConfigContainer();
		Iterator<DiscoveryServiceProperties> iterator = cc.getRegisteredServices().iterator();
		boolean serviceFound = false;
		while (iterator.hasNext()) {
			if (iterator.next().getId().equals(serviceId)) {
				iterator.remove();
				serviceFound = true;
			}
		}
		if (!serviceFound) {
			throw new ServiceNotFoundException(serviceId);
		} else {
			configManager.updateConfigContainer(cc);
		}
	};

	/**
	 * Retrieve current configuration of a discovery services registered in ODF
	 * @param serviceId Discovery Service ID
	 * @return Properties of the service with this ID
	 * @throws ServiceNotFoundException A service with this ID is not registered
	 */
	public DiscoveryServiceProperties getDiscoveryServiceProperties(String serviceId) throws ServiceNotFoundException {
		logger.entering(DiscoveryServiceManager.class.getName(), "getDiscoveryServiceProperties");
		DiscoveryServiceProperties serviceFound = null;
		List<DiscoveryServiceProperties> registeredServices;
		registeredServices = configManager.getConfigContainer().getRegisteredServices();
		for (DiscoveryServiceProperties service : registeredServices) {
			if (service.getId().equals(serviceId)) {
				serviceFound = service;
				break;
			}
		}
		if (serviceFound == null) {
			throw new ServiceNotFoundException(serviceId);
		}
		return serviceFound;
	};

	/**
	 * Retrieve status overview of all discovery services registered in ODF
	 * @return List of status count maps for all discovery services
	 */
	public List<ServiceStatusCount> getDiscoveryServiceStatusOverview() {
		DiscoveryServiceStatistics stats = new DiscoveryServiceStatistics(new ODFInternalFactory().create(AnalysisRequestTrackerStore.class).getRecentTrackers(0,-1));
		return stats.getStatusCountPerService();
	}

	/**
	 * Retrieve status of a specific discovery service. Returns null if no service info can be obtained
	 * @param serviceId Discovery Service ID
	 * @return Status of the service with this ID
	 */
	public DiscoveryServiceStatus getDiscoveryServiceStatus(String serviceId) throws ServiceNotFoundException {
		logger.entering(DiscoveryServiceManager.class.getName(), "getDiscoveryServiceStatus");

		DiscoveryServiceStatus dsStatus = null;
		ControlCenter cc = new ODFInternalFactory().create(ControlCenter.class);
		DiscoveryService ds = cc.getDiscoveryServiceProxy(serviceId, null);
		if (ds == null) {
			throw new ServiceNotFoundException(serviceId);
		}
		dsStatus = new DiscoveryServiceStatus();
		dsStatus.setStatus(DiscoveryServiceStatus.Status.OK);
		dsStatus.setMessage(MessageFormat.format("Discovery service ''{0}'' status is OK", serviceId));
		ServiceStatusCount serviceStatus = null;
		List<ServiceStatusCount> statusCounts = getDiscoveryServiceStatusOverview();
		for (ServiceStatusCount cnt : statusCounts) {
			if (cnt.getId().equals(serviceId)) {
				serviceStatus = cnt;
				break;
			}
		}
		if (serviceStatus != null) {
			dsStatus.setStatusCount(serviceStatus);
		}
		return dsStatus;
	};

	/**
	 * Retrieve runtime statistics of a specific discovery service
	 * @param serviceId Discovery Service ID
	 * @return Runtime statistics of the service with this ID
	 */
	public DiscoveryServiceRuntimeStatistics getDiscoveryServiceRuntimeStatistics(String serviceId) throws ServiceNotFoundException {
		logger.entering(DiscoveryServiceManager.class.getName(), "getDiscoveryServiceRuntimeStatistics");
		DiscoveryServiceRuntimeStatistics dsrs = new DiscoveryServiceRuntimeStatistics();
		dsrs.setAverageProcessingTimePerItemInMillis(0);   // TODO: implement
		return dsrs;
	};

	/**
	 * Delete runtime statistics of a specific discovery service
	 * @param serviceId Discovery Service ID
	 */
	public void deleteDiscoveryServiceRuntimeStatistics(String serviceId) throws ServiceNotFoundException {
		logger.entering(DiscoveryServiceManager.class.getName(), "deleteDiscoveryServiceRuntimeStatistics");
		// TODO: implement
	};

	/**
	 * Retrieve picture representing a discovery service
	 * @param serviceId Discovery Service ID
	 * @return Input stream for image
	 */
	public InputStream getDiscoveryServiceImage(String serviceId) throws ServiceNotFoundException {
		logger.entering(DiscoveryServiceManager.class.getName(), "getDiscoveryServiceImage");
		final String defaultImageDir = "org/apache/atlas/odf/images";

		String imgUrl = null;
		for (DiscoveryServiceProperties info : configManager.getConfigContainer().getRegisteredServices()) {
			if (info.getId().equals(serviceId)) {
				imgUrl = info.getIconUrl();
				break;
			}
		}

		ClassLoader cl = this.getClass().getClassLoader();
		InputStream is = null;
		if (imgUrl != null) {
			is = cl.getResourceAsStream("META-INF/odf/" + imgUrl);
			if (is == null) {
				is = cl.getResourceAsStream(defaultImageDir + "/" + imgUrl);
				if (is == null) {
					try {
						is = new URL(imgUrl).openStream();
					} catch (MalformedURLException e) {
						logger.log(Level.WARNING, "The specified image url {0} for service {1} is invalid!", new String[] { imgUrl, serviceId });
					} catch (IOException e) {
						logger.log(Level.WARNING, "The specified image url {0} for service {1} could not be accessed!", new String[] { imgUrl, serviceId });
					}
				}
			}
		}
		if (imgUrl == null || is == null) {
			//TODO is this correct? maybe we should use a single default image instead of a random one
			try {
				is = cl.getResourceAsStream(defaultImageDir);
				if (is != null) {
					InputStreamReader r = new InputStreamReader(is);
					BufferedReader br = new BufferedReader(r);
					List<String> images = new ArrayList<>();
					String line = null;
					while ((line = br.readLine()) != null) {
						images.add(line);
					}
					// return random image
					int ix = Math.abs(serviceId.hashCode()) % images.size();
					is = cl.getResourceAsStream(defaultImageDir + "/" + images.get(ix));
				}
			} catch (IOException exc) {
				logger.log(Level.WARNING, "Exception occurred while retrieving random image, ignoring it", exc);
				is = null;
			}
		}
		return is;
	};

}

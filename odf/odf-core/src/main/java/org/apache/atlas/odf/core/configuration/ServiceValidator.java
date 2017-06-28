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
package org.apache.atlas.odf.core.configuration;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.settings.validation.PropertyValidator;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntime;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntimes;

public class ServiceValidator implements PropertyValidator {

	public void validate(String property, Object value) throws ValidationException {
		validate(property, value, true);
	}

	private void validate(String property, Object value, boolean topLevel) throws ValidationException {
		if (value == null) {
			throw new ValidationException("Null values are not allowed for this property");
		}

		if (value instanceof List) {
			List<DiscoveryServiceProperties> newServices = (List<DiscoveryServiceProperties>) value;
			List<String> ids = new ArrayList<String>();
			for (int no = 0; no < newServices.size(); no++) {
				DiscoveryServiceProperties service = (DiscoveryServiceProperties) newServices.get(no);
				validate(property, service, false);
				String serviceId = service.getId();
				if (ids.contains(serviceId)) {
					throw new ValidationException(property, MessageFormat.format("you cannot register multiple services with the same id {0}!", serviceId));
				} else {
					ids.add(serviceId);
				}
			}
		} else if (value instanceof DiscoveryServiceProperties) {
			DiscoveryServiceProperties service = (DiscoveryServiceProperties) value;
			if (service.getId() == null || service.getId().trim().isEmpty() || service.getName() == null || service.getName().trim().isEmpty() || service.getEndpoint() == null) {
				throw new ValidationException(property, MessageFormat.format("A service requires {0}", "id, name and an endpoint"));
			}

			if (topLevel) {
				List<String> regServices = new ArrayList<String>();
				List<DiscoveryServiceProperties> services = new ODFFactory().create().getDiscoveryServiceManager().getDiscoveryServicesProperties();
				for (DiscoveryServiceProperties regService : services) {
					regServices.add(regService.getId());
				}

				if (regServices.contains(service.getId())) {
					throw new ValidationException(property, MessageFormat.format("a service with id {0} already exists!", service.getId()));
				}
			}

			ServiceRuntime runtime = ServiceRuntimes.getRuntimeForDiscoveryService(service);
			runtime.validate(service);
		} else {
			throw new ValidationException(property, "only DiscoveryServiceRegistrationInfo objects or list of such objects are allowed for this property");
		}
	}
}

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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.sync.SyncDiscoveryService;
import org.apache.atlas.odf.api.settings.validation.ImplementationValidator;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryService;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceJavaEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.async.AsyncDiscoveryService;
import org.apache.atlas.odf.core.Utils;

public class JavaServiceRuntime implements ServiceRuntime {

	Logger logger = Logger.getLogger(JavaServiceRuntime.class.getName());

	public static final String NAME = "Java";
	
	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public long getWaitTimeUntilAvailable() {
		// for now, always run
		return 0;
	}

	@Override
	public DiscoveryService createDiscoveryServiceProxy(DiscoveryServiceProperties props) {
		DiscoveryService service = null;
		String className = null;
		try {
			className = JSONUtils.convert(props.getEndpoint(), DiscoveryServiceJavaEndpoint.class).getClassName();
			Class<?> clazz = Class.forName(className);
			Object o = clazz.newInstance();
			service = (DiscoveryService) o;
		} catch (Exception e) {
			logger.log(Level.FINE, "An error occurred while instatiating Java implementation", e);
			logger.log(Level.WARNING, "Java implementation ''{0}'' for discovery service ''{1}'' could not be instantiated (internal error: ''{2}'')",
					new Object[] { className, props.getId(), e.getMessage() });
			return null;
		}
		if (service instanceof SyncDiscoveryService) {
			return new TransactionSyncDiscoveryServiceProxy((SyncDiscoveryService) service);
		} else if (service instanceof AsyncDiscoveryService) {
			return new TransactionAsyncDiscoveryServiceProxy((AsyncDiscoveryService) service);
		}
		return service;
	}

	@Override
	public String getDescription() {
		return "The default Java runtime";
	}

	@Override
	public void validate(DiscoveryServiceProperties props) throws ValidationException {
		DiscoveryServiceJavaEndpoint javaEP;
		try {
			javaEP = JSONUtils.convert(props.getEndpoint(), DiscoveryServiceJavaEndpoint.class);
		} catch (JSONException e) {
			throw new ValidationException("Endpoint definition for Java service is not correct: " + Utils.getExceptionAsString(e));
		}
		new ImplementationValidator().validate("Service.endpoint", javaEP.getClassName());
	}

}

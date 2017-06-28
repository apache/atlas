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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceManager;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.ServiceNotFoundException;
import org.apache.atlas.odf.api.engine.ServiceRuntimeInfo;
import org.apache.atlas.odf.api.engine.ServiceRuntimesInfo;
import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInternalFactory;

public class ServiceRuntimes {

	static Logger logger = Logger.getLogger(ServiceRuntimes.class.getName());

	static List<ServiceRuntime> getRuntimeExtensions() throws IOException {
		ClassLoader cl = ServiceRuntimes.class.getClassLoader();
		Enumeration<URL> services = cl.getResources("META-INF/odf/odf-runtimes.txt");
		List<ServiceRuntime> result = new ArrayList<>();
		while (services.hasMoreElements()) {
			URL url = services.nextElement();
			InputStream is = url.openStream();
			InputStreamReader isr = new InputStreamReader(is, "UTF-8");
			LineNumberReader lnr = new LineNumberReader(isr);
			String line = null;
			while ((line = lnr.readLine()) != null) {
				line = line.trim();
				logger.log(Level.INFO,  "Loading runtime extension ''{0}''", line);
				try {
					@SuppressWarnings("unchecked")
					Class<ServiceRuntime> clazz = (Class<ServiceRuntime>) cl.loadClass(line);
					ServiceRuntime sr = clazz.newInstance();
					result.add(sr);
				} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
					logger.log(Level.WARNING, MessageFormat.format("Runtime extension of class ''{0}'' could not be instantiated", line), e);
				} 
			}
		}
		logger.log(Level.INFO, "Number of classpath services found: {0}", result.size());
		return result;
	}
	
	static {
		List<ServiceRuntime> allRuntimes = new ArrayList<>(Arrays.asList( //
				new HealthCheckServiceRuntime(), //
				new JavaServiceRuntime(), //
				new SparkServiceRuntime() //
		));
		try {
			List<ServiceRuntime> runtimeExtensions = getRuntimeExtensions();
			allRuntimes.addAll(runtimeExtensions);
		} catch (IOException e) {
			logger.log(Level.WARNING, "An exception occurred when loading runtime extensions, ignoring them", e);
		}
		runtimes = Collections.unmodifiableList(allRuntimes);
	}

	private static List<ServiceRuntime> runtimes;

	public static List<ServiceRuntime> getActiveRuntimes() {
		Environment env = new ODFInternalFactory().create(Environment.class);
		List<String> activeRuntimeNames = env.getActiveRuntimeNames();
		if (activeRuntimeNames == null) {
			return getAllRuntimes();
		}
		// always add health check runtime
		Set<String> activeRuntimeNamesSet = new HashSet<>(activeRuntimeNames);
		activeRuntimeNamesSet.add(HealthCheckServiceRuntime.HEALTH_CHECK_RUNTIME_NAME);
		List<ServiceRuntime> activeRuntimes = new ArrayList<>();
		for (ServiceRuntime rt : runtimes) {
			if (activeRuntimeNamesSet.contains(rt.getName())) {
				activeRuntimes.add(rt);
			}
		}
		return activeRuntimes;
	}

	public static List<ServiceRuntime> getAllRuntimes() {
		return runtimes;
	}

	public static ServiceRuntime getRuntimeForDiscoveryService(DiscoveryServiceProperties discoveryServiceProps) {
		DiscoveryServiceEndpoint ep = discoveryServiceProps.getEndpoint();
		for (ServiceRuntime runtime : getAllRuntimes()) {
			if (runtime.getName().equals(ep.getRuntimeName())) {
				return runtime;
			}
		}
		return null;
	}

	public static ServiceRuntime getRuntimeForDiscoveryService(String discoveryServiceId) {
		// special check because the healch check runtime is not part of the configuration
		if (discoveryServiceId.startsWith(ControlCenter.HEALTH_TEST_DISCOVERY_SERVICE_ID)) {
			return new HealthCheckServiceRuntime();
		}
		DiscoveryServiceManager dsm = new ODFInternalFactory().create(DiscoveryServiceManager.class);
		try {
			DiscoveryServiceProperties props = dsm.getDiscoveryServiceProperties(discoveryServiceId);
			return getRuntimeForDiscoveryService(props);
		} catch (ServiceNotFoundException e) {
			return null;
		}
	}

	public static ServiceRuntimesInfo getRuntimesInfo(List<ServiceRuntime> runtimes) {
		List<ServiceRuntimeInfo> rts = new ArrayList<>();
		for (ServiceRuntime rt : runtimes) {
			ServiceRuntimeInfo sri = new ServiceRuntimeInfo();
			sri.setName(rt.getName());
			sri.setDescription(rt.getDescription());
			rts.add(sri);
		}
		ServiceRuntimesInfo result = new ServiceRuntimesInfo();
		result.setRuntimes(rts);
		return result;
	}
}

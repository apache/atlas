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
package org.apache.atlas.odf.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.atlas.odf.core.configuration.ConfigContainer;

public class StandaloneEnvironment implements Environment {

	@Override
	public String getProperty(String propertyName) {
		return System.getProperty(propertyName);
	}

	@Override
	public String getCurrentUser() {
		return System.getProperty("user.name");
	}

	@Override
	public String getZookeeperConnectString() {
		return getProperty("odf.zookeeper.connect");
	}

	@Override
	public ConfigContainer getDefaultConfiguration() {
		return Utils.readConfigurationFromClasspath("org/apache/atlas/odf/core/internal/odf-initial-configuration.json");
	}

	@Override
	public Map<String, String> getPropertiesWithPrefix(String prefix) {
		Map<String, String> foundProps = new HashMap<>();
		Properties props = System.getProperties();
		for (String key : props.stringPropertyNames()) {
			if (key.startsWith(prefix)) {
				foundProps.put(key, props.getProperty(key));
			}
		}
		return foundProps;
	}

	@Override
	public List<String> getActiveRuntimeNames() {
		String p = getProperty("odf.active.runtimes");
		if (p == null || p.equals("ALL")) {
			return null;
		}
		if (p.equals("NONE")) {
			return new ArrayList<>();
		}
		return Arrays.asList(p.split(","));
	}

}

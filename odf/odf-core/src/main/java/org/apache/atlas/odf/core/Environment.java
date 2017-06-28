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

import java.util.List;
import java.util.Map;

import org.apache.atlas.odf.core.configuration.ConfigContainer;

public interface Environment {
	
	String getZookeeperConnectString();
	
	String getProperty(String propertyName);
	
	Map<String, String> getPropertiesWithPrefix(String prefix);
	
	String getCurrentUser();
	
	ConfigContainer getDefaultConfiguration();
	
	/**
	 * Returns the names of the runtimes active in this environment.
	 * Return null to indicate that all available runtimes should be active.
	 */
	List<String> getActiveRuntimeNames();

}

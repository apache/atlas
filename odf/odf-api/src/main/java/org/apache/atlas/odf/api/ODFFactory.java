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
package org.apache.atlas.odf.api;

import java.text.MessageFormat;

public class ODFFactory {

	private final static String ODF_DEFAULT_IMPLEMENTATION = "org.apache.atlas.odf.core.OpenDiscoveryFrameworkImpl";

	public OpenDiscoveryFramework create() {
		Object o = null;
		Class<?> clazz;
		try {
			clazz = this.getClass().getClassLoader().loadClass(ODF_DEFAULT_IMPLEMENTATION);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(MessageFormat.format("Class {0} was not found. Make sure that the odf-core jar and all its dependencies are available on the classpath.", ODF_DEFAULT_IMPLEMENTATION));
		}
		try {
			o = clazz.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(MessageFormat.format("Class {0} was found on the classpath but could not be accessed.", ODF_DEFAULT_IMPLEMENTATION));
		}
		if (o instanceof OpenDiscoveryFramework) {
			return (OpenDiscoveryFramework) o;
		} else {
			throw new RuntimeException(MessageFormat.format("The class {0} on the classpath is not of type OpenDiscoveryFramework.", ODF_DEFAULT_IMPLEMENTATION));
		}
	}
}

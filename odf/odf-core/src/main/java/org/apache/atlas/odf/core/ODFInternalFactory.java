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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.utils.ODFLogConfig;

public class ODFInternalFactory {

	private static Properties defaultImplemenetations = Utils.readConfigProperties("org/apache/atlas/odf/core/internal/odf-default-implementation.properties");
	private static ODFImplementations overwrittenImplementations = null;
	private static Map<Class<?>, Object> singletons = new HashMap<>();

	public static String SINGLETON_MARKER = "@singleton";

	static {
		ODFLogConfig.run();

		Logger logger = Logger.getLogger(ODFInternalFactory.class.getName());
		ClassLoader cl = ODFInternalFactory.class.getClassLoader();
		String overwriteConfig = "org/apache/atlas/odf/odf-implementation.properties";
		overwrittenImplementations = new ODFImplementations(overwriteConfig, cl);
		if (overwrittenImplementations.getImplementations().isEmpty()) {
			overwrittenImplementations = null;
		} else {
			logger.log(Level.INFO, "Found overwritten implementation config: {0}", overwrittenImplementations.getImplementations());
		}
		if (overwrittenImplementations == null) {
			logger.log(Level.INFO, "Default implementations are used");
		}
	}

	private Object createObject(Class<?> cl) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		String clazz = null;
		if (overwrittenImplementations != null) {
			clazz = overwrittenImplementations.getImplementations().get(cl.getName());
		}
		if (clazz == null) {
			clazz = defaultImplemenetations.getProperty(cl.getName());
		}
		if (clazz == null) {
			// finally try to instantiate the class as such
			clazz = cl.getName();
		}
		boolean isSingleton = false;
		if (clazz.endsWith(SINGLETON_MARKER)) {
			clazz = clazz.substring(0, clazz.length() - SINGLETON_MARKER.length());
			isSingleton = true;
		}
		Object o = null;
		Class<?> implClass = this.getClass().getClassLoader().loadClass(clazz);
		if (isSingleton) {
			o = singletons.get(implClass);
			if (o == null) {
				o = implClass.newInstance();
				singletons.put(implClass, o);
			}
		} else {
			o = implClass.newInstance();
		}
		return o;
	}

	@SuppressWarnings("unchecked")
	public <T> T create(Class<T> cl) {
		try {
			return (T) createObject(cl);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		}
	}

}

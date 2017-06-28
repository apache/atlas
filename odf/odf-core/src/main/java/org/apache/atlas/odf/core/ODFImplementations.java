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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ODFImplementations {

	Logger logger = Logger.getLogger(ODFImplementations.class.getName());

	private Map<String, String> implementations = new HashMap<String, String>();

	public ODFImplementations(String path, ClassLoader cl) {
		Enumeration<URL> resources;
		try {
			resources = cl.getResources(path);
		} catch (IOException exc) {
			logger.log(Level.WARNING, MessageFormat.format("An error occurred while reading properties ''0'' could not be loaded", path), exc);
			return;
		}
		while (resources.hasMoreElements()) {
			URL url = resources.nextElement();
			try {
				InputStream is = url.openStream();
				if (is != null) {
					Properties props = new Properties();
					props.load(is);
					for (Object key : props.keySet()) {
						String keyString = (String) key;
						try {
							if (implementations.containsKey(key)) {
								String existingClassString = implementations.get(keyString);
								String newClassString = props.getProperty(keyString);
								if (!existingClassString.equals(newClassString)) {
									Class<?> existingClass = cl.loadClass(existingClassString);
									Class<?> newClass = cl.loadClass(newClassString);
									String superClass = null;
									String subClass = null;
									// select the class lowest in the class hierarchy 
									if (existingClass.isAssignableFrom(newClass)) {
										superClass = existingClassString;
										subClass = newClassString;
									} else if (newClass.isAssignableFrom(existingClass)) {
										superClass = newClassString;
										subClass = existingClassString;
									}
									if (superClass != null) {
										logger.log(Level.INFO, "Implementation for interface ''{0}'' was found more than once, using subclass ''{1}'' (found superclass ''{2}'')",
												new Object[] { key, subClass, superClass });
										implementations.put(keyString, subClass);
									} else {
										logger.log(Level.WARNING, "Implementation for interface ''{0}'' was found more than once, using ''{1}''. (Conflict between ''{1}'' and ''{2}'')",
												new Object[] { key, existingClassString, newClassString });
									}
								}
							} else {
								cl.loadClass(props.getProperty(keyString));
								implementations.put(keyString, props.getProperty(keyString));
							}
						} catch (ClassNotFoundException exc) {
							logger.log(Level.SEVERE, "Class found in odf-implementation.properties file could not be loaded", exc);
						}
					}
				}
			} catch (IOException e) {
				logger.log(Level.WARNING, MessageFormat.format("Properties ''0'' could not be loaded", url), e);
			}
		}
	}

	public Map<String, String> getImplementations() {
		return implementations;
	}

}

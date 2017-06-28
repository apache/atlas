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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.settings.KafkaConsumerConfig;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.core.configuration.ConfigContainer;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONObject;

import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;

public class Utils {

	static Logger logger = Logger.getLogger(Utils.class.getName());

	private static final List<Class<? extends Object>> MERGABLE_CLASSES = Arrays.asList(ConfigContainer.class, KafkaConsumerConfig.class, ODFSettings.class, DiscoveryServiceProperties.class);

	public static void mergeODFPOJOs(Object source, Object update) {
		if (!source.getClass().isAssignableFrom(update.getClass())) {
			return;
		}

		Method[] sourceMethods = source.getClass().getDeclaredMethods();

		for (Method getterMethod : sourceMethods) {
			if (getterMethod.getName().startsWith("get") || getterMethod.getName().startsWith("is")) {
				String setterMethodName = getterMethod.getName().replaceFirst("get", "set");
				if (getterMethod.getName().startsWith("is")) {
					setterMethodName = setterMethodName.replaceFirst("is", "set");
				}
				try {
					Method setterMethod = source.getClass().getDeclaredMethod(setterMethodName, getterMethod.getReturnType());
					Object updateValue = getterMethod.invoke(update);
					if (updateValue != null) {
						Object sourceValue = getterMethod.invoke(source);

						if (sourceValue != null && MERGABLE_CLASSES.contains(updateValue.getClass())) {
							//Value is another POJO, must also try merging these instead of overwriting
							mergeODFPOJOs(sourceValue, updateValue);
							setterMethod.invoke(source, sourceValue);
						} else if (sourceValue instanceof Map && updateValue instanceof Map) {
							Map updateJSON = (Map) updateValue;
							Map sourceJSON = (Map) sourceValue;
							for (Object key : updateJSON.keySet()) {
								sourceJSON.put(key, updateJSON.get(key));
							}
							setterMethod.invoke(source, sourceJSON);
						} else {
							setterMethod.invoke(source, updateValue);
						}
					}

				} catch (NoSuchMethodException e) {
					throw new RuntimeException(MessageFormat.format("Objects of type {0}  and {1} could not be merged, no matching method found for {2}!", source.getClass().getName(), update
							.getClass().getName(), getterMethod.getName()), e);
				} catch (SecurityException e) {
					throw new RuntimeException(MessageFormat.format("Objects of type {0}  and {1} could not be merged, method {2} could not be accessed (SecurityException)!", source.getClass()
							.getName(), update.getClass().getName(), setterMethodName), e);
				} catch (IllegalAccessException e) {
					throw new RuntimeException(MessageFormat.format("Objects of type {0}  and {1} could not be merged, method {2} could not be accessed! (IllegalAccessException)", source.getClass()
							.getName(), update.getClass().getName(), getterMethod.getName()), e);
				} catch (IllegalArgumentException e) {
					throw new RuntimeException(MessageFormat.format("Objects of type {0}  and {1} could not be merged, method {2} does not accept the right parameters!", source.getClass().getName(),
							update.getClass().getName(), setterMethodName), e);
				} catch (InvocationTargetException e) {
					e.printStackTrace();
					throw new RuntimeException(MessageFormat.format("Objects of type {0}  and {1} could not be merged, method {2} or {3} could not be invoked!", source.getClass().getName(), update
							.getClass().getName(), getterMethod.getName(), setterMethodName), e);
				}

			}
		}
	}

	public static Properties readConfigProperties(String path) {
		// TODO cache this in static variables, it doesn't change at runtime 
		InputStream is = Utils.class.getClassLoader().getResourceAsStream(path);
		if (is == null) {
			return null;
		}
		Properties props = new Properties();
		try {
			props.load(is);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return props;
	}

	public static void setCurrentTimeAsLastModified(AnalysisRequestTracker tracker) {
		tracker.setLastModified(System.currentTimeMillis());
	}

	public static String getExceptionAsString(Throwable exc) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		exc.printStackTrace(pw);
		String st = sw.toString();
		return st;
	}

	public static String collectionToString(Collection<?> coll, String separator) {
		StringBuffer buf = null;
		for (Object o : coll) {
			if (buf == null) {
				buf = new StringBuffer("[ ");
			} else {
				buf.append(separator);
			}
			buf.append(o.toString());
		}
		buf.append(" ]");
		return buf.toString();
	}

	public static <T> boolean containsOnly(List<T> l, T[] elements) {
		for (T t : l) {
			boolean containsOnlyElements = false;
			for (T el : elements) {
				if (t.equals(el)) {
					containsOnlyElements = true;
					break;
				}
			}
			if (!containsOnlyElements) {
				return false;
			}
		}
		return true;
	}

	public static <T> boolean containsNone(List<T> l, T[] elements) {
		for (T t : l) {
			boolean containsAnyElement = false;
			for (T el : elements) {
				if (t.equals(el)) {
					containsAnyElement = true;
					break;
				}
			}
			if (containsAnyElement) {
				return true;
			}
		}
		return false;
	}

	public static List<String> splitString(String s, char separator) {
		List<String> l = new ArrayList<String>();
		if (s != null) {
			StringTokenizer tok = new StringTokenizer(s, String.valueOf(separator));
			while (tok.hasMoreTokens()) {
				l.add(tok.nextToken());
			}
		}
		return l;
	}

	public static String getInputStreamAsString(InputStream is, String encoding) {
		try {
			final int n = 2048;
			byte[] b = new byte[0];
			byte[] temp = new byte[n];
			int bytesRead;
			while ((bytesRead = is.read(temp)) != -1) {
				byte[] newB = new byte[b.length + bytesRead];
				System.arraycopy(b, 0, newB, 0, b.length);
				System.arraycopy(temp, 0, newB, b.length, bytesRead);
				b = newB;
			}
			String s = new String(b, encoding);
			return s;
		} catch (IOException exc) {
			return getExceptionAsString(exc);
		}
	}

	public static void mergeJSONObjects(JSONObject source, JSONObject target) {
		if (source != null && target != null) {
			target.putAll(source);
		}
	}

	public static <T> T getValue(T value, T defaultValue) {
		if (value == null) {
			return defaultValue;
		}
		return value;
	}

	public static String getSystemPropertyExceptionIfMissing(String propertyName) {
		Environment env = new ODFInternalFactory().create(Environment.class);
		String value = env.getProperty(propertyName);
		if (value == null) {
			String msg = MessageFormat.format("System property ''{0}'' is not set", propertyName);
			logger.log(Level.SEVERE, msg);
			throw new RuntimeException(msg);
		}
		return value;
	}
	
	public static int getIntEnvironmentProperty(String propertyName, int defaultValue) {
		Environment env = new ODFInternalFactory().create(Environment.class);
		String value = env.getProperty(propertyName);
		if (value == null) {
			return defaultValue;
		}
		try {
			return Integer.parseInt(value);
		} catch(NumberFormatException exc) {
			return defaultValue;
		}
	}


	public static void runSystemCommand(String command) {
		logger.log(Level.INFO, "Running system command: " + command);
		try {
			Runtime r = Runtime.getRuntime();
			Process p = r.exec(command);
			p.waitFor();
			BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = b.readLine()) != null) {
				logger.log(Level.INFO, "System command out: " + line);
			}
			b.close();
		} catch(IOException | InterruptedException e) {
			logger.log(Level.INFO, "Error executing system command.", e);
		}
	}
	
	public static ConfigContainer readConfigurationFromClasspath(String jsonFileInClasspath) {
		InputStream is = SettingsManager.class.getClassLoader().getResourceAsStream(jsonFileInClasspath);
		try {
			JSONObject configJSON = new JSONObject(is);
			ConfigContainer config = JSONUtils.fromJSON(configJSON.write(), ConfigContainer.class);
			return config;
		} catch (Exception exc) {
			throw new RuntimeException(exc);
		}
	}

	public static String joinStrings(List<String> l, char separator) {
		String result = null;
		if ((l != null) && !l.isEmpty()) {
			StringBuilder buf = null;
			for (String s : l) {
				if (buf == null) {
					buf = new StringBuilder();
				} else {
					buf.append(separator);
				}
				buf.append(s);
			}
			result = buf.toString();
		}
		return result;
	}
	
	public static String getEnvironmentProperty(String name, String defaultValue) {
		Environment env = new ODFInternalFactory().create(Environment.class);
		String s = env.getProperty(name);
		return s != null ? s : defaultValue;		
	}
	
	public static long getEnvironmentProperty(String name, long defaultValue) {
		Environment env = new ODFInternalFactory().create(Environment.class);
		String s = env.getProperty(name);
		if (s == null) {
			return defaultValue;
		}
		try {
			return Long.parseLong(s);
		} catch(NumberFormatException exc) {
			String msg = MessageFormat.format("Property ''{0}'' could not be converted to an integer", new Object[]{name});
			logger.log(Level.WARNING, msg);
			return defaultValue;
		}
	}
}

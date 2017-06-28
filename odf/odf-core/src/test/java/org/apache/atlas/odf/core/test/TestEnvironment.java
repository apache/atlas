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
package org.apache.atlas.odf.core.test;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInitializer;
import org.apache.atlas.odf.core.ODFInternalFactory;

/**
 * The class can be used to start components required for testing.
 *
 *
 */
public class TestEnvironment {

	static Logger logger = Logger.getLogger(TestEnvironment.class.getName());

	public static String MESSAGING_CLASS = "org.apache.atlas.odf.core.test.messaging.kafka.TestEnvironmentMessagingInitializer";

	public static <T> T createObject(String className, Class<T> clazz) {
		ClassLoader cl = TestEnvironment.class.getClassLoader();
		// messaging
		try {
			Class<?> tei = cl.loadClass(className);
			return (T) tei.newInstance();
		} catch (Exception exc) {
			logger.log(Level.WARNING, "An exception occurred when starting the messaging test environment", exc);
		}
		return null;
	}

	public static void start(String className) {
		TestEnvironmentInitializer initializer = createObject(className, TestEnvironmentInitializer.class);
		if (initializer != null) {
			initializer.start();
		}
	}

	public static void startMessaging() {
		if ("true".equals(new ODFInternalFactory().create(Environment.class).getProperty("odf.dont.start.messaging"))) {
			// do nothing
			logger.info("Messaging test environment not started because environment variable odf.dont.start.messaging is set");
		} else {
			start(MESSAGING_CLASS);
		}
	}

	public static void startAll() {
		startMessaging();
		ODFInitializer.start();
	}

}

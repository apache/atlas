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
package org.apache.atlas.odf.core.runtime;

import java.util.logging.Logger;

import org.apache.atlas.odf.api.spark.SparkServiceExecutor;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.messaging.DiscoveryServiceQueueManager;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.core.store.ODFConfigurationStorage;
import org.apache.atlas.odf.core.test.ODFTestcase;
import org.apache.atlas.odf.core.test.messaging.MockQueueManager;
import org.apache.atlas.odf.core.test.spark.MockSparkServiceExecutor;
import org.apache.atlas.odf.core.test.store.MockConfigurationStorage;

public class ODFFactoryClassesNoMockTest extends ODFTestcase {

	Logger logger = Logger.getLogger(ODFFactoryClassesNoMockTest.class.getName());

	<T> void testFactoryDoesNotCreateInstanceOf(Class<T> interfaceClass, Class<? extends T> mockClass) {
		ODFInternalFactory f = new ODFInternalFactory();
		logger.info("Testing mock class for interface: " + interfaceClass.getName());
		T obj = f.create(interfaceClass);
		logger.info("Factory created object of type " + obj.getClass().getName());
		Assert.assertFalse(mockClass.isInstance(obj));
	}

	@Test
	public void testNoMockClasses() {
		logger.info("Testing that no mock classes are used");

		testFactoryDoesNotCreateInstanceOf(ODFConfigurationStorage.class, MockConfigurationStorage.class);
		testFactoryDoesNotCreateInstanceOf(DiscoveryServiceQueueManager.class, MockQueueManager.class);
		testFactoryDoesNotCreateInstanceOf(SparkServiceExecutor.class, MockSparkServiceExecutor.class);
	}
}

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

import static org.junit.Assert.assertNotNull;

import java.util.logging.Logger;

import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.controlcenter.AnalysisRequestTrackerStore;
import org.apache.atlas.odf.core.controlcenter.ExecutorServiceFactory;
import org.apache.atlas.odf.core.messaging.DiscoveryServiceQueueManager;
import org.junit.Test;

import org.apache.atlas.odf.core.controlcenter.ControlCenter;
import org.apache.atlas.odf.core.controlcenter.ThreadManager;
import org.apache.atlas.odf.core.notification.NotificationManager;

public class ODFInternalFactoryTest extends TimerTestBase {

	Logger logger = ODFTestLogger.get();

	@Test
	public void testFactoryInstantiations() throws Exception {
		try {
			ODFInternalFactory factory = new ODFInternalFactory();
			Class<?>[] interfaces = new Class<?>[] { //
			DiscoveryServiceQueueManager.class, //
					ControlCenter.class, //
					AnalysisRequestTrackerStore.class, //
					ThreadManager.class, //
					ExecutorServiceFactory.class, //
					NotificationManager.class, //
					DiscoveryServiceQueueManager.class, //
			};
			for (Class<?> cl : interfaces) {
				Object o = factory.create(cl);
				assertNotNull(o);
				logger.info("Object created for class " + cl.getName() + ": " + o.getClass().getName());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

}

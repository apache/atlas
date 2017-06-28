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

import java.util.logging.Logger;

import org.apache.atlas.odf.api.engine.SystemHealth;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.engine.EngineManager;

/**
 * All JUnit test cases that require proper Kafka setup should inherit from this class.
 * 
 *
 */
public class ODFTestBase extends TimerTestBase {

	static protected Logger log = ODFTestLogger.get();
	@Test
	public void testHealth() {
		testHealth(true);
	}

	private void testHealth(boolean kafkaRunning) {
		log.info("Starting health check...");
		EngineManager engineManager = new ODFFactory().create().getEngineManager();
		SystemHealth health = engineManager.checkHealthStatus();
		if (!kafkaRunning) {
			Assert.assertEquals(SystemHealth.HealthStatus.ERROR, health.getStatus());
		} else {
			Assert.assertEquals(SystemHealth.HealthStatus.OK, health.getStatus());
		}
		log.info("Health check finished");
	}

	@BeforeClass
	public static void startup() throws Exception {
		TestEnvironment.startAll();
	}

	@Before
	public void setup() throws Exception {
		testHealth(true);
	}

	@After
	public void tearDown() throws Exception {
		testHealth(true);
	}
}

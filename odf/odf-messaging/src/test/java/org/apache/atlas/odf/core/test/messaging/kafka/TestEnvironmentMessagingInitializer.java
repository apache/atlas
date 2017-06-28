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
package org.apache.atlas.odf.core.test.messaging.kafka;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.core.test.TestEnvironmentInitializer;

public class TestEnvironmentMessagingInitializer implements TestEnvironmentInitializer {

	public TestEnvironmentMessagingInitializer() {
	}
	
	public void start() {
		Logger logger = Logger.getLogger(TestEnvironmentMessagingInitializer.class.getName());
		try {
			logger.info("Starting Test-Kafka during initialization...");
			TestKafkaStarter starter = new TestKafkaStarter();
			starter.startKafka();
			logger.info("Test-Kafka initialized");
		} catch (Exception exc) {
			logger.log(Level.INFO, "Exception occurred while starting test kafka", exc);
			throw new RuntimeException(exc);
		}
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getName() {
		return "Kafka1001";
	}
}

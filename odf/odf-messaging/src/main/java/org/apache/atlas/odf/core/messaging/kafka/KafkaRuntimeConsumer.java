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
package org.apache.atlas.odf.core.messaging.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.core.controlcenter.ODFRunnable;
import org.apache.atlas.odf.core.controlcenter.QueueMessageProcessor;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntime;

/**
 * This consumer is started for a certain runtime and starts a KafkaQueueConsumer if
 * the runtime is available. 
 * 
 *
 */
public class KafkaRuntimeConsumer implements ODFRunnable {

	Logger logger = Logger.getLogger(KafkaRuntimeConsumer.class.getName());

	private ServiceRuntime runtime;
	private boolean isShutdown = false;
	private ExecutorService executorService = null;
	private KafkaQueueConsumer kafkaQueueConsumer = null;

	private String topic;
	private Properties config;
	private QueueMessageProcessor processor;

	private KafkaQueueConsumer.ConsumptionCallback callback = new KafkaQueueConsumer.ConsumptionCallback() {
		@Override
		public boolean stopConsumption() {
			return isShutdown || (runtime.getWaitTimeUntilAvailable() > 0);
		}
	};

	public KafkaRuntimeConsumer(ServiceRuntime runtime, String topicName, Properties config, QueueMessageProcessor processor) {
		this.runtime = runtime;
		this.processor = processor;
		this.topic = topicName;
		this.config = config;
	}

	@Override
	public void run() {
		logger.log(Level.INFO, "Starting runtime consumer for topic ''{0}''", topic);
		while (!isShutdown) {
			long waitTime = runtime.getWaitTimeUntilAvailable();
			if (waitTime <= 0) {
				logger.log(Level.INFO, "Starting Kafka consumer for topic ''{0}''", topic);
				kafkaQueueConsumer = new KafkaQueueConsumer(topic, config, processor, callback);
				kafkaQueueConsumer.setExecutorService(executorService);
				// run consumer synchronously
				kafkaQueueConsumer.run();
				logger.log(Level.INFO, "Kafka consumer for topic ''{0}'' is finished", topic);

				// if we are here, this means that the consumer was cancelled
				// either directly or (more likely) through the Consumption callback 
				kafkaQueueConsumer = null;
			} else {
				try {
					logger.log(Level.FINER, "Runtime ''{0}'' is not available, waiting for ''{1}''ms", new Object[]{runtime.getName(), waitTime });
					Thread.sleep(waitTime);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
		logger.log(Level.INFO, "Kafka runtime consumer for topic ''{0}'' has shut down", topic);
	}

	@Override
	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}

	@Override
	public void cancel() {
		isShutdown = true;
		if (kafkaQueueConsumer != null) {
			kafkaQueueConsumer.cancel();
		}
	}

	@Override
	public boolean isReady() {
		return true;
	}

}

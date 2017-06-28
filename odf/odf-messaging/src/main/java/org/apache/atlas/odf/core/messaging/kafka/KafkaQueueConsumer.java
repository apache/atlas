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

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.controlcenter.ODFRunnable;
import org.apache.atlas.odf.core.controlcenter.QueueMessageProcessor;
import org.apache.atlas.odf.core.messaging.MessageEncryption;

import kafka.consumer.ConsumerTimeoutException;

public class KafkaQueueConsumer implements ODFRunnable {
	private Logger logger = Logger.getLogger(KafkaQueueConsumer.class.getName());
	final static int POLLING_DURATION_MS = 100;
	public static final int MAX_PROCESSING_EXCEPTIONS = 3;
	public final static int MAX_CONSUMPTION_EXCEPTIONS = 5;
	
	public static interface ConsumptionCallback {
		boolean stopConsumption();
	}

	private boolean ready = false;

	private String topic;
	private KafkaConsumer<String, String> kafkaConsumer;
	private Properties config;
	private boolean isShutdown = false;
	private ExecutorService executorService;
	private QueueMessageProcessor requestConsumer;
	private int consumptionExceptionCount = 0;
	private ConsumptionCallback consumptionCallback;

	public KafkaQueueConsumer(String topicName, Properties config, QueueMessageProcessor requestConsumer) {
		this(topicName, config, requestConsumer, null);
	}
	
	public KafkaQueueConsumer(String topicName, Properties config, QueueMessageProcessor requestConsumer, ConsumptionCallback consumptionCallback) {
		this.topic = topicName;
		this.config = config;
		this.requestConsumer = requestConsumer;
		this.consumptionCallback = consumptionCallback;
		if (this.consumptionCallback == null) {
			this.consumptionCallback = new ConsumptionCallback() {

				@Override
				public boolean stopConsumption() {
					// default: never stop
					return false;
				}
				
			};
		}
	}

	public void run() {
		final String groupId = this.config.getProperty("group.id");
		while (consumptionExceptionCount < MAX_CONSUMPTION_EXCEPTIONS && !isShutdown) {
			try {
				logger.info("Starting consumption for " + groupId);
				startConsumption();
			} catch (RuntimeException ex) {
				if (ex.getCause() instanceof WakeupException) {
					isShutdown = true;
				} else {
					consumptionExceptionCount++;
					logger.log(Level.WARNING, "Caught exception in KafkaQueueConsumer " + groupId + ", restarting consumption!", ex);
				}
				if (this.kafkaConsumer != null) {
					this.kafkaConsumer.close();
					this.kafkaConsumer = null;
				}
			} catch (Exception e) {
				consumptionExceptionCount++;
				logger.log(Level.WARNING, "Caught exception in KafkaQueueConsumer " + groupId + ", restarting consumption!", e);
				if (this.kafkaConsumer != null) {
					this.kafkaConsumer.close();
					this.kafkaConsumer = null;
				}
			}
		}
		logger.info("Enough consumption for " + groupId);
		this.ready = false;
		this.cancel();
	}

	private void startConsumption() {
		if (this.consumptionCallback.stopConsumption()) {
			return;
		}
		Exception caughtException = null;
		final String logPrefix = this + " consumer: [" + this.requestConsumer.getClass().getSimpleName() + "], on " + topic + ": ";
		try {
			if (this.kafkaConsumer == null) {
				logger.fine(logPrefix + " create new consumer for topic " + topic);
				try {
					this.kafkaConsumer = new KafkaConsumer<String, String>(config);
					kafkaConsumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {

						@Override
						public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
							logger.fine(logPrefix + " partitions revoked " + topic + " new partitions: " + partitions.size());
						}

						@Override
						public void onPartitionsAssigned(Collection<TopicPartition> partitions) {						
							logger.finer(logPrefix + " partitions assigned " + topic + " , new partitions: " + partitions.size());
							logger.info(logPrefix + "consumer is ready with " + partitions.size() + " partitions assigned");
							ready = true;
						}
					});
				} catch (ZkTimeoutException zkte) {
					String zkHosts = config.getProperty("zookeeper.connect");
					logger.log(Level.SEVERE, logPrefix + " Could not connect to the Zookeeper instance at ''{0}''. Please ensure that Zookeeper is running", zkHosts);
					throw zkte;
				}
			}
			logger.log(Level.INFO, logPrefix + " Consumer " + "''{1}'' is now listening on ODF queue ''{0}'' with configuration {2}", new Object[] { topic, requestConsumer, config });
			MessageEncryption msgEncryption = new ODFInternalFactory().create(MessageEncryption.class);
			while (!Thread.interrupted() && !isShutdown && kafkaConsumer != null) {
				if (this.consumptionCallback.stopConsumption()) {
					isShutdown = true;
					break;
				}
				ConsumerRecords<String, String> records = kafkaConsumer.poll(POLLING_DURATION_MS);
				kafkaConsumer.commitSync(); // commit offset immediately to avoid timeouts for long running processors
				for (TopicPartition partition : kafkaConsumer.assignment()) {
					List<ConsumerRecord<String, String>> polledRecords = records.records(partition);
					//		logger.log(Level.FINEST, logPrefix + "Polling finished got {0} results, continue processing? {1}", new Object[] { polledRecords.size(), continueProcessing });
					if (!polledRecords.isEmpty()) {
						logger.fine(polledRecords.get(0).value() + " offset: " + polledRecords.get(0).offset());
					}

					for (int no = 0; no < polledRecords.size(); no++) {
						ConsumerRecord<String, String> record = polledRecords.get(no);
						String s = record.value();
						logger.log(Level.FINEST, logPrefix + "Decrypting message {0}", s);
						try {
							s = msgEncryption.decrypt(s);
						} catch (Exception exc) {
							logger.log(Level.WARNING, "Message could not be decrypted, ignoring it", exc);
							s = null;
						}
						if (s != null) {
							logger.log(Level.FINEST, logPrefix + "Sending message to consumer ''{0}''", s);
							int exceptionCount = 0;
							boolean processedSuccessfully = false;
							while (exceptionCount < MAX_PROCESSING_EXCEPTIONS && !processedSuccessfully) {
								try {
									exceptionCount++;
									this.requestConsumer.process(executorService, s, record.partition(), record.offset());
									processedSuccessfully = true;
								} catch (Exception ex) {
									logger.warning("Exception " + exceptionCount + " caught processing message!");
								}
							}
						}
					}
				}
			}
		} catch (ConsumerTimeoutException e) {
			String msg = MessageFormat.format(" Caught timeout on queue ''{0}''", topic);
			logger.log(Level.WARNING, logPrefix + msg, e);
			caughtException = e;
		} catch (Exception exc) {
			String msg = MessageFormat.format(" Caught exception on queue ''{0}''", topic);
			logger.log(Level.WARNING, logPrefix + msg, exc);
			caughtException = exc;
		} finally {
			if (kafkaConsumer != null) {
				logger.log(Level.FINE, logPrefix + "Closing consumer " + " on topic ''{0}''", topic);
				kafkaConsumer.close();
				logger.log(Level.FINE, logPrefix + "Closed consumer " + " on topic ''{0}''", topic);
				kafkaConsumer = null;
			}
		}
		logger.log(Level.INFO, logPrefix + "Finished consumer on topic ''{0}''", topic);
		if (caughtException != null) {
			caughtException.printStackTrace();
			throw new RuntimeException(caughtException);
		}
	}

	public void cancel() {
		logger.log(Level.INFO, "Shutting down consumer on topic ''{0}''", topic);
		if (this.kafkaConsumer != null) {
			this.kafkaConsumer.wakeup();
		}
		isShutdown = true;
	}

	public boolean isShutdown() {
		return isShutdown;
	}

	@Override
	public void setExecutorService(ExecutorService service) {
		this.executorService = service;
	}

	@Override
	public boolean isReady() {
		return ready;
	}

}

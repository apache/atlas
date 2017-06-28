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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.atlas.odf.api.engine.ThreadStatus;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInitializer;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.controlcenter.QueueMessageProcessor;
import org.apache.atlas.odf.core.messaging.kafka.KafkaMonitor;
import org.apache.atlas.odf.core.messaging.kafka.KafkaQueueConsumer;
import org.apache.atlas.odf.core.messaging.kafka.KafkaQueueManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.controlcenter.ThreadManager;
import org.apache.atlas.odf.core.controlcenter.ThreadManager.ThreadStartupResult;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.core.test.ODFTestcase;

import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class MultiPartitionConsumerTest extends ODFTestcase {
	static Logger logger = ODFTestLogger.get();
	final static String topicName = "my_dummy_test_topic" + UUID.randomUUID().toString();
	static String zookeeperHost = new ODFInternalFactory().create(Environment.class).getZookeeperConnectString();
	static final int PARTITION_COUNT = 3;
	private static final int MSG_PER_PARTITION = 5;
	private final ThreadManager threadManager = new ODFInternalFactory().create(ThreadManager.class);

	@BeforeClass
	public static void setupTopic() {
		ZkClient zkClient = null;
		try {
			zkClient = new ZkClient(zookeeperHost, 5000, 5000, ZKStringSerializer$.MODULE$);
			logger.log(Level.FINEST, "Checking if topic ''{0}'' already exists", topicName);
			// using partition size 1 and replication size 1, no special
			// per-topic config needed
			logger.log(Level.FINE, "Topic ''{0}'' does not exist, creating it", topicName);
			//FIXME zkUtils isSecure parameter? Only with SSL! --> parse zkhosts?
			AdminUtils.createTopic(new ZkUtils(zkClient, new ZkConnection(zookeeperHost), false), topicName, PARTITION_COUNT, 1, new Properties(), KafkaQueueManager.DEFAULT_RACK_AWARE_MODE);
			logger.log(Level.FINE, "Topic ''{0}'' created", topicName);
		} catch (TopicExistsException ex) {
			logger.log(Level.FINE, "Topic ''{0}'' already exists.", topicName);
		} catch (ZkTimeoutException zkte) {
			logger.log(Level.SEVERE, "Could not connect to the Zookeeper instance at ''{0}''. Please ensure that Zookeeper is running", zookeeperHost);
		} finally {
			if (zkClient != null) {
				zkClient.close();
			}
		}
	}

	@After
	public void cleanupConsumers() {
		logger.info("Cleaning up consumers...");
		logger.info("----------------------------------  Stopping ODF...");
		ODFInitializer.stop();
		logger.info("----------------------------------  Starting ODF...");
		ODFInitializer.start();
		logger.info("----------------------------------  ODF started.");
	}

	@Test
	public void testMultiPartitionDelayedConsumption() throws InterruptedException, ExecutionException {
		Properties kafkaConsumerProperties = getConsumerProps();
		final List<String> consumedMsgs = new ArrayList<String>();
		List<ThreadStartupResult> startupList = new ArrayList<ThreadStartupResult>();

		final String threadPrefix = "TEST_CONSUMER_RETRY_RUNNING_";
		final int processingDelay = 2000;
		for (int no = 0; no < PARTITION_COUNT; no++) {
			final int currentThread = no;
			final QueueMessageProcessor requestConsumer = new QueueMessageProcessor() {

				@Override
				public void process(ExecutorService executorService, String msg, int partition, long msgOffset) {
					try {
						Thread.sleep(processingDelay);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					consumedMsgs.add(msg);
					logger.info("process " + msg + " in thread " + currentThread);
				}
			};

			KafkaQueueConsumer cnsmr = new KafkaQueueConsumer(topicName, kafkaConsumerProperties, requestConsumer);

			final String consumerThread = threadPrefix + no;
			final ThreadStartupResult startUnmanagedThread = threadManager.startUnmanagedThread(consumerThread, cnsmr);
			startupList.add(startUnmanagedThread);
		}
		try {
			threadManager.waitForThreadsToBeReady(30000, startupList);
			for (int no = 0; no < PARTITION_COUNT; no++) {
				for (int msgNo = 0; msgNo < MSG_PER_PARTITION; msgNo++) {
					sendMsg("Partition " + no + " msg " + msgNo);
				}
			}

			int totalWait = 0;
			while (totalWait < PARTITION_COUNT * MSG_PER_PARTITION * processingDelay + 10000 && consumedMsgs.size() < PARTITION_COUNT * MSG_PER_PARTITION) {
				Thread.sleep(2000);
				totalWait += 2000;
			}

			logger.info("Done with all messages after " + totalWait);

			Assert.assertEquals(PARTITION_COUNT * MSG_PER_PARTITION, consumedMsgs.size());

			for (int no = 0; no < PARTITION_COUNT; no++) {
				final ThreadStatus.ThreadState stateOfUnmanagedThread = threadManager.getStateOfUnmanagedThread(threadPrefix + no);
				Assert.assertEquals(ThreadStatus.ThreadState.RUNNING, stateOfUnmanagedThread);
			}
		} catch (TimeoutException e) {
			Assert.fail("Consumer could not be started on time");
		}
	}

	@Test
	public void testMultiPartitionConsumption() throws InterruptedException, ExecutionException {
		Properties kafkaConsumerProperties = getConsumerProps();
		final List<String> consumedMsgs = new ArrayList<String>();
		List<ThreadStartupResult> startupList = new ArrayList<ThreadStartupResult>();

		final String threadPrefix = "TEST_CONSUMER_RETRY_RUNNING_";
		for (int no = 0; no < PARTITION_COUNT; no++) {
			final int currentThread = no;
			final QueueMessageProcessor requestConsumer = new QueueMessageProcessor() {

				@Override
				public void process(ExecutorService executorService, String msg, int partition, long msgOffset) {
					consumedMsgs.add(msg);
					logger.info("process " + msg + " in thread " + currentThread);
				}
			};

			KafkaQueueConsumer cnsmr = new KafkaQueueConsumer(topicName, kafkaConsumerProperties, requestConsumer);

			final String consumerThread = threadPrefix + no;
			final ThreadStartupResult startUnmanagedThread = threadManager.startUnmanagedThread(consumerThread, cnsmr);
			startupList.add(startUnmanagedThread);
		}
		try {
			threadManager.waitForThreadsToBeReady(30000, startupList);
			for (int no = 0; no < PARTITION_COUNT; no++) {
				for (int msgNo = 0; msgNo < MSG_PER_PARTITION; msgNo++) {
					sendMsg("Partition " + no + " msg " + msgNo);
				}
			}

			int totalWait = 0;
			boolean done = false;
			while (totalWait < 30 && !done) {
				if (consumedMsgs.size() == PARTITION_COUNT * MSG_PER_PARTITION) {
					done = true;
				}
				totalWait++;
				Thread.sleep(500);
			}

			Assert.assertEquals(PARTITION_COUNT * MSG_PER_PARTITION, consumedMsgs.size());

			for (int no = 0; no < PARTITION_COUNT; no++) {
				final ThreadStatus.ThreadState stateOfUnmanagedThread = threadManager.getStateOfUnmanagedThread(threadPrefix + no);
				Assert.assertEquals(ThreadStatus.ThreadState.RUNNING, stateOfUnmanagedThread);
			}
		} catch (TimeoutException e) {
			Assert.fail("Consumer could not be started on time");
		}
	}

	@Test
	public void testMultiPartitionExceptionAndRetryDuringProcessing() throws InterruptedException, ExecutionException {
		Properties kafkaConsumerProperties = getConsumerProps();
		final List<String> consumedMsgs = new ArrayList<String>();
		List<ThreadStartupResult> startupList = new ArrayList<ThreadStartupResult>();

		final String threadPrefix = "TEST_CONSUMER_RETRY_RUNNING_";
		for (int no = 0; no < PARTITION_COUNT; no++) {
			final int currentThread = no;
			final QueueMessageProcessor requestConsumer = new QueueMessageProcessor() {

				private int excCount = 0;

				@Override
				public void process(ExecutorService executorService, String msg, int partition, long msgOffset) {
					if (excCount < KafkaQueueConsumer.MAX_PROCESSING_EXCEPTIONS - 1) {
						excCount++;
						logger.info("Throw exception " + excCount + " on consumer " + currentThread);
						throw new RuntimeException("Forced error on consumer");
					}
					consumedMsgs.add(msg);
					logger.info("process " + msg + " in thread " + currentThread);
				}
			};

			KafkaQueueConsumer cnsmr = new KafkaQueueConsumer(topicName, kafkaConsumerProperties, requestConsumer);

			final String consumerThread = threadPrefix + no;
			final ThreadStartupResult startUnmanagedThread = threadManager.startUnmanagedThread(consumerThread, cnsmr);
			startupList.add(startUnmanagedThread);
		}
		try {
			threadManager.waitForThreadsToBeReady(30000, startupList);
			for (int no = 0; no < PARTITION_COUNT; no++) {
				for (int msgNo = 0; msgNo < MSG_PER_PARTITION; msgNo++) {
					sendMsg("Partition " + no + " msg " + msgNo);
				}
			}

			int totalWait = 0;
			boolean done = false;
			while (totalWait < 30 && !done) {
				if (consumedMsgs.size() == PARTITION_COUNT * MSG_PER_PARTITION) {
					done = true;
				}
				totalWait++;
				Thread.sleep(500);
			}
			Assert.assertEquals(PARTITION_COUNT * MSG_PER_PARTITION, consumedMsgs.size());

			for (int no = 0; no < PARTITION_COUNT; no++) {
				final ThreadStatus.ThreadState stateOfUnmanagedThread = threadManager.getStateOfUnmanagedThread(threadPrefix + no);
				Assert.assertEquals(ThreadStatus.ThreadState.RUNNING, stateOfUnmanagedThread);
			}
		} catch (TimeoutException e) {
			Assert.fail("Consumer could not be started on time");
		}
	}

	private Properties getConsumerProps() {
		SettingsManager odfConfig = new ODFFactory().create().getSettingsManager();
		Properties kafkaConsumerProperties = odfConfig.getKafkaConsumerProperties();
		final String groupId = "retrying-dummy-consumer";
		kafkaConsumerProperties.put("group.id", groupId);
		kafkaConsumerProperties.put("zookeeper.connect", zookeeperHost);
		final Iterator<String> brokers = new ODFInternalFactory().create(KafkaMonitor.class).getBrokers(zookeeperHost).iterator();
		StringBuilder brokersString = new StringBuilder();
		while (brokers.hasNext()) {
			brokersString.append(brokers.next());
			if (brokers.hasNext()) {
				brokersString.append(",");
			}
		}
		kafkaConsumerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersString.toString());
		kafkaConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

		return kafkaConsumerProperties;
	}

	void sendMsg(String msg) throws InterruptedException, ExecutionException, TimeoutException {
		SettingsManager odfConfig = new ODFFactory().create().getSettingsManager();
		Properties props = odfConfig.getKafkaProducerProperties();
		final Iterator<String> brokers = new ODFInternalFactory().create(KafkaMonitor.class).getBrokers(zookeeperHost).iterator();
		StringBuilder brokersString = new StringBuilder();
		while (brokers.hasNext()) {
			brokersString.append(brokers.next());
			if (brokers.hasNext()) {
				brokersString.append(",");
			}
		}
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersString.toString());
		//Should we use a custom partitioner? we could try to involve consumer offsets and always put on "emptiest" partition
		//props.put("partitioner.class", TestMessagePartitioner.class);

		final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, UUID.randomUUID().toString(), msg);
		producer.send(producerRecord).get(3000, TimeUnit.MILLISECONDS);
		producer.close();
	}

}

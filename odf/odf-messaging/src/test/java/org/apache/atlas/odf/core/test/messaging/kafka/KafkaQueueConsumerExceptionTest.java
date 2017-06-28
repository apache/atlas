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
import java.util.Arrays;
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
import org.apache.atlas.odf.core.messaging.kafka.KafkaQueueConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.atlas.odf.api.engine.ThreadStatus.ThreadState;
import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.controlcenter.QueueMessageProcessor;
import org.apache.atlas.odf.core.controlcenter.ThreadManager;
import org.apache.atlas.odf.core.messaging.kafka.KafkaMonitor;
import org.apache.atlas.odf.core.messaging.kafka.KafkaQueueManager;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.core.test.ODFTestcase;

import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class KafkaQueueConsumerExceptionTest extends ODFTestcase {
	static Logger logger = ODFTestLogger.get();
	static final String topicName = "my_dummy_test_topic";
	static String zookeeperHost = new ODFInternalFactory().create(Environment.class).getZookeeperConnectString();

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
			AdminUtils.createTopic(new ZkUtils(zkClient, new ZkConnection(zookeeperHost), false), topicName, 1, 1, new Properties(), KafkaQueueManager.DEFAULT_RACK_AWARE_MODE);
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

	@Test
	public void testExceptionAndRetryDuringProcessing() throws InterruptedException, ExecutionException, TimeoutException {
		final ODFInternalFactory odfFactory = new ODFInternalFactory();
		final String groupId = "retrying-exception-dummy-consumer";
		Properties kafkaConsumerProperties = new KafkaQueueManager().getConsumerConfigProperties(groupId, true);
		kafkaConsumerProperties.put("group.id", groupId);
		final List<String> consumedMsgs1 = new ArrayList<String>();
		KafkaQueueConsumer cnsmr = new KafkaQueueConsumer(topicName, kafkaConsumerProperties, new QueueMessageProcessor() {

			@Override
			public void process(ExecutorService executorService, String msg, int partition, long offset) {
				consumedMsgs1.add(msg);
				logger.info("retry_consumer process " + msg + " throw exception and try again");
				throw new RuntimeException("Oops!");
			}
		});

		final ThreadManager threadManager = odfFactory.create(ThreadManager.class);
		final String consumerThread = "TEST_CONSUMER_RETRY_RUNNING";
		threadManager.waitForThreadsToBeReady(10000, Arrays.asList(threadManager.startUnmanagedThread(consumerThread, cnsmr)));

		sendMsg("TEST_MSG");
		sendMsg("TEST_MSG2");

		Thread.sleep(2000);

		Assert.assertEquals(2 * KafkaQueueConsumer.MAX_PROCESSING_EXCEPTIONS, consumedMsgs1.size());

		final ThreadState stateOfUnmanagedThread = threadManager.getStateOfUnmanagedThread(consumerThread);
		Assert.assertEquals(ThreadState.RUNNING, stateOfUnmanagedThread);
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

		final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, UUID.randomUUID().toString(), msg);
		producer.send(producerRecord).get(3000, TimeUnit.MILLISECONDS);
		producer.close();
	}

}

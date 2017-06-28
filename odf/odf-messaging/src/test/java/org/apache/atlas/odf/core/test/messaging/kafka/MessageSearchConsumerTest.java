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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.atlas.odf.core.messaging.kafka.MessageSearchConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.controlcenter.ThreadManager;
import org.apache.atlas.odf.api.engine.PartitionOffsetInfo;
import org.apache.atlas.odf.core.messaging.kafka.KafkaMonitor;
import org.apache.atlas.odf.core.messaging.kafka.KafkaQueueManager;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.core.test.ODFTestBase;
import org.apache.atlas.odf.core.test.ODFTestLogger;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class MessageSearchConsumerTest extends ODFTestBase {
	private static final String TEST_SEARCH_STRING = "TEST_STRING_" + UUID.randomUUID().toString();
	private static final String TEST_SEARCH_FAILURE_STRING = "TEST_FAILURE_STRING";
	static Logger logger = ODFTestLogger.get();
	final static String topicName = "MessageSearchConsumerTest" + UUID.randomUUID().toString();
	private static final int PERFORMANCE_MSG_COUNT = 1000;
	static String zookeeperHost = new ODFInternalFactory().create(Environment.class).getZookeeperConnectString();
	private KafkaProducer<String, String> producer;

	@BeforeClass
	public static void createTopc() {
		ZkClient zkClient = new ZkClient(zookeeperHost, 5000, 5000, ZKStringSerializer$.MODULE$);
		ZkUtils utils = new ZkUtils(zkClient, new ZkConnection(zookeeperHost), false);
		if (!AdminUtils.topicExists(utils, topicName)) {
			AdminUtils.createTopic(utils, topicName, 2, 1, new Properties(), KafkaQueueManager.DEFAULT_RACK_AWARE_MODE);
		}
	}

	@Test
	public void testMsgSearchPerformance() throws InterruptedException, ExecutionException, TimeoutException {
		logger.info("Producing msgs");
		for (int no = 0; no < PERFORMANCE_MSG_COUNT; no++) {
			sendMsg("DUMMY_MSG" + no);
		}
		sendMsg(TEST_SEARCH_STRING);
		logger.info("Done producing ...");
		Thread.sleep(200);

		final ThreadManager threadManager = new ODFInternalFactory().create(ThreadManager.class);
		final CountDownLatch searchLatch = new CountDownLatch(1);
		threadManager.startUnmanagedThread(UUID.randomUUID().toString() + "_searchThread", new MessageSearchConsumer(topicName, new MessageSearchConsumer.SearchCompletedCallback() {

			@Override
			public void onDoneSearching(Map<String, PartitionOffsetInfo> msgPositionMap) {
				logger.info("Done searching " + msgPositionMap.get(TEST_SEARCH_STRING).getOffset());
				Assert.assertTrue(msgPositionMap.get(TEST_SEARCH_STRING).getOffset() > -1);
				searchLatch.countDown();
			}
		}, Arrays.asList(TEST_SEARCH_STRING)));

		boolean await = searchLatch.await(5, TimeUnit.SECONDS);
		if (await) {
			logger.info("Messages searched in time");
		} else {
			logger.warning("Couldnt finish search in time");
		}

		final CountDownLatch failureSearchLatch = new CountDownLatch(1);
		threadManager.startUnmanagedThread(UUID.randomUUID().toString() + "_searchThread", new MessageSearchConsumer(topicName, new MessageSearchConsumer.SearchCompletedCallback() {

			@Override
			public void onDoneSearching(Map<String, PartitionOffsetInfo> msgPositionMap) {
				logger.info("Done searching " + msgPositionMap.toString());
				Assert.assertFalse(msgPositionMap.containsKey(TEST_SEARCH_FAILURE_STRING));
				failureSearchLatch.countDown();
			}
		}, Arrays.asList(TEST_SEARCH_FAILURE_STRING)));

		await = searchLatch.await(5, TimeUnit.SECONDS);
		if (await) {
			logger.info("Messages searched in time");
		} else {
			logger.warning("Couldnt finish search in time");
		}
	}

	@Test
	public void testMsgSearchSuccessAndFailure() throws InterruptedException, ExecutionException, TimeoutException {
		sendMsg(TEST_SEARCH_STRING);

		Thread.sleep(200);

		final ThreadManager threadManager = new ODFInternalFactory().create(ThreadManager.class);
		final CountDownLatch searchLatch = new CountDownLatch(1);
		threadManager.startUnmanagedThread(UUID.randomUUID().toString() + "_searchThread", new MessageSearchConsumer(topicName, new MessageSearchConsumer.SearchCompletedCallback() {

			@Override
			public void onDoneSearching(Map<String, PartitionOffsetInfo> msgPositionMap) {
				logger.info("Done searching " + msgPositionMap.get(TEST_SEARCH_STRING).getOffset());
				Assert.assertTrue(msgPositionMap.get(TEST_SEARCH_STRING).getOffset() > -1);
				searchLatch.countDown();
			}
		}, Arrays.asList(TEST_SEARCH_STRING)));

		boolean await = searchLatch.await(5, TimeUnit.SECONDS);
		if (await) {
			logger.info("Messages searched in time");
		} else {
			logger.warning("Couldnt finish search in time");
		}

		final CountDownLatch failureSearchLatch = new CountDownLatch(1);
		threadManager.startUnmanagedThread(UUID.randomUUID().toString() + "_searchThread", new MessageSearchConsumer(topicName, new MessageSearchConsumer.SearchCompletedCallback() {

			@Override
			public void onDoneSearching(Map<String, PartitionOffsetInfo> msgPositionMap) {
				logger.info("Done searching " + msgPositionMap);
				Assert.assertFalse(msgPositionMap.containsKey(TEST_SEARCH_FAILURE_STRING));
				failureSearchLatch.countDown();
			}
		}, Arrays.asList(TEST_SEARCH_FAILURE_STRING)));

		await = searchLatch.await(5, TimeUnit.SECONDS);
		if (await) {
			logger.info("Messages searched in time");
		} else {
			logger.warning("Couldnt finish search in time");
		}
	}

	void sendMsg(String msg) throws InterruptedException, ExecutionException, TimeoutException {
		final KafkaProducer<String, String> producer = getProducer();
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, UUID.randomUUID().toString(), msg);
		producer.send(producerRecord).get(15000, TimeUnit.MILLISECONDS);
	}

	private KafkaProducer<String, String> getProducer() {
		if (this.producer == null) {
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
			producer = new KafkaProducer<String, String>(props);
		}
		return producer;
	}

	@After
	public void closeProducer() {
		if (getProducer() != null) {
			getProducer().close();
		}
	}
}

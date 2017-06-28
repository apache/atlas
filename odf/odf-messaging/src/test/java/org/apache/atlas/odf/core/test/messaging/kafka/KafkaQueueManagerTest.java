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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.settings.MessagingConfiguration;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.core.messaging.kafka.KafkaQueueConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.wink.json4j.JSONException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackerStatus.STATUS;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.engine.ThreadStatus.ThreadState;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.controlcenter.AnalysisRequestTrackerStore;
import org.apache.atlas.odf.core.controlcenter.DefaultStatusQueueStore;
import org.apache.atlas.odf.core.controlcenter.DefaultThreadManager;
import org.apache.atlas.odf.core.controlcenter.QueueMessageProcessor;
import org.apache.atlas.odf.core.controlcenter.StatusQueueEntry;
import org.apache.atlas.odf.core.controlcenter.ThreadManager.ThreadStartupResult;
import org.apache.atlas.odf.core.controlcenter.TrackerUtil;
import org.apache.atlas.odf.core.messaging.kafka.KafkaMonitor;
import org.apache.atlas.odf.core.messaging.kafka.KafkaProducerManager;
import org.apache.atlas.odf.core.messaging.kafka.KafkaQueueManager;
import org.apache.atlas.odf.core.test.ODFTestBase;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.json.JSONUtils;

public class KafkaQueueManagerTest extends ODFTestBase {

	private static Long origRetention;
	Logger logger = ODFTestLogger.get();
	String zookeeperConnectString = new ODFInternalFactory().create(Environment.class).getZookeeperConnectString();

	@BeforeClass
	public static void setupTrackerRetention() throws ValidationException {
		SettingsManager settingsManager = new ODFFactory().create().getSettingsManager();
		//SETUP RETENTION TO KEEP TRACKERS!!!
		final MessagingConfiguration messagingConfiguration = settingsManager.getODFSettings().getMessagingConfiguration();
		origRetention = messagingConfiguration.getAnalysisRequestRetentionMs();
		messagingConfiguration.setAnalysisRequestRetentionMs(120000000l);

		ODFTestLogger.get().info("Set request retention to " + settingsManager.getODFSettings().getMessagingConfiguration().getAnalysisRequestRetentionMs());
	}

	@AfterClass
	public static void cleanupTrackerRetention() throws ValidationException {
		SettingsManager settingsManager = new ODFFactory().create().getSettingsManager();
		ODFSettings settings = settingsManager.getODFSettings();
		settings.getMessagingConfiguration().setAnalysisRequestRetentionMs(origRetention);
		settingsManager.updateODFSettings(settings);
	}

	@Test
	public void testStatusQueue() throws Exception {
		KafkaQueueManager kqm = new KafkaQueueManager();

		logger.info("Queue manager created");
		AnalysisRequestTracker tracker = JSONUtils.readJSONObjectFromFileInClasspath(AnalysisRequestTracker.class, "org/apache/atlas/odf/core/test/messaging/kafka/tracker1.json", null);

		long before = System.currentTimeMillis();
		tracker.setLastModified(before);
		int maxEntries = 10;
		for (int i = 0; i < maxEntries; i++) {
			tracker.getRequest().setId("id" + i);
			StatusQueueEntry sqe = new StatusQueueEntry();
			sqe.setAnalysisRequestTracker(tracker);
			kqm.enqueueInStatusQueue(sqe);

			//			System.out.println("tracker "+i+" enqueued in status queue");
		}
		long after = System.currentTimeMillis();
		logger.info("Time for enqueueing " + maxEntries + " objects: " + (after - before) + ", " + ((after - before) / maxEntries) + "ms per object");
		Thread.sleep(100 * maxEntries);

		AnalysisRequestTrackerStore store = new DefaultStatusQueueStore();

		for (int i = 0; i < maxEntries; i++) {
			logger.info("Querying status " + i);
			AnalysisRequestTracker queriedTracker = store.query("id" + i);
			Assert.assertNotNull(queriedTracker);
			Assert.assertEquals(STATUS.FINISHED, queriedTracker.getStatus());
		}

		//	Thread.sleep(5000);
		//	Assert.fail("you fail");
		logger.info("Test testEnqueueStatusQueue finished");
	}

	/**
	 * This test creates a tracker, puts it on the status queue, kills the service consumer and creates a new dummy consumer to put the offset of the service consumer behind the new tracker
	 * Then the status consumer is shut down and its offset is reset in order to make it consume from the start again and thereby cleaning up stuck processes
	 * Then kafka queue manager is re-initialized, causing all consumers to come up and triggering the cleanup process
	 */
	@Test
	@Ignore("Adjust once ServiceRuntimes are fully implemented")
	public void testStuckRequestCleanup() throws JSONException, InterruptedException, ExecutionException, TimeoutException {
		final AnalysisRequestTracker tracker = JSONUtils.readJSONObjectFromFileInClasspath(AnalysisRequestTracker.class, "org/apache/atlas/odf/core/test/messaging/kafka/tracker1.json",
				null);
		tracker.setStatus(STATUS.IN_DISCOVERY_SERVICE_QUEUE);
		tracker.setNextDiscoveryServiceRequest(0);
		tracker.setLastModified(System.currentTimeMillis());
		final String newTrackerId = "KAFKA_QUEUE_MANAGER_09_TEST" + UUID.randomUUID().toString();
		tracker.getRequest().setId(newTrackerId);
		DiscoveryServiceRequest dsRequest = TrackerUtil.getCurrentDiscoveryServiceStartRequest(tracker);
		final DiscoveryServiceProperties discoveryServiceRegistrationInfo = new ODFFactory().create().getDiscoveryServiceManager().getDiscoveryServicesProperties()
				.get(0);
		dsRequest.setDiscoveryServiceId(discoveryServiceRegistrationInfo.getId());
		String dsID = dsRequest.getDiscoveryServiceId();
		String topicName = KafkaQueueManager.SERVICE_TOPIC_PREFIX + dsID;
		//Add tracker to queue, set offset behind request so that it should be cleanup

		String consumerGroupId = "odf-topic-" + dsID + "_group";
		String threadName = "Dummy_DiscoveryServiceQueueConsumer" + topicName;

		final List<Throwable> multiThreadErrors = new ArrayList<Throwable>();
		final DefaultThreadManager tm = new DefaultThreadManager();
		logger.info("shutdown old test 09 consumer and replace with fake doing nothing");
		for (int no = 0; no < discoveryServiceRegistrationInfo.getParallelismCount(); no++) {
			tm.shutdownThreads(Collections.singletonList("DiscoveryServiceQueueConsumer" + topicName + "_" + no));
		}
		Properties kafkaConsumerProps = getKafkaConsumerConfigProperties(consumerGroupId);

		final long[] producedMsgOffset = new long[1];

		final CountDownLatch msgProcessingLatch = new CountDownLatch(1);
		ThreadStartupResult created = tm.startUnmanagedThread(threadName, new KafkaQueueConsumer(topicName, kafkaConsumerProps, new QueueMessageProcessor() {

			@Override
			public void process(ExecutorService executorService, String msg, int partition, long msgOffset) {
				logger.info("Dequeue without processing " + msgOffset);
				if (msgOffset == producedMsgOffset[0]) {
					try {
						msgProcessingLatch.countDown();
					} catch (Exception e) {
						msgProcessingLatch.countDown();
						multiThreadErrors.add(e);
					}
				}
			}

		}));

		tm.waitForThreadsToBeReady(30000, Arrays.asList(created));

		String key = tracker.getRequest().getId();
		String value = JSONUtils.toJSON(tracker);

		new DefaultStatusQueueStore().store(tracker);

		KafkaMonitor kafkaMonitor = new ODFInternalFactory().create(KafkaMonitor.class);
		List<String> origQueueConsumers = kafkaMonitor.getConsumerGroups(zookeeperConnectString, KafkaQueueManager.TOPIC_NAME_STATUS_QUEUE);
		logger.info("Found status consumers: " + origQueueConsumers.toString() + ", shutting down StatusWatcher");

		//kill status queue watcher so that it is restarted when queue manager is initialized and detects stuck requests
		tm.shutdownThreads(Collections.singletonList("StatusWatcher" + KafkaQueueManager.TOPIC_NAME_STATUS_QUEUE + "_0"));

		int maxWaitForConsumerDeath = 60;
		while (tm.getStateOfUnmanagedThread("StatusWatcher" + KafkaQueueManager.TOPIC_NAME_STATUS_QUEUE + "_0") != ThreadState.NON_EXISTENT
				|| tm.getStateOfUnmanagedThread("StatusWatcher" + KafkaQueueManager.TOPIC_NAME_STATUS_QUEUE + "_0") != ThreadState.FINISHED && maxWaitForConsumerDeath > 0) {
			maxWaitForConsumerDeath--;
			Thread.sleep(500);
		}

		logger.info("Only 1 consumer left? " + maxWaitForConsumerDeath + " : " + tm.getStateOfUnmanagedThread("StatusWatcher" + KafkaQueueManager.TOPIC_NAME_STATUS_QUEUE + "_0"));
		logger.info(" set offset for status consumer to beginning so that it consumes from when restarting");
		final int offset = 1000000;
		for (String statusConsumerGroup : origQueueConsumers) {
			if (statusConsumerGroup.contains("DSStatusWatcherConsumerGroup")) {
				boolean success = false;
				int retryCount = 0;
				final int maxOffsetRetry = 20;
				while (!success && retryCount < maxOffsetRetry) {
					success = kafkaMonitor.setOffset(zookeeperConnectString, statusConsumerGroup, KafkaQueueManager.TOPIC_NAME_STATUS_QUEUE, 0, offset);
					retryCount++;
					Thread.sleep(500);
				}

				Assert.assertNotEquals(retryCount, maxOffsetRetry);
				Assert.assertTrue(success);
			}
		}

		new ODFInternalFactory().create(KafkaProducerManager.class).sendMsg(topicName, key, value, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				producedMsgOffset[0] = metadata.offset();
			}
		});

		final boolean await = msgProcessingLatch.await(240, TimeUnit.SECONDS);
		Assert.assertTrue(await);
		if (await) {
			logger.info("run after message consumption...");
			AnalysisRequestTrackerStore store = new ODFInternalFactory().create(AnalysisRequestTrackerStore.class);
			AnalysisRequestTracker storeTracker = store.query(tracker.getRequest().getId());
			Assert.assertEquals(tracker.getRequest().getId(), storeTracker.getRequest().getId());
			Assert.assertEquals(STATUS.IN_DISCOVERY_SERVICE_QUEUE, storeTracker.getStatus());

			//start odf and cleanup here...
			logger.info("shutdown all threads and restart ODF");
			tm.shutdownAllUnmanagedThreads();

			int threadKillRetry = 0;
			while (tm.getNumberOfRunningThreads() > 0 && threadKillRetry < 20) {
				Thread.sleep(500);
				threadKillRetry++;
			}

			logger.info("All threads down, restart ODF " + threadKillRetry);

			// Initialize analysis manager
			new ODFFactory().create().getAnalysisManager();

			kafkaMonitor = new ODFInternalFactory().create(KafkaMonitor.class);
			origQueueConsumers = kafkaMonitor.getConsumerGroups(zookeeperConnectString, KafkaQueueManager.TOPIC_NAME_STATUS_QUEUE);
			int healthRetrieveRetry = 0;
			//wait for max of 40 secs for status consumer to come up. If it is, we can continue because ODF is restarted successfully
			while (origQueueConsumers.isEmpty() && healthRetrieveRetry < 240) {
				healthRetrieveRetry++;
				Thread.sleep(500);
				origQueueConsumers = kafkaMonitor.getConsumerGroups(zookeeperConnectString, KafkaQueueManager.TOPIC_NAME_STATUS_QUEUE);
			}
			Assert.assertNotEquals(healthRetrieveRetry, 240);

			logger.info("initialized, wait for cleanup ... " + healthRetrieveRetry);
			Thread.sleep(5000);
			logger.info("Found health consumers: " + origQueueConsumers.toString());
			logger.info("hopefully cleaned up ...");
			AnalysisRequestTracker storedTracker = store.query(tracker.getRequest().getId());
			Assert.assertEquals(STATUS.ERROR, storedTracker.getStatus());
			logger.info("DONE CLEANING UP, ALL FINE");
		}

		Assert.assertEquals(0, multiThreadErrors.size());
	}

	public Properties getKafkaConsumerConfigProperties(String consumerGroupID) {
		SettingsManager odfConfig = new ODFFactory().create().getSettingsManager();
		Properties kafkaConsumerProps = odfConfig.getKafkaConsumerProperties();
		kafkaConsumerProps.put("group.id", consumerGroupID);
		if (zookeeperConnectString != null) {
			kafkaConsumerProps.put("zookeeper.connect", zookeeperConnectString);
		}

		kafkaConsumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		StringBuilder bld = new StringBuilder();
		final Iterator<String> iterator = new ODFInternalFactory().create(KafkaMonitor.class).getBrokers(zookeeperConnectString).iterator();
		while (iterator.hasNext()) {
			bld.append(iterator.next());
			if (iterator.hasNext()) {
				bld.append(",");
			}
		}
		kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bld.toString());
		kafkaConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		return kafkaConsumerProps;
	}

}

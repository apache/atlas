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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.controlcenter.ODFRunnable;
import org.apache.atlas.odf.api.engine.PartitionOffsetInfo;

public class MessageSearchConsumer implements ODFRunnable {
	private static final long POLLING_DURATION_MS = 100;
	private static final int MAX_POLL_COUNT = 5;

	private Logger logger = Logger.getLogger(MessageSearchConsumer.class.getName());
	private SearchCompletedCallback searchCompletedCallback;
	private List<String> searchStrings;
	protected String topic;
	private KafkaConsumer<String, String> kafkaConsumer;
	private boolean shutdown;
	private boolean ready = false;
	private List<PartitionOffsetInfo> maxOffsetsForTopic = new ArrayList<PartitionOffsetInfo>();


	public MessageSearchConsumer(String topic, SearchCompletedCallback completitionCallback, List<String> searchStrings) {
		setTopic(topic);
		setSearchStrings(searchStrings);
		setCompletitionCallback(completitionCallback);
	}

	public MessageSearchConsumer() {
	}

	protected List<PartitionOffsetInfo> retrieveTopicOffsets() {
		List<PartitionOffsetInfo> offsetsForTopic = new ArrayList<PartitionOffsetInfo>();
		String zookeeperConnect = new ODFInternalFactory().create(Environment.class).getZookeeperConnectString();

		if (zookeeperConnect != null) {
			final KafkaMonitor create = new ODFInternalFactory().create(KafkaMonitor.class);
			for (int part : create.getPartitionsForTopic(zookeeperConnect, this.topic)) {
				offsetsForTopic.add(create.getOffsetsOfLastMessagesForTopic(zookeeperConnect, this.topic, part));
			}
		}
		return offsetsForTopic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public void setSearchStrings(List<String> searchStrings) {
		this.searchStrings = searchStrings;
	}

	public void setCompletitionCallback(SearchCompletedCallback completitionCallback) {
		this.searchCompletedCallback = completitionCallback;
	}

	protected Properties getKafkaConsumerProperties() {
		Properties consumerProperties = new ODFFactory().create().getSettingsManager().getKafkaConsumerProperties();
		consumerProperties.put("group.id", UUID.randomUUID().toString() + "_searchConsumer");
		final String zookeeperConnect = new ODFInternalFactory().create(Environment.class).getZookeeperConnectString();
		consumerProperties.put("zookeeper.connect", zookeeperConnect);
		consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		final Iterator<String> brokers = new ODFInternalFactory().create(KafkaMonitor.class).getBrokers(zookeeperConnect).iterator();
		StringBuilder brokersString = new StringBuilder();
		while (brokers.hasNext()) {
			brokersString.append(brokers.next());
			if (brokers.hasNext()) {
				brokersString.append(",");
			}
		}
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersString.toString());
		consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		return consumerProperties;
	}

	@Override
	public void run() {
		this.maxOffsetsForTopic = retrieveTopicOffsets();
		final String logPrefix = "Consumer for topic " + topic + ": ";
		try {

			Map<Integer, Boolean> maxOffsetReachedMap = new HashMap<Integer, Boolean>();
			if (maxOffsetsForTopic.isEmpty()) {
				logger.info("No offsets found for topic " + this.topic + ", therefore no matching messages can be found");
				if (searchCompletedCallback != null) {
					searchCompletedCallback.onDoneSearching(new HashMap<String, PartitionOffsetInfo>());
					return;
				}
			}
			for (PartitionOffsetInfo info : maxOffsetsForTopic) {
				//if the max offset is -1, no message exists on the partition
				if (info.getOffset() > -1) {
					maxOffsetReachedMap.put(info.getPartitionId(), false);
				}
			}

			Map<String, PartitionOffsetInfo> resultMap = new HashMap<String, PartitionOffsetInfo>();

			Properties consumerProperties = getKafkaConsumerProperties();

			if (this.kafkaConsumer == null) {
				logger.fine(logPrefix + " create new consumer for topic " + topic);
				try {
					this.kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);
					//In order to prevent other consumers from getting assigned this partition during a rebalance, the partition(s) MUST be assigned manually (not using auto assign because of subscribe())
					kafkaConsumer.subscribe(Arrays.asList(topic));
				} catch (ZkTimeoutException zkte) {
					String zkHosts = consumerProperties.getProperty("zookeeper.connect");
					logger.log(Level.SEVERE, logPrefix + " Could not connect to the Zookeeper instance at ''{0}''. Please ensure that Zookeeper is running", zkHosts);
					throw zkte;
				}
			}
			logger.log(Level.INFO, logPrefix + " Consumer " + "''{1}'' is now listening on ODF queue ''{0}'' with configuration {2}",
					new Object[] { topic, kafkaConsumer, consumerProperties });

			int pollCount = 0;
			while (!Thread.interrupted() && pollCount < MAX_POLL_COUNT && !shutdown && kafkaConsumer != null) {
				logger.info("searching ...");
				pollCount++;
				ConsumerRecords<String, String> records = kafkaConsumer.poll(POLLING_DURATION_MS);
				ready = true;
				final Iterator<ConsumerRecord<String, String>> polledRecords = records.records(topic).iterator();
				
				while (polledRecords.hasNext() && !shutdown) {
					final ConsumerRecord<String, String> next = polledRecords.next();
					for (String s : searchStrings) {
						if ((next.key() != null && next.key().equals(s)) || (next.value() != null && next.value().contains(s))) {
							final PartitionOffsetInfo position = new PartitionOffsetInfo();
							position.setOffset(next.offset());
							position.setPartitionId(next.partition());
							resultMap.put(s, position);
						}
					}

					if (next.offset() == maxOffsetsForTopic.get(next.partition()).getOffset()) {
						maxOffsetReachedMap.put(next.partition(), true);
					}

					boolean allCompleted = true;
					for (Entry<Integer, Boolean> entry : maxOffsetReachedMap.entrySet()) {
						if (!entry.getValue()) {
							allCompleted = false;
							break;
						}
					}

					if (allCompleted) {
						logger.info("Done searching all messages");
						if (searchCompletedCallback != null) {
							searchCompletedCallback.onDoneSearching(resultMap);
							return;
						}
						shutdown = true;
					}
				}
			}
		} catch (Exception exc) {
			String msg = MessageFormat.format(" Caught exception on queue ''{0}''", topic);
			logger.log(Level.WARNING, logPrefix + msg, exc);
		} finally {
			if (kafkaConsumer != null) {
				logger.log(Level.FINE, logPrefix + "Closing consumer " + " on topic ''{0}''", topic);
				kafkaConsumer.close();
				logger.log(Level.FINE, logPrefix + "Closed consumer " + " on topic ''{0}''", topic);
				kafkaConsumer = null;
			}
		}
		logger.log(Level.FINE, logPrefix + "Finished consumer on topic ''{0}''", topic);
	}

	@Override
	public void setExecutorService(ExecutorService service) {

	}

	@Override
	public void cancel() {
		this.shutdown = true;
	}

	@Override
	public boolean isReady() {
		return ready;
	}

	public interface SearchCompletedCallback {
		void onDoneSearching(Map<String, PartitionOffsetInfo> msgPositionMap);
	}
}

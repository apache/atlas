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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.atlas.odf.api.engine.BrokerNode;
import org.apache.atlas.odf.api.engine.KafkaBrokerPartitionMessageCountInfo;
import org.apache.atlas.odf.api.engine.KafkaPartitionInfo;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.engine.PartitionOffsetInfo;
import org.apache.atlas.odf.json.JSONUtils;

import kafka.admin.AdminClient;
import kafka.admin.AdminClient.ConsumerSummary;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.GroupCoordinatorRequest;
import kafka.api.GroupCoordinatorResponse;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.cluster.EndPoint;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.coordinator.GroupOverview;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.network.BlockingChannel;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class KafkaMonitor {
	private final static String CLIENT_ID = "odfMonitorClient";

	private Logger logger = Logger.getLogger(KafkaMonitor.class.getName());

	//this only works for consumer groups managed by the kafka coordinator (unlike with kafka < 0.9 where consumers where managed by zookeeper)
	public List<String> getConsumerGroups(String zookeeperHost, String topic) {
		List<String> result = new ArrayList<String>();
		try {
			List<String> brokers = getBrokers(zookeeperHost);
			StringBuilder brokersParam = new StringBuilder();
			final Iterator<String> iterator = brokers.iterator();
			while (iterator.hasNext()) {
				brokersParam.append(iterator.next());
				if (iterator.hasNext()) {
					brokersParam.append(";");
				}
			}
			Properties props = new Properties();
			props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokersParam.toString());
			final AdminClient client = AdminClient.create(props);
			final Map<Node, scala.collection.immutable.List<GroupOverview>> javaMap = JavaConversions.mapAsJavaMap(client.listAllConsumerGroups());
			for (Entry<Node, scala.collection.immutable.List<GroupOverview>> entry : javaMap.entrySet()) {
				for (GroupOverview group : JavaConversions.seqAsJavaList(entry.getValue())) {
					//Option<scala.collection.immutable.List<ConsumerSummary>> optConsumerSummary = client.describeConsumerGroup(group.groupId());
					//if (optConsumerSummary.nonEmpty()) {
						for (ConsumerSummary summary : JavaConversions.seqAsJavaList(client.describeConsumerGroup(group.groupId()) ) ) {
							for (TopicPartition part : JavaConversions.seqAsJavaList(summary.assignment())) {
								if (part.topic().equals(topic) && !result.contains(group.groupId())) {
									result.add(group.groupId());
								break;
							}
						}
					}
					//}
				}
			}
		} catch (Exception ex) {
			logger.log(Level.WARNING, "An error occured retrieving the consumer groups", ex.getCause());
			ex.printStackTrace();
		}
		return result;
	}

	private ZkUtils getZkUtils(String zookeeperHost, ZkClient zkClient) {
		return new ZkUtils(zkClient, new ZkConnection(zookeeperHost), false);
	}

	private ZkClient getZkClient(String zookeeperHost) {
		return new ZkClient(zookeeperHost, 5000, 5000, ZKStringSerializer$.MODULE$);
	}

	public boolean setOffset(String zookeeperHost, String consumerGroup, String topic, int partition, long offset) {
		logger.info("set offset for " + consumerGroup + " " + offset);
		long now = System.currentTimeMillis();
		Map<TopicAndPartition, OffsetAndMetadata> offsets = new LinkedHashMap<TopicAndPartition, OffsetAndMetadata>();
		final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		offsets.put(topicAndPartition, new OffsetAndMetadata(new OffsetMetadata(offset, "Manually set offset"), now, -1));
		int correlationId = 0;
		OffsetCommitRequest req = new OffsetCommitRequest(consumerGroup, offsets, correlationId++, CLIENT_ID, (short) 1);
		final BlockingChannel channel = getOffsetManagerChannel(zookeeperHost, consumerGroup);
		channel.send(req.underlying());
		OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(channel.receive().payload());
		if (commitResponse.hasError()) {
			logger.warning("Could not commit offset! " + topic + ":" + partition + "-" + offset + " error: " + commitResponse.errorCode(topicAndPartition));
			channel.disconnect();
			return false;
		} else {
			logger.info("offset commit successfully");
			channel.disconnect();
			return true;
		}
	}

	public List<String> getBrokers(String zookeeperHost) {
		List<String> result = new ArrayList<String>();
		ZkClient zkClient = getZkClient(zookeeperHost);
		List<Broker> brokerList = JavaConversions.seqAsJavaList(getZkUtils(zookeeperHost, zkClient).getAllBrokersInCluster());
		Iterator<Broker> brokerIterator = brokerList.iterator();
		while (brokerIterator.hasNext()) {
			for (Entry<SecurityProtocol, EndPoint> entry : JavaConversions.mapAsJavaMap(brokerIterator.next().endPoints()).entrySet()) {
				String connectionString = entry.getValue().connectionString();
				//remove protocol from string
				connectionString = connectionString.split("://")[1];
				result.add(connectionString);
			}
		}
		zkClient.close();
		return result;
	}

	public PartitionOffsetInfo getOffsetsOfLastMessagesForTopic(String zookeeperHost, String topic, int partition) {
		List<String> kafkaBrokers = getBrokers(zookeeperHost);
		return getOffsetsOfLastMessagesForTopic(kafkaBrokers, topic, partition);
	}

	public PartitionOffsetInfo getOffsetsOfLastMessagesForTopic(final List<String> kafkaBrokers, final String topic, final int partition) {
		logger.entering(this.getClass().getName(), "getOffsetsOfLastMessagesForTopic");

		final PartitionOffsetInfo info = new PartitionOffsetInfo();
		info.setOffset(-1l);
		info.setPartitionId(partition);

		final CountDownLatch subscribeAndPollLatch = new CountDownLatch(2);

		final Thread consumerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				Properties kafkaConsumerProps = getKafkaConsumerProps(kafkaBrokers);
				final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaConsumerProps);
				final TopicPartition topicPartition = new TopicPartition(topic, partition);
				consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {

					@Override
					public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
						// TODO Auto-generated method stub

					}

					@Override
					public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
						subscribeAndPollLatch.countDown();
					}
				});
				logger.info("poll records from kafka for offset retrieval");

				final ConsumerRecords<String, String> poll = consumer.poll(500);
				List<ConsumerRecord<String, String>> polledRecords = poll.records(topicPartition);
				logger.info("polled records: " + poll.count());
				if (!polledRecords.isEmpty()) {
					ConsumerRecord<String, String> record = polledRecords.get(polledRecords.size() - 1);
					info.setMessage(record.value());
					info.setOffset(record.offset());
					info.setPartitionId(partition);
					logger.info("polled last offset: " + record.offset());
				}
				subscribeAndPollLatch.countDown();
				consumer.close();
			}
		});
		logger.info("start retrieval of offset");
		consumerThread.start();

		try {
			boolean result = subscribeAndPollLatch.await(5000, TimeUnit.MILLISECONDS);
			if (result) {
				logger.info("Subscribed and retrieved offset on time: " + JSONUtils.toJSON(info));
			} else {
				logger.warning("Could not subscribe and retrieve offset on time " + JSONUtils.toJSON(info));
				consumerThread.interrupt();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			logger.log(Level.WARNING, "An error occured retrieving the last retrieved offset", e.getCause());
		} catch (JSONException e) {
			e.printStackTrace();
			logger.log(Level.WARNING, "An error occured retrieving the last retrieved offset", e.getCause());
		}

		return info;
	}

	protected Properties getKafkaConsumerProps(List<String> kafkaBrokers) {
		Properties kafkaConsumerProps = new Properties();
		kafkaConsumerProps.put("group.id", "OffsetRetrieverConsumer" + UUID.randomUUID().toString());
		kafkaConsumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

		StringBuilder brokers = new StringBuilder();
		final Iterator<String> iterator = kafkaBrokers.iterator();
		while (iterator.hasNext()) {
			brokers.append(iterator.next());
			if (iterator.hasNext()) {
				brokers.append(",");
			}
		}
		kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.toString());
		kafkaConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		return kafkaConsumerProps;
	}

	public List<KafkaBrokerPartitionMessageCountInfo> getMessageCountForTopic(String zookeeperHost, String topic) {
		logger.entering(this.getClass().getName(), "getMessageCountForTopic");
		List<KafkaBrokerPartitionMessageCountInfo> result = new ArrayList<KafkaBrokerPartitionMessageCountInfo>();

		List<Integer> partitions = getPartitionIdsForTopic(zookeeperHost, topic);

		List<String> kafkaBrokers = getBrokers(zookeeperHost);
		for (int cnt = 0; cnt < kafkaBrokers.size(); cnt++) {
			String broker = kafkaBrokers.get(cnt);
			logger.info("getMessageCountForTopic from broker: " + broker);
			KafkaBrokerPartitionMessageCountInfo container = new KafkaBrokerPartitionMessageCountInfo();
			container.setBroker(broker);

			String[] splitBroker = broker.split(":");
			String host = splitBroker[0];
			String port = splitBroker[1];
			SimpleConsumer consumer = new SimpleConsumer(host, Integer.valueOf(port), 100000, 64 * 1024, "leaderLookup");
			Map<Integer, Long> partitionCountMap = new HashMap<Integer, Long>();

			for (Integer partition : partitions) {
				logger.info("broker: " + broker + ", partition " + partition);
				partitionCountMap.put(partition, null);
				FetchRequest req = new FetchRequestBuilder().clientId(CLIENT_ID).addFetch(topic, partition, 0, 100000).build();
				FetchResponse fetchResponse = consumer.fetch(req);

				if (fetchResponse.hasError()) {
					//in case of a broker error, do nothing. The broker has no information about the partition so we continue with the next one.
					if (fetchResponse.errorCode(topic, partition) == ErrorMapping.NotLeaderForPartitionCode()) {
						logger.info("broker " + broker + " is not leader for partition " + partition + ", cannot retrieve MessageCountForTopic");
					} else {
						logger.warning("broker: " + broker + ", partition " + partition + " has error: " + fetchResponse.errorCode(topic, partition));
					}
					continue;
				}

				long numRead = 0;
				long readOffset = numRead;

				for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
					long currentOffset = messageAndOffset.offset();
					if (currentOffset < readOffset) {
						logger.info("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
						continue;
					}
					readOffset = messageAndOffset.nextOffset();
					numRead++;
				}

				logger.info("broker: " + broker + ", partition " + partition + " total messages: " + numRead);
				partitionCountMap.put(partition, numRead);
			}
			consumer.close();
			container.setPartitionMsgCountMap(partitionCountMap);
			result.add(container);
		}

		return result;
	}

	/**
	 * @param group
	 * @param topic
	 * @return a list of partitions and their offsets. If no offset is found, it is returned as -1
	 */
	public List<PartitionOffsetInfo> getOffsetsForTopic(String zookeeperHost, String group, String topic) {
		BlockingChannel channel = getOffsetManagerChannel(zookeeperHost, group);

		List<Integer> partitionIds = getPartitionIdsForTopic(zookeeperHost, topic);
		List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
		int correlationId = 0;
		for (Integer id : partitionIds) {
			TopicAndPartition testPartition0 = new TopicAndPartition(topic, id);
			partitions.add(testPartition0);
		}

		OffsetFetchRequest fetchRequest = new OffsetFetchRequest(group, partitions, (short) 1 /* version */, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
				correlationId++, CLIENT_ID);

		List<PartitionOffsetInfo> offsetResult = new ArrayList<PartitionOffsetInfo>();
		int retryCount = 0;
		//it is possible that a ConsumerCoordinator is not available yet, if this is the case we need to wait and try again.
		boolean done = false;
		while (retryCount < 5 && !done) {
			offsetResult = new ArrayList<PartitionOffsetInfo>();
			retryCount++;
			channel.send(fetchRequest.underlying());
			OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload());

			boolean errorFound = false;
			for (TopicAndPartition part : partitions) {
				if (part.topic().equals(topic)) {
					PartitionOffsetInfo offsetInfo = new PartitionOffsetInfo();
					offsetInfo.setPartitionId(part.partition());
					OffsetMetadataAndError result = fetchResponse.offsets().get(part);
					short offsetFetchErrorCode = result.error();
					if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
						channel.disconnect();
						String msg = "Offset could not be fetched, the used broker is not the coordinator for this consumer";
						offsetInfo.setMessage(msg);
						logger.warning(msg);
						errorFound = true;
						break;
					} else if (offsetFetchErrorCode == ErrorMapping.OffsetsLoadInProgressCode()) {
						logger.warning("Offset could not be fetched at this point, the offsets are not available yet");
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						//Offsets are not available yet. Wait and try again
						errorFound = true;
						break;
					} else if (result.error() != ErrorMapping.NoError()) {
						String msg = MessageFormat.format("Offset could not be fetched at this point, an unknown error occured ( {0} )", result.error());
						offsetInfo.setMessage(msg);
						logger.warning(msg);
					} else {
						long offset = result.offset();
						offsetInfo.setOffset(offset);
					}

					offsetResult.add(offsetInfo);
				}
			}
			if (!errorFound) {
				done = true;
			}
		}

		if (channel.isConnected()) {
			channel.disconnect();
		}
		return offsetResult;
	}

	public List<TopicMetadata> getMetadataForTopic(String zookeeperHost, String kafkaTopic) {
		//connecting to a single broker should be enough because every single broker knows everything we need
		for (String brokerHost : getBrokers(zookeeperHost)) {
			brokerHost = brokerHost.replace("PLAINTEXT://", "");
			String[] splitBroker = brokerHost.split(":");
			String ip = splitBroker[0];
			String port = splitBroker[1];

			//it is possible that a ConsumerCoordinator is not available yet, if this is the case we need to wait and try again.
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(ip, Integer.valueOf(port), 100000, 64 * 1024, "leaderLookup");
				int retryCount = 0;
				boolean done = false;
				while (retryCount < 5 && !done) {
					retryCount++;

					List<String> topics = Collections.singletonList(kafkaTopic);
					TopicMetadataRequest req = new TopicMetadataRequest(topics);
					kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
					List<TopicMetadata> metaData = resp.topicsMetadata();

					boolean errorFound = false;
					for (TopicMetadata item : metaData) {
						if (item.topic().equals(kafkaTopic)) {
							if (item.errorCode() == ErrorMapping.LeaderNotAvailableCode()) {
								//wait and try again
								errorFound = true;
								try {
									Thread.sleep(2000);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								break;
							}
							return metaData;
						}
					}

					if (!errorFound) {
						done = true;
					}
				}
			} finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}
		return null;
	}

	public List<Integer> getPartitionsForTopic(String zookeeperHost, String topic) {
		ZkClient zkClient = new ZkClient(zookeeperHost, 5000, 5000, ZKStringSerializer$.MODULE$);
		Map<String, Seq<Object>> partitions = JavaConversions
				.mapAsJavaMap(new ZkUtils(zkClient, new ZkConnection(zookeeperHost), false).getPartitionsForTopics(JavaConversions.asScalaBuffer(Arrays.asList(topic)).toList()));
		List<Object> partitionObjList = JavaConversions.seqAsJavaList(partitions.entrySet().iterator().next().getValue());
		List<Integer> partitionsList = new ArrayList<Integer>();
		for (Object partObj : partitionObjList) {
			partitionsList.add((Integer) partObj);
		}
		zkClient.close();
		return partitionsList;
	}

	public List<KafkaPartitionInfo> getPartitionInfoForTopic(String zookeeperHost, String topic) {
		List<TopicMetadata> topicInfos = getMetadataForTopic(zookeeperHost, topic);
		List<KafkaPartitionInfo> partitionInfoList = new ArrayList<KafkaPartitionInfo>();
		for (TopicMetadata topicInfo : topicInfos) {
			for (PartitionMetadata part : topicInfo.partitionsMetadata()) {
				KafkaPartitionInfo info = new KafkaPartitionInfo();
				info.setPartitionId(part.partitionId());

				List<BrokerNode> partitionNodes = new ArrayList<BrokerNode>();
				for (BrokerEndPoint brokerPoint : part.isr()) {
					BrokerNode node = new BrokerNode();
					node.setHost(brokerPoint.connectionString());
					node.setLeader(brokerPoint.connectionString().equals(part.leader().connectionString()));
					partitionNodes.add(node);
				}
				info.setNodes(partitionNodes);
				partitionInfoList.add(info);
			}
		}
		//partitionInformation is collected, end loop and return
		return partitionInfoList;
	}

	public List<Integer> getPartitionIdsForTopic(String zookeeperHost, String topic) {
		List<TopicMetadata> metadata = getMetadataForTopic(zookeeperHost, topic);

		List<Integer> partitionsList = new ArrayList<Integer>();
		if (metadata != null && metadata.size() > 0) {
			for (PartitionMetadata partData : metadata.get(0).partitionsMetadata()) {
				partitionsList.add(partData.partitionId());
			}
		}

		return partitionsList;
	}

	private BlockingChannel getOffsetManagerChannel(String zookeeperHost, String group) {
		int correlationId = 0;
		for (String broker : getBrokers(zookeeperHost)) {
			String[] splitBroker = broker.split(":");
			String ip = splitBroker[0];
			String port = splitBroker[1];

			int retryCount = 0;
			//it is possible that a ConsumerCoordinator is not available yet, if this is the case we need to wait and try again.
			while (retryCount < 5) {
				retryCount++;

				BlockingChannel channel = new BlockingChannel(ip, Integer.valueOf(port), BlockingChannel.UseDefaultBufferSize(), BlockingChannel.UseDefaultBufferSize(),
						5000 /* read timeout in millis */);
				channel.connect();
				channel.send(new GroupCoordinatorRequest(group, OffsetRequest.CurrentVersion(), correlationId++, CLIENT_ID));
				GroupCoordinatorResponse metadataResponse = GroupCoordinatorResponse.readFrom(channel.receive().payload());

				if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
					BrokerEndPoint endPoint = metadataResponse.coordinatorOpt().get();
					if (!endPoint.host().equals(ip) && !port.equals(endPoint.port())) {
						channel.disconnect();
						channel = new BlockingChannel(endPoint.host(), endPoint.port(), BlockingChannel.UseDefaultBufferSize(), BlockingChannel.UseDefaultBufferSize(), 5000);
						channel.connect();
					}
					return channel;
				} else if (metadataResponse.errorCode() == ErrorMapping.ConsumerCoordinatorNotAvailableCode()
						|| metadataResponse.errorCode() == ErrorMapping.OffsetsLoadInProgressCode()) {
					//wait and try again
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					//unknown error, continue with next broker
					break;
				}
			}
		}
		throw new RuntimeException("Kafka Consumer Broker not available!");
	}
}

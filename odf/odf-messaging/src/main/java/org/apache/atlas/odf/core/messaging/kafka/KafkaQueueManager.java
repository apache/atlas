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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.atlas.odf.api.OpenDiscoveryFramework;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.engine.KafkaGroupOffsetInfo;
import org.apache.atlas.odf.api.engine.KafkaStatus;
import org.apache.atlas.odf.api.engine.KafkaTopicStatus;
import org.apache.atlas.odf.api.engine.ThreadStatus;
import org.apache.atlas.odf.api.settings.KafkaMessagingConfiguration;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.messaging.DiscoveryServiceQueueManager;
import org.apache.atlas.odf.core.notification.NotificationListener;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.engine.MessagingStatus;
import org.apache.atlas.odf.api.engine.PartitionOffsetInfo;
import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.controlcenter.AdminMessage;
import org.apache.atlas.odf.core.controlcenter.AdminQueueProcessor;
import org.apache.atlas.odf.core.controlcenter.ConfigChangeQueueProcessor;
import org.apache.atlas.odf.core.controlcenter.DefaultStatusQueueStore.StatusQueueProcessor;
import org.apache.atlas.odf.core.controlcenter.DiscoveryServiceStarter;
import org.apache.atlas.odf.core.controlcenter.ExecutorServiceFactory;
import org.apache.atlas.odf.core.controlcenter.QueueMessageProcessor;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntime;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntimes;
import org.apache.atlas.odf.core.controlcenter.StatusQueueEntry;
import org.apache.atlas.odf.core.controlcenter.ThreadManager;
import org.apache.atlas.odf.core.controlcenter.ThreadManager.ThreadStartupResult;
import org.apache.atlas.odf.core.controlcenter.TrackerUtil;
import org.apache.atlas.odf.core.notification.NotificationManager;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class KafkaQueueManager implements DiscoveryServiceQueueManager {

	public static final String TOPIC_NAME_STATUS_QUEUE = "odf-status-topic";
	public static final String TOPIC_NAME_ADMIN_QUEUE = "odf-admin-topic";
	public static final String ADMIN_QUEUE_KEY = "odf-admin-queue-key";
	public static final String SERVICE_TOPIC_PREFIX = "odf-topic-";

	public static final RackAwareMode DEFAULT_RACK_AWARE_MODE = RackAwareMode.Disabled$.MODULE$;
	
	//use static UUID so that no unnecessary consumer threads are started
	private final static String UNIQUE_SESSION_THREAD_ID = UUID.randomUUID().toString();

	private final static int THREAD_STARTUP_TIMEOUT_MS = 5000;
	
	private static List<String> queueConsumerNames = null;
	private static Object startLock = new Object();

	private final static Logger logger = Logger.getLogger(KafkaQueueManager.class.getName());

	private ThreadManager threadManager;
	private SettingsManager odfConfig;
	private String zookeeperConnectString;

	public KafkaQueueManager() {
		ODFInternalFactory factory = new ODFInternalFactory();
		threadManager = factory.create(ThreadManager.class);
		ExecutorServiceFactory esf = factory.create(ExecutorServiceFactory.class);
		threadManager.setExecutorService(esf.createExecutorService());
		zookeeperConnectString = factory.create(Environment.class).getZookeeperConnectString();
		odfConfig = factory.create(SettingsManager.class);
	}
	
	
	public Properties getConsumerConfigProperties(String consumerGroupID, boolean consumeFromEnd) {
		Properties kafkaConsumerProps = odfConfig.getKafkaConsumerProperties();
		kafkaConsumerProps.put("group.id", consumerGroupID);
		if (zookeeperConnectString != null) {
			kafkaConsumerProps.put("zookeeper.connect", zookeeperConnectString);
		}
		if (consumeFromEnd) {
			kafkaConsumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		} else {
			kafkaConsumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		}
		kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
		kafkaConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);		
		return kafkaConsumerProps;
	}

	private String getBootstrapServers() {
		final List<String> brokers = new ODFInternalFactory().create(KafkaMonitor.class).getBrokers(zookeeperConnectString);
		StringBuilder servers = new StringBuilder();
		final Iterator<String> iterator = brokers.iterator();
		while(iterator.hasNext()){
			servers.append(iterator.next());
			if(iterator.hasNext()){
				servers.append(",");
			}
		}
		return servers.toString();
	}

	protected void createTopicIfNotExists(String topicName, int partitionCount, Properties props) {
		String zkHosts = props.getProperty("zookeeper.connect");
		ZkClient zkClient = null;
		try {
			zkClient = new ZkClient(zkHosts, Integer.valueOf(props.getProperty("zookeeperSessionTimeoutMs")),
					Integer.valueOf(props.getProperty("zookeeperConnectionTimeoutMs")), ZKStringSerializer$.MODULE$);
		} catch (ZkTimeoutException zkte) {
			logger.log(Level.SEVERE, "Could not connect to the Zookeeper instance at ''{0}''. Please ensure that Zookeeper is running", zkHosts);
		}
		try {
			logger.log(Level.FINEST, "Checking if topic ''{0}'' already exists", topicName);
			// using partition size 1 and replication size 1, no special
			// per-topic config needed
			try {
				final ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkHosts), false);
				if (!AdminUtils.topicExists(zkUtils, topicName)) {
					logger.log(Level.INFO, "Topic ''{0}'' does not exist, creating it", topicName);

					//FIXME zkUtils isSecure parameter? Only with SSL! --> parse zkhosts?
					KafkaMessagingConfiguration kafkaConfig = ((KafkaMessagingConfiguration) odfConfig.getODFSettings().getMessagingConfiguration());
					AdminUtils.createTopic(zkUtils, topicName, partitionCount, kafkaConfig.getKafkaBrokerTopicReplication(),
							new Properties(), DEFAULT_RACK_AWARE_MODE);
					logger.log(Level.FINE, "Topic ''{0}'' created", topicName);
					//wait before continuing to make sure the topic exists BEFORE consumers are started
					try {
						Thread.sleep(1500);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (TopicExistsException ex) {
				logger.log(Level.FINE, "Topic ''{0}'' already exists.", topicName);
			}
		} finally {
			if (zkClient != null) {
				zkClient.close();
			}
		}
	}


	private String getTopicName(ServiceRuntime runtime) {
		return "odf-runtime-" + runtime.getName();
	}
	
	private String getConsumerGroup(ServiceRuntime runtime) {
		return getTopicName(runtime) + "_group";
	}
	
	private List<ThreadStartupResult> scheduleAllRuntimeConsumers() {
		List<ThreadStartupResult> results = new ArrayList<>();
		for (ServiceRuntime runtime : ServiceRuntimes.getActiveRuntimes()) {
			results.addAll(scheduleRuntimeConsumers(runtime));
		}
		return results;
	}
	
	private List<ThreadStartupResult> scheduleRuntimeConsumers(ServiceRuntime runtime) {
		logger.log(Level.FINER, "Create consumers on queue for runtime ''{0}'' if it doesn't already exist", runtime.getName());

		String topicName = getTopicName(runtime);
		String consumerGroupId = getConsumerGroup(runtime);
		Properties kafkaConsumerProps = getConsumerConfigProperties(consumerGroupId, false); // read entries from beginning if consumer was never initialized 
		String threadName = "RuntimeQueueConsumer" + topicName;
		List<ThreadStartupResult> result = new ArrayList<ThreadStartupResult>();
		if (threadManager.getStateOfUnmanagedThread(threadName) != ThreadStatus.ThreadState.RUNNING) {
			createTopicIfNotExists(topicName, 1, kafkaConsumerProps);
			ThreadStartupResult startupResult = threadManager.startUnmanagedThread(threadName, new KafkaRuntimeConsumer(runtime, topicName, kafkaConsumerProps, new DiscoveryServiceStarter()));
			result.add(startupResult);		
		} else {
			result.add(new ThreadStartupResult(threadName) {
				@Override
				public boolean isNewThreadCreated() {
					return false;
				}

				@Override
				public boolean isReady() {
					return true;
				}
			});
		}
		return result;
	}

	
	private List<ThreadStartupResult> scheduleConsumerThreads(String topicName, int partitionCount, Properties kafkaConsumerProps, String threadName,
			List<QueueMessageProcessor> processors) {
		if (processors.size() != partitionCount) {
			final String msg = "The number of processors must be equal to the partition count in order to support parallel processing";
			logger.warning(msg);
			throw new RuntimeException(msg);
		}
		createTopicIfNotExists(topicName, partitionCount, kafkaConsumerProps);

		List<ThreadStartupResult> result = new ArrayList<ThreadStartupResult>();
		for (int no = 0; no < partitionCount; no++) {
			if (threadManager.getStateOfUnmanagedThread(threadName + "_" + no) != ThreadStatus.ThreadState.RUNNING) {
				QueueMessageProcessor processor = processors.get(no);
				ThreadStartupResult created = threadManager.startUnmanagedThread(threadName + "_" + no, new KafkaQueueConsumer(topicName, kafkaConsumerProps, processor));
				if (created.isNewThreadCreated()) {
					logger.log(Level.INFO, "Created new consumer thread on topic ''{0}'' with group ID ''{1}'', thread name: ''{2}'', properties: ''{3}''",
							new Object[] { topicName, kafkaConsumerProps.getProperty("group.id"), threadName + "_" + no, kafkaConsumerProps.toString() });
				} else {
					logger.log(Level.FINE, "Consumer thread with thread name: ''{0}'' already exists, doing nothing", new Object[] { threadName + "_" + no });
				}
				result.add(created);
			} else {
				result.add(new ThreadStartupResult(threadName) {
					@Override
					public boolean isNewThreadCreated() {
						return false;
					}

					@Override
					public boolean isReady() {
						return true;
					}
				});
			}
		}
		return result;
	}

	private ThreadStartupResult scheduleConsumerThread(String topicName, Properties kafkaConsumerProps, String threadName, QueueMessageProcessor processor) {
		return scheduleConsumerThreads(topicName, 1, kafkaConsumerProps, threadName, Arrays.asList(processor)).get(0);
	}

	@Override
	public void enqueue(AnalysisRequestTracker tracker) {
		DiscoveryServiceRequest dsRequest = TrackerUtil.getCurrentDiscoveryServiceStartRequest(tracker);
		if (dsRequest == null) {
			throw new RuntimeException("Tracker is finished, should not be enqueued");
		}
		String dsID = dsRequest.getDiscoveryServiceId();
		dsRequest.setPutOnRequestQueue(System.currentTimeMillis());
		ServiceRuntime runtime = ServiceRuntimes.getRuntimeForDiscoveryService(dsID);
		if (runtime == null) {
			throw new RuntimeException(MessageFormat.format("Service runtime for service ''{0}'' was not found.", dsID));
		}
		enqueueJSONMessage(getTopicName(runtime), tracker, tracker.getRequest().getId());
	}

	private void enqueueJSONMessage(String topicName, Object jsonObject, String key) {
		String value = null;
		try {
			value = JSONUtils.toJSON(jsonObject);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
		new ODFInternalFactory().create(KafkaProducerManager.class).sendMsg(topicName, key, value);
	}

	List<ThreadStartupResult> scheduleStatusQueueConsumers() {
		logger.log(Level.FINER, "Create consumers on status queue if they don't already exist");
		List<ThreadStartupResult> results = new ArrayList<ThreadStartupResult>();

		// create consumer thread for the status watcher of all trackes
		String statusWatcherConsumerGroupID = "DSStatusWatcherConsumerGroup" + UNIQUE_SESSION_THREAD_ID; // have a new group id on each node that reads all from the beginning
		// always read from beginning for the status queue
		Properties statusWatcherKafkaConsumerProps = getConsumerConfigProperties(statusWatcherConsumerGroupID, false);
		final String statusWatcherThreadName = "StatusWatcher" + TOPIC_NAME_STATUS_QUEUE; // a fixed name
		String threadNameWithPartition = statusWatcherThreadName + "_0";
		final ThreadStatus.ThreadState stateOfUnmanagedThread = threadManager.getStateOfUnmanagedThread(threadNameWithPartition);
		logger.fine("State of status watcher thread: " + stateOfUnmanagedThread);
		if (stateOfUnmanagedThread != ThreadStatus.ThreadState.RUNNING) {
			final ThreadStartupResult scheduleConsumerThread = scheduleConsumerThread(TOPIC_NAME_STATUS_QUEUE, statusWatcherKafkaConsumerProps, statusWatcherThreadName,
					new StatusQueueProcessor());
			results.add(scheduleConsumerThread);
		} else {
			results.add(new ThreadStartupResult(statusWatcherThreadName) {
				@Override
				public boolean isNewThreadCreated() {
					return false;
				}

				@Override
				public boolean isReady() {
					return true;
				}
			});
		}

		return results;
	}


	@Override
	public void enqueueInStatusQueue(StatusQueueEntry sqe) {
		enqueueJSONMessage(TOPIC_NAME_STATUS_QUEUE, sqe, StatusQueueEntry.getRequestId(sqe));
	}


	private List<ThreadStartupResult> scheduleAdminQueueConsumers() {
		List<ThreadStartupResult> results = new ArrayList<ThreadStartupResult>();
		//schedule admin queue consumers
		// consumer group so that every node receives events
		String adminWatcherConsumerGroupID = "DSAdminQueueConsumerGroup" + UNIQUE_SESSION_THREAD_ID; // have a new group id on each node 
		Properties adminWatcherKafkaConsumerProps = getConsumerConfigProperties(adminWatcherConsumerGroupID, true);
		final String adminWatcherThreadName = "AdminWatcher" + TOPIC_NAME_ADMIN_QUEUE;
		String threadNameWithPartition = adminWatcherThreadName + "_0";
		if (threadManager.getStateOfUnmanagedThread(threadNameWithPartition) != ThreadStatus.ThreadState.RUNNING) {
			results.add(scheduleConsumerThread(TOPIC_NAME_ADMIN_QUEUE, adminWatcherKafkaConsumerProps, adminWatcherThreadName, new AdminQueueProcessor()));
			// consumer group so only one node receives events
			String distributedAdminConsumerGroup = "DSAdminQueueConsumerGroupCommon";
			Properties kafkaProps = getConsumerConfigProperties(distributedAdminConsumerGroup, true);
			final String threadName = "DistributedAdminWatcher";
			results.add(scheduleConsumerThread(TOPIC_NAME_ADMIN_QUEUE, kafkaProps, threadName, new ConfigChangeQueueProcessor()));
		} else {
			results.add(new ThreadStartupResult(adminWatcherThreadName) {
				@Override
				public boolean isNewThreadCreated() {
					return false;
				}

				@Override
				public boolean isReady() {
					return true;
				}
			});
		}
		return results;
	}

	@Override
	public void enqueueInAdminQueue(AdminMessage message) {
		enqueueJSONMessage(TOPIC_NAME_ADMIN_QUEUE, message, ADMIN_QUEUE_KEY);
	}

	@Override
	public void start() throws TimeoutException {
		synchronized (startLock) {
			if (queueConsumerNames == null) {
				List<ThreadStartupResult> results = new ArrayList<>();
				results.addAll(scheduleStatusQueueConsumers());
				results.addAll(scheduleAdminQueueConsumers());
				results.addAll(scheduleAllRuntimeConsumers());
				results.addAll(scheduleNotificationListenerThreads());
				List<String> consumerNames = new ArrayList<>();
				for (ThreadStartupResult tsr : results) {
					consumerNames.add(tsr.getThreadId());
				}
				queueConsumerNames = consumerNames;
				this.threadManager.waitForThreadsToBeReady(THREAD_STARTUP_TIMEOUT_MS * results.size(), results);
				logger.info("KafkaQueueManager successfully initialized");
			}
		}
	}
	
	public void stop() {
		synchronized (startLock) {
			if (queueConsumerNames != null) {
				threadManager.shutdownThreads(queueConsumerNames);
				queueConsumerNames = null;
			}
		}
	}

	@Override
	public MessagingStatus getMessagingStatus() {
		KafkaStatus status = new KafkaStatus();
		KafkaMonitor monitor = new ODFInternalFactory().create(KafkaMonitor.class);
		status.setBrokers(monitor.getBrokers(zookeeperConnectString));

		List<String> topics = new ArrayList<String>(Arrays.asList(KafkaQueueManager.TOPIC_NAME_ADMIN_QUEUE, KafkaQueueManager.TOPIC_NAME_STATUS_QUEUE));
		for (DiscoveryServiceProperties info : new ODFFactory().create().getDiscoveryServiceManager().getDiscoveryServicesProperties()) {
			topics.add(KafkaQueueManager.SERVICE_TOPIC_PREFIX + info.getId());
		}

		List<KafkaTopicStatus> topicStatusList = new ArrayList<KafkaTopicStatus>();
		for (String topic : topics) {
			KafkaTopicStatus topicStatus = getTopicStatus(topic, monitor);
			topicStatusList.add(topicStatus);
		}
		status.setTopicStatus(topicStatusList);
		return status;
	}

	private KafkaTopicStatus getTopicStatus(String topic, KafkaMonitor monitor) {
		KafkaTopicStatus topicStatus = new KafkaTopicStatus();
		topicStatus.setTopic(topic);
		topicStatus.setBrokerPartitionMessageInfo(monitor.getMessageCountForTopic(zookeeperConnectString, topic));

		List<KafkaGroupOffsetInfo> offsetInfoList = new ArrayList<KafkaGroupOffsetInfo>();
		List<String> consumerGroupsFromZookeeper = monitor.getConsumerGroups(zookeeperConnectString, topic);
		for (String group : consumerGroupsFromZookeeper) {
			KafkaGroupOffsetInfo offsetInfoContainer = new KafkaGroupOffsetInfo();
			offsetInfoContainer.setGroupId(group);
			List<PartitionOffsetInfo> offsetsForTopic = monitor.getOffsetsForTopic(zookeeperConnectString, group, topic);
			for (PartitionOffsetInfo info : offsetsForTopic) {
				// to reduce clutter, only if at least 1 partition has an offset > -1 (== any offset) for this consumer group, 
				// it will be included in the result
				if (info.getOffset() > -1) {
					offsetInfoContainer.setOffsets(offsetsForTopic);
					offsetInfoList.add(offsetInfoContainer);
					break;
				}
			}
		}
		topicStatus.setConsumerGroupOffsetInfo(offsetInfoList);

		topicStatus.setPartitionBrokersInfo(monitor.getPartitionInfoForTopic(zookeeperConnectString, topic));
		return topicStatus;
	}

	private List<ThreadStartupResult> scheduleNotificationListenerThreads() {
		NotificationManager nm = new ODFInternalFactory().create(NotificationManager.class);
		List<NotificationListener> listeners = nm.getListeners();
		List<ThreadStartupResult> result = new ArrayList<>();
		if (listeners == null) {
			return result;
		}
		final OpenDiscoveryFramework odf = new ODFFactory().create();
		for (final NotificationListener listener : listeners) {
			String topicName = listener.getTopicName();
			String consumerGroupId = "ODFNotificationGroup" + topicName;
			Properties kafkaConsumerProps = getConsumerConfigProperties(consumerGroupId, true);  
			String threadName = "NotificationListenerThread" + topicName;
			if (threadManager.getStateOfUnmanagedThread(threadName) != ThreadStatus.ThreadState.RUNNING) {
				KafkaQueueConsumer consumer = new KafkaQueueConsumer(topicName, kafkaConsumerProps, new QueueMessageProcessor() {
					
					@Override
					public void process(ExecutorService executorService, String msg, int partition, long msgOffset) {
						try {
							listener.onEvent(msg, odf);
						} catch(Exception exc) {
							String errorMsg = MessageFormat.format("Notification listsner ''{0}'' has thrown an exception. Ignoring it", listener.getName());
							logger.log(Level.WARNING, errorMsg, exc);
						}
					}
				});
				ThreadStartupResult startupResult = threadManager.startUnmanagedThread(threadName, consumer);
				result.add(startupResult);		
			} else {
				result.add(new ThreadStartupResult(threadName) {
					@Override
					public boolean isNewThreadCreated() {
						return false;
					}

					@Override
					public boolean isReady() {
						return true;
					}
				});
			}
		}
		return result;
	}
	
}

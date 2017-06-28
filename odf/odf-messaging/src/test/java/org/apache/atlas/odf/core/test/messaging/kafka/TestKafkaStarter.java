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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.BindException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.rmi.NotBoundException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.wink.json4j.JSONObject;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import org.apache.atlas.odf.core.Utils;

import kafka.cluster.Broker;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class TestKafkaStarter {

	public static boolean deleteRecursive(File path) throws FileNotFoundException {
		if (!path.exists()) {
			throw new FileNotFoundException(path.getAbsolutePath());
		}
		boolean ret = true;
		if (path.isDirectory()) {
			for (File f : path.listFiles()) {
				ret = ret && deleteRecursive(f);
			}
		}
		return ret && path.delete();
	}

	static Thread zookeeperThread = null;
	static boolean kafkaStarted = false;
	static Object lockObject = new Object();
	static KafkaServerStartable kafkaServer = null;
	static ZooKeeperServerMainWithShutdown zooKeeperServer = null;


	boolean cleanData = true; // all data is cleaned at server start !!

	public boolean isCleanData() {
		return cleanData;
	}

	public void setCleanData(boolean cleanData) {
		this.cleanData = cleanData;
	}

	Logger logger = Logger.getLogger(TestKafkaStarter.class.getName());

	void log(String s) {
		logger.info(s);
	}

	int zookeeperStartupTime = 10000;
	int kafkaStartupTime = 10000;

	static class ZooKeeperServerMainWithShutdown extends ZooKeeperServerMain {
		public void shutdown() {
			super.shutdown();
		}
	}

	private void startZookeeper() throws Exception {
		log("Starting zookeeper");

		final Properties zkProps = Utils.readConfigProperties("org/apache/atlas/odf/core/messaging/kafka/test-embedded-zookeeper.properties");
		final String zkPort = (String) zkProps.get("clientPort");
		if (zooKeeperServer == null) {
			log("zookeeper properties: " + zkProps);
			if (cleanData) {
				String dataDir = zkProps.getProperty("dataDir");
				log("Removing all data from zookeeper data dir " + dataDir);
				File dir = new File(dataDir);
				if (dir.exists()) {
					if (!deleteRecursive(dir)) {
						throw new IOException("Could not delete directory " + dataDir);
					}
				}
			}
			final ZooKeeperServerMainWithShutdown zk = new ZooKeeperServerMainWithShutdown();
			final ServerConfig serverConfig = new ServerConfig();
			log("Loading zookeeper config...");
			QuorumPeerConfig zkConfig = new QuorumPeerConfig();
			zkConfig.parseProperties(zkProps);
			serverConfig.readFrom(zkConfig);

			Runnable zookeeperStarter = new Runnable() {

				@Override
				public void run() {
					try {
						log("Now starting Zookeeper with API...");
						zk.runFromConfig(serverConfig);
					} catch (BindException ex) {
						log("Embedded zookeeper could not be started, port is already in use. Trying to use external zookeeper");
						ZooKeeper zk = null;
						try {
							zk = new ZooKeeper("localhost:" + zkPort, 5000, null);
							if (zk.getState().equals(States.CONNECTED)) {
								log("Using existing zookeeper running on port " + zkPort);
								return;
							} else {
								throw new NotBoundException();
							}
						} catch (Exception zkEx) {
							throw new RuntimeException("Could not connect to zookeeper on port " + zkPort + ". Please close all applications listening on this port.");
						} finally {
							if (zk != null) {
								try {
									zk.close();
								} catch (InterruptedException e) {
									logger.log(Level.WARNING, "An error occured closing the zk connection", e);
								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}

				}
			};

			zookeeperThread = new Thread(zookeeperStarter);
			zookeeperThread.setDaemon(true);
			zookeeperThread.start();
			log("Zookeeper start initiated");
			zooKeeperServer = zk;
		}
		ZkConnection conn = new ZkConnection("localhost:" + zkPort);
		final CountDownLatch latch = new CountDownLatch(1);
		conn.connect(new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				log("Zookeeper event: " + event.getState());
				if (event.getState().equals(KeeperState.SyncConnected)) {
					log("Zookeeper server up and running");
					latch.countDown();
				}
			}
		});

		boolean zkReady = latch.await(zookeeperStartupTime, TimeUnit.MILLISECONDS);
		if (zkReady) {
			log("Zookeeper initialized and started");

		} else {
			logger.severe("Zookeeper could not be initialized within " + (zookeeperStartupTime / 1000) + " sec");
		}
		conn.close();
	}

	public boolean isRunning() {
		return kafkaStarted;
	}

	public void startKafka() throws Exception {
		synchronized (lockObject) {
			if (kafkaStarted) {
				log("Kafka already running");
				return;
			}
			this.startZookeeper();

			log("Starting Kafka server...");
			Properties kafkaProps = Utils.readConfigProperties("org/apache/atlas/odf/core/messaging/kafka/test-embedded-kafka.properties");
			log("Kafka properties: " + kafkaProps);
			KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);
			int kafkaPort = kafkaConfig.port();
			if (cleanData && isPortAvailable(kafkaPort)) {
				String logDir = kafkaProps.getProperty("log.dirs");
				log("Removing all data from kafka log dir: " + logDir);
				File dir = new File(logDir);
				if (dir.exists()) {
					if (!deleteRecursive(dir)) {
						throw new IOException("Kafka logDir could not be deleted: " + logDir);
					}
				}
			}
			if (!isPortAvailable(kafkaPort)) {
				log("Kafka port " + kafkaPort + " is already in use. "
						+ "Checking if zookeeper has a registered broker on this port to make sure it is an existing kafka instance using the port.");
				ZooKeeper zk = new ZooKeeper(kafkaConfig.zkConnect(), 10000, null);
				try {
					List<String> ids = zk.getChildren("/brokers/ids", false);
					if (ids != null && !ids.isEmpty()) {
						for (String id : ids) {
							String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null), "UTF-8");
							JSONObject broker = new JSONObject(brokerInfo);
							Integer port = new Integer(String.valueOf(broker.get("port")));
							if (port != null && port.equals(kafkaPort)) {
								log("Using externally started kafka broker on port " + port);
								kafkaStarted = true;
								return;
							}
						}
					}
				} catch (NoNodeException ex) {
					log("No brokers registered with zookeeper!");
					throw new RuntimeException("Kafka broker port " + kafkaPort
							+ " not available and no broker found! Please close all running applications listening on this port");
				} finally {
					if (zk != null) {
						try {
							zk.close();
						} catch (InterruptedException e) {
							logger.log(Level.WARNING, "An error occured closing the zk connection", e);
						}
					}
				}
			}
			KafkaServerStartable kafka  = KafkaServerStartable.fromProps(kafkaProps);
			kafka.startup();
			log("Kafka server start initiated");

			kafkaServer = kafka;
			log("Give Kafka a maximum of " + kafkaStartupTime + " ms to start");
			ZkClient zk = new ZkClient(kafkaConfig.zkConnect(), 10000, 5000, ZKStringSerializer$.MODULE$);
			int maxRetryCount = kafkaStartupTime / 1000;
			int cnt = 0;
			while (cnt < maxRetryCount) {
				cnt++;
				Seq<Broker> allBrokersInCluster = new ZkUtils(zk, new ZkConnection(kafkaConfig.zkConnect()), false).getAllBrokersInCluster();
				List<Broker> brokers = JavaConversions.seqAsJavaList(allBrokersInCluster);
				for (Broker broker : brokers) {
					if (broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT).port() == kafkaPort) {
						log("Broker is registered, Kafka is available after " + cnt + " seconds");
						kafkaStarted = true;
						return;
					}
				}
				Thread.sleep(1000);
			}
			logger.severe("Kafka broker was not started after " + kafkaStartupTime + " ms");
		}
	}

	public void shutdownKafka() {
		// do nothing for shutdown
	}

	boolean isPortAvailable(int port) {
		ServerSocket ss = null;
		DatagramSocket ds = null;
		try {
			ss = new ServerSocket(port);
			ss.setReuseAddress(true);
			ds = new DatagramSocket(port);
			ds.setReuseAddress(true);
			return true;
		} catch (IOException e) {
		} finally {
			if (ds != null) {
				ds.close();
			}

			if (ss != null) {
				try {
					ss.close();
				} catch (IOException e) {
				}
			}
		}

		return false;
	}
}

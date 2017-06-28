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
package org.apache.atlas.odf.core.store.zookeeper34.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.BindException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.rmi.NotBoundException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import org.apache.atlas.odf.core.Utils;

public class TestZookeeper {

	public TestZookeeper() {
	}

	public void start() {
		try {
			startZookeeper();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

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
	static Object lockObject = new Object();
	static ZooKeeperServerMainWithShutdown zooKeeperServer = null;

	boolean cleanData = true; // all data is cleaned at server start !!

	Logger logger = Logger.getLogger(TestZookeeper.class.getName());

	void log(String s) {
		logger.info(s);
	}

	int zookeeperStartupTime = 10000;

	static class ZooKeeperServerMainWithShutdown extends ZooKeeperServerMain {
		public void shutdown() {
			super.shutdown();
		}
	}

	private void startZookeeper() throws Exception {
		log("Starting zookeeper");

		final Properties zkProps = Utils.readConfigProperties("org/apache/atlas/odf/core/messaging/kafka/test-embedded-zookeeper.properties");
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
		final String zkPort = (String) zkProps.get("clientPort");

		Runnable zookeeperStarter = new Runnable() {

			@Override
			public void run() {
				try {
					log("Now starting Zookeeper with API...");
					zk.runFromConfig(serverConfig);
				} catch (BindException ex) {
					log("Embedded zookeeper could not be started, port is already in use. Trying to use external zookeeper");
					ZooKeeper zK = null;
					try {
						zK = new ZooKeeper("localhost:" + zkPort, 5000, null);
						if (zK.getState().equals(States.CONNECTED)) {
							log("Using existing zookeeper running on port " + zkPort);
							return;
						} else {
							throw new NotBoundException();
						}
					} catch (Exception zkEx) {
						throw new RuntimeException("Could not connect to zookeeper on port " + zkPort + ". Please close all applications listening on this port.");
					} finally {
						if (zK != null) {
							try {
								zK.close();
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
		log("Zookeeper start initiated, waiting 10s...");
		Thread.sleep(10000);
		zooKeeperServer = zk;
		log("Zookeeper started");

	}

	public boolean isRunning() {
		return zooKeeperServer != null;
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

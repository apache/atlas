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
package org.apache.atlas.odf.core.store.zookeeper34;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.configuration.ConfigContainer;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import org.apache.atlas.odf.core.store.ODFConfigurationStorage;

public class ZookeeperConfigurationStorage implements ODFConfigurationStorage {
	private Logger logger = Logger.getLogger(ZookeeperConfigurationStorage.class.getName());
	static final String ZOOKEEPER_CONFIG_PATH = "/odf/config";
	static String configCache = null; // cache is a string so that the object is not accidentally modified
	static Object configCacheLock = new Object();
	static HashSet<String> pendingConfigChanges = new HashSet<String>();

	String zookeeperString;

	public ZookeeperConfigurationStorage() {
		zookeeperString = new ODFInternalFactory().create(Environment.class).getZookeeperConnectString();
	}

	public void clearCache() {
		synchronized (configCacheLock) {
			configCache = null;
		}
	}
	
	@Override
	public void storeConfig(ConfigContainer config) {
		synchronized (configCacheLock) {
			ZooKeeper zk = null;
			String configTxt = null;
			try {
				configTxt = JSONUtils.toJSON(config);
				zk = getZkConnectionSynchronously();
				if (zk.exists(getZookeeperConfigPath(), false) == null) {
					//config file doesn't exist in zookeeper yet, write default config
					logger.log(Level.WARNING, "Zookeeper config not found - creating it before writing: {0}", configTxt);
					initializeConfiguration(zk, configTxt);
				}
				zk.setData(getZookeeperConfigPath(), configTxt.getBytes("UTF-8"), -1);
				configCache = configTxt;
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException("A zookeeper connection could not be established in time to write settings");
			} catch (KeeperException e) {
				if (Code.NONODE.equals(e.code())) {
					logger.info("Setting could not be written, the required node is not available!");
					initializeConfiguration(zk, configTxt);
					return;
				}
				//This should never happen! Only NoNode or BadVersion codes are possible. Because the file version is ignored, a BadVersion should never occur
				throw new RuntimeException("A zookeeper connection could not be established because of an unknown exception", e);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("A zookeeper connection could not be established because of an incorrect encoding");
			} catch (JSONException e) {
				throw new RuntimeException("Configuration is not valid", e);
			} finally {
				if (zk != null) {
					try {
						zk.close();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	@Override
	public ConfigContainer getConfig(ConfigContainer defaultConfiguration) {
		synchronized (configCacheLock) {
			if (configCache == null) {
				ZooKeeper zk = getZkConnectionSynchronously();
				try {
					if (zk.exists(getZookeeperConfigPath(), false) == null) {
						//config file doesn't exist in zookeeper yet, write default config
						String defaultConfigString = JSONUtils.toJSON(defaultConfiguration);
						logger.log(Level.WARNING, "Zookeeper config not found - creating now with default: {0}", defaultConfigString);
						initializeConfiguration(zk, defaultConfigString);
					}
					byte[] configBytes = zk.getData(getZookeeperConfigPath(), true, new Stat());
					if (configBytes != null) {
						String configString = new String(configBytes, "UTF-8");
						configCache = configString;
					} else {
						// should never happen
						throw new RuntimeException("Zookeeper configuration was not stored");
					}
				} catch (KeeperException e) {
					throw new RuntimeException(MessageFormat.format("Zookeeper config could not be read, {0} Zookeeper exception occured!", e.code().name()), e);
				} catch (InterruptedException e) {
					throw new RuntimeException("Zookeeper config could not be read, the connection was interrupded", e);
				} catch (IOException | JSONException e) {
					throw new RuntimeException("Zookeeper config could not be read, the file could not be parsed correctly", e);
				} finally {
					if (zk != null) {
						try {
							zk.close();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

					}
				}

			}
			try {
				return JSONUtils.fromJSON(configCache, ConfigContainer.class);
			} catch (JSONException e) {
				throw new RuntimeException("Cached configuration was not valid", e);
			}
		}
	}

	private void initializeConfiguration(ZooKeeper zk, String config) {
		try {
			if (getZookeeperConfigPath().contains("/")) {
				String[] nodes = getZookeeperConfigPath().split("/");
				StringBuilder path = new StringBuilder();
				for (String node : nodes) {
					if (node.trim().equals("")) {
						//ignore empty paths
						continue;
					}
					path.append("/" + node);
					try {
						zk.create(path.toString(), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					} catch (NodeExistsException ex) {
						//ignore if node already exists and continue with next node
					}
				}
			}

			//use version -1 to ignore versioning conflicts
			try {
				zk.setData(getZookeeperConfigPath(), config.toString().getBytes("UTF-8"), -1);
			} catch (UnsupportedEncodingException e) {
				// should not happen
				throw new RuntimeException(e);
			}
		} catch (KeeperException e) {
			throw new RuntimeException(MessageFormat.format("The zookeeper config could not be initialized, a Zookeeper exception of type {0} occured!", e.code().name()), e);
		} catch (InterruptedException e) {
			throw new RuntimeException("The zookeeper config could not be initialized, the connection got interrupted!", e);
		}
	}

	private ZooKeeper getZkConnectionSynchronously() {
		final CountDownLatch latch = new CountDownLatch(1);
		logger.log(Level.FINE, "Trying to connect to zookeeper at {0}", zookeeperString);
		ZooKeeper zk = null;
		try {
			int timeout = 5;
			zk = new ZooKeeper(zookeeperString, timeout * 1000, new Watcher() {

				@Override
				public void process(WatchedEvent event) {
					if (event.getState().equals(Watcher.Event.KeeperState.ConnectedReadOnly) || event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
						//count down latch, connected successfully to zk
						latch.countDown();
					}
				}
			});
			//block thread till countdown, maximum of "timeout" seconds
			latch.await(5 * timeout, TimeUnit.SECONDS);
			if (latch.getCount() > 0) {
				zk.close();
				throw new RuntimeException("The zookeeper connection could not be retrieved on time!");
			}
			return zk;
		} catch (IOException e1) {
			throw new RuntimeException("The zookeeper connection could not be retrieved, the connection failed!", e1);
		} catch (InterruptedException e) {
			throw new RuntimeException("Zookeeper connection could not be retrieved, the thread was interrupted!", e);
		}
	}

	public String getZookeeperConfigPath() {
		return ZOOKEEPER_CONFIG_PATH;
	}

	@Override
	public void onConfigChange(ConfigContainer container) {
		synchronized (configCacheLock) {
			try {
				configCache = JSONUtils.toJSON(container);
			} catch (JSONException e) {
				throw new RuntimeException("Config could not be cloned!", e);
			}
		}
	}

	@Override
	public void addPendingConfigChange(String changeId) {
		synchronized (configCacheLock) {
			pendingConfigChanges.add(changeId);
		}
	}

	@Override
	public void removePendingConfigChange(String changeId) {
		synchronized (configCacheLock) {
			pendingConfigChanges.remove(changeId);
		}
	}

	@Override
	public boolean isConfigChangePending(String changeId) {
		synchronized (configCacheLock) {
			return pendingConfigChanges.contains(changeId);
		}
	}
}

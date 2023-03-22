/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.plugin.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.atlas.authz.admin.client.AtlasAuthAdminClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.atlas.admin.client.RangerAdminClient;
import org.apache.atlas.plugin.service.RangerBasePlugin;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;


public class RangerUserStoreProvider {
	private static final Log LOG = LogFactory.getLog(RangerUserStoreProvider.class);

	private static final Log PERF_POLICYENGINE_INIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.init");

	private final String            serviceType;
	private final String            serviceName;
	private final AtlasAuthAdminClient atlasAuthAdminClient;
	private final KeycloakUserStore keycloakUserStore;

	private final String            cacheFileName;
	private final String			cacheFileNamePrefix;
	private final String            cacheDir;
	private final Gson              gson;
	private final boolean           disableCacheIfServiceNotFound;

	private long	lastActivationTimeInMillis;
	private long	lastUpdateTimeInMillis = -1L;
	private long    lastKnownUserStoreVersion = -1L;
	private boolean rangerUserStoreSetInPlugin;
	private boolean serviceDefSetInPlugin;

	public RangerUserStoreProvider(String serviceType, String appId, String serviceName, AtlasAuthAdminClient atlasAuthAdminClient, String cacheDir, Configuration config) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerUserStoreProvider(serviceName=" + serviceName + ").RangerUserStoreProvider()");
		}

		this.serviceType = serviceType;
		this.serviceName = serviceName;
		this.atlasAuthAdminClient = atlasAuthAdminClient;

		this.keycloakUserStore = new KeycloakUserStore(serviceType);

		if (StringUtils.isEmpty(appId)) {
			appId = serviceType;
		}

		cacheFileNamePrefix = "userstore";
		String cacheFilename = String.format("%s_%s_%s.json", appId, serviceName, cacheFileNamePrefix);
		cacheFilename = cacheFilename.replace(File.separatorChar, '_');
		cacheFilename = cacheFilename.replace(File.pathSeparatorChar, '_');

		this.cacheFileName = cacheFilename;
		this.cacheDir = cacheDir;

		Gson gson = null;
		try {
			gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").create();
		} catch (Throwable excp) {
			LOG.fatal("RangerUserStoreProvider(): failed to create GsonBuilder object", excp);
		}
		this.gson = gson;

		String propertyPrefix = "ranger.plugin." + serviceType;
		disableCacheIfServiceNotFound = config.getBoolean(propertyPrefix + ".disable.cache.if.servicenotfound", true);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerUserStoreProvider(serviceName=" + serviceName + ").RangerUserStoreProvider()");
		}
	}

	public long getLastActivationTimeInMillis() {
		return lastActivationTimeInMillis;
	}

	public void setLastActivationTimeInMillis(long lastActivationTimeInMillis) {
		this.lastActivationTimeInMillis = lastActivationTimeInMillis;
	}

	public boolean isRangerUserStoreSetInPlugin() {
		return rangerUserStoreSetInPlugin;
	}

	public void setRangerUserStoreSetInPlugin(boolean rangerUserStoreSetInPlugin) {
		this.rangerUserStoreSetInPlugin = rangerUserStoreSetInPlugin;
	}

	public void loadUserStore(RangerBasePlugin plugIn) {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerUserStoreProvider(serviceName= " + serviceName  + " serviceType= " + serviceType +").loadUserGroups()");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerUserStoreProvider.loadUserGroups(serviceName=" + serviceName + ")");
			long freeMemory = Runtime.getRuntime().freeMemory();
			long totalMemory = Runtime.getRuntime().totalMemory();
			PERF_POLICYENGINE_INIT_LOG.debug("In-Use memory: " + (totalMemory-freeMemory) + ", Free memory:" + freeMemory);
		}

		try {
			//load userGroupRoles from ranger admin
			RangerUserStore userStore = loadUserStoreFromAdmin();

			if (userStore == null) {
				//if userGroupRoles fetch from ranger Admin Fails, load from cache
				if (!rangerUserStoreSetInPlugin) {
					userStore = loadUserStoreFromCache();
				}
			}

			if (PERF_POLICYENGINE_INIT_LOG.isDebugEnabled()) {
				long freeMemory = Runtime.getRuntime().freeMemory();
				long totalMemory = Runtime.getRuntime().totalMemory();
				PERF_POLICYENGINE_INIT_LOG.debug("In-Use memory: " + (totalMemory - freeMemory) + ", Free memory:" + freeMemory);
			}

			if (userStore != null) {
				plugIn.setUserStore(userStore);
				rangerUserStoreSetInPlugin = true;
				setLastActivationTimeInMillis(System.currentTimeMillis());
				lastKnownUserStoreVersion = userStore.getUserStoreVersion() == null ? -1 : userStore.getUserStoreVersion();
				lastUpdateTimeInMillis = userStore.getUserStoreUpdateTime() == null ? -1 : userStore.getUserStoreUpdateTime().getTime();
			} else {
				if (!rangerUserStoreSetInPlugin && !serviceDefSetInPlugin) {
					plugIn.setUserStore(userStore);
					serviceDefSetInPlugin = true;
				}
			}
		} catch (RangerServiceNotFoundException snfe) {
			if (disableCacheIfServiceNotFound) {
				disableCache();
				plugIn.setUserStore(null);
				setLastActivationTimeInMillis(System.currentTimeMillis());
				lastKnownUserStoreVersion = -1L;
				lastUpdateTimeInMillis = -1L;
				serviceDefSetInPlugin = true;
			}
		} catch (Exception excp) {
			LOG.error("Encountered unexpected exception, ignoring..", excp);
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerUserStoreProvider(serviceName=" + serviceName + ").loadUserGroups()");
		}
	}

	private RangerUserStore loadUserStoreFromAdmin() throws RangerServiceNotFoundException {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerUserStoreProvider(serviceName=" + serviceName + ").loadUserStoreFromAdmin()");
		}

		RangerUserStore userStore = null;

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerUserStoreProvider.loadUserStoreFromAdmin(serviceName=" + serviceName + ")");
		}

		try {
			if ("atlas".equals(serviceName)) {
				LOG.info("RangerUserStoreProvider: fetching using keycloak directly for atlas service");
				userStore = keycloakUserStore.loadUserStoreIfUpdated(lastUpdateTimeInMillis);
			} else {
				userStore = atlasAuthAdminClient.getUserStoreIfUpdated(lastUpdateTimeInMillis);
			}

			boolean isUpdated = userStore != null;

			if(isUpdated) {

				long newVersion = userStore.getUserStoreVersion() == null ? -1 : userStore.getUserStoreVersion().longValue();
				saveToCache(userStore);
				LOG.info("RangerUserStoreProvider(serviceName=" + serviceName + "): found updated version. lastKnownUserStoreVersion=" + lastKnownUserStoreVersion + "; newVersion=" + newVersion );
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("RangerUserStoreProvider(serviceName=" + serviceName + ").run(): no update found. lastKnownUserStoreVersion=" + lastKnownUserStoreVersion );
				}
			}
		} catch (Exception excp) {
			LOG.error("RangerUserStoreProvider(serviceName=" + serviceName + "): failed to refresh userStore. Will continue to use last known version of userStore (" + "lastKnowRoleVersion= " + lastKnownUserStoreVersion, excp);
			userStore = null;
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerUserStoreProvider(serviceName=" + serviceName + " serviceType= " + serviceType + " ).loadUserStoreFromAdmin()");
		}

		return userStore;
	}

	private RangerUserStore loadUserStoreFromCache() {

		RangerUserStore userStore = null;

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerUserStoreProvider(serviceName=" + serviceName + ").loadUserStoreFromCache()");
		}

		File cacheFile = cacheDir == null ? null : new File(cacheDir + File.separator + cacheFileName);

		if (cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
			Reader reader = null;

			RangerPerfTracer perf = null;

			if (RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerUserStoreProvider.loadUserStoreFromCache(serviceName=" + serviceName + ")");
			}

			try {
				reader = new FileReader(cacheFile);

				userStore = gson.fromJson(reader, RangerUserStore.class);

				if (userStore != null) {
					if (!StringUtils.equals(serviceName, userStore.getServiceName())) {
						LOG.warn("ignoring unexpected serviceName '" + userStore.getServiceName() + "' in cache file '" + cacheFile.getAbsolutePath() + "'");

						userStore.setServiceName(serviceName);
					}

					lastKnownUserStoreVersion = userStore.getUserStoreVersion() == null ? -1 : userStore.getUserStoreVersion().longValue();
					lastUpdateTimeInMillis = userStore.getUserStoreUpdateTime() == null ? -1 : userStore.getUserStoreUpdateTime().getTime();
				}
			} catch (Exception excp) {
				LOG.error("failed to load userStore from cache file " + cacheFile.getAbsolutePath(), excp);
			} finally {
				RangerPerfTracer.log(perf);

				if (reader != null) {
					try {
						reader.close();
					} catch (Exception excp) {
						LOG.error("error while closing opened cache file " + cacheFile.getAbsolutePath(), excp);
					}
				}
			}
		} else {
			userStore = new RangerUserStore();
			userStore.setServiceName(serviceName);
			userStore.setUserStoreVersion(-1L);
			userStore.setUserStoreUpdateTime(null);
			userStore.setUserGroupMapping(new HashMap<String, Set<String>>());
			saveToCache(userStore);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerUserStoreProvider(serviceName=" + serviceName + ").RangerUserStoreProvider()");
		}

		return userStore;
	}

	public void saveToCache(RangerUserStore userStore) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerUserStoreProvider(serviceName=" + serviceName + ").saveToCache()");
		}

		if(userStore != null) {
			File cacheFile = null;
			if (cacheDir != null) {
				// Create the cacheDir if it doesn't already exist
				File cacheDirTmp = new File(cacheDir);
				if (cacheDirTmp.exists()) {
					cacheFile =  new File(cacheDir + File.separator + cacheFileName);
				} else {
					try {
						cacheDirTmp.mkdirs();
						cacheFile =  new File(cacheDir + File.separator + cacheFileName);
					} catch (SecurityException ex) {
						LOG.error("Cannot create cache directory", ex);
					}
				}
			}

			if(cacheFile != null) {

				RangerPerfTracer perf = null;

				if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
					perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerUserStoreProvider.saveToCache(serviceName=" + serviceName + ")");
				}

				Writer writer = null;

				try {
					writer = new FileWriter(cacheFile);

					gson.toJson(userStore, writer);
				} catch (Exception excp) {
					LOG.error("failed to save userStore to cache file '" + cacheFile.getAbsolutePath() + "'", excp);
				} finally {
					if(writer != null) {
						try {
							writer.close();
						} catch(Exception excp) {
							LOG.error("error while closing opened cache file '" + cacheFile.getAbsolutePath() + "'", excp);
						}
					}
				}

				RangerPerfTracer.log(perf);
			}
		} else {
			LOG.info("userStore is null. Nothing to save in cache");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerUserStoreProvider.saveToCache(serviceName=" + serviceName + ")");
		}
	}

	private void disableCache() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerUserStoreProvider.disableCache(serviceName=" + serviceName + ")");
		}

		File cacheFile = cacheDir == null ? null : new File(cacheDir + File.separator + cacheFileName);

		if(cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
			LOG.warn("Cleaning up local RangerUserStore cache");
			String renamedCacheFile = cacheFile.getAbsolutePath() + "_" + System.currentTimeMillis();
			if (!cacheFile.renameTo(new File(renamedCacheFile))) {
				LOG.error("Failed to move " + cacheFile.getAbsolutePath() + " to " + renamedCacheFile);
			} else {
				LOG.warn("Moved " + cacheFile.getAbsolutePath() + " to " + renamedCacheFile);
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No local RangerRoles cache found. No need to disable it!");
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerUserStoreProvider.disableCache(serviceName=" + serviceName + ")");
		}
	}
}

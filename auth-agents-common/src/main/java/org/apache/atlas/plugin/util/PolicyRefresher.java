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
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.authz.admin.client.AtlasAuthAdminClient;
import org.apache.atlas.policytransformer.CachePolicyTransformerImpl;
import org.apache.atlas.repository.audit.ESBasedAuditRepository;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.admin.client.RangerAdminClient;
import org.apache.atlas.authorization.hadoop.config.RangerPluginConfig;
import org.apache.atlas.plugin.policyengine.RangerPluginContext;
import org.apache.atlas.plugin.service.RangerBasePlugin;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.atlas.ApplicationProperties.DELTA_BASED_REFRESH;


public class PolicyRefresher extends Thread {
	private static final Log LOG = LogFactory.getLog(PolicyRefresher.class);

	private static final Log PERF_POLICYENGINE_INIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.init");

	private final RangerBasePlugin               plugIn;
	private final String                         serviceType;
	private final String                         serviceName;
	private final RangerAdminClient              rangerAdmin;
	private final AtlasAuthAdminClient			 atlasAuthAdminClient;
	private final RangerRolesProvider            rolesProvider;
	private final RangerUserStoreProvider		 userStoreProvider;
	private final long                           pollingIntervalMs;
	private final String                         cacheFileName;
	private final String                         cacheDir;
	private final Gson                           gson;
	private final BlockingQueue<DownloadTrigger> policyDownloadQueue = new LinkedBlockingQueue<>();
	private       Timer                          policyDownloadTimer;
	private       long                           lastKnownVersion    = -1L;
	private       long                           lastUpdatedTiemInMillis    = -1L;
	private       long                           lastActivationTimeInMillis;
	private       boolean                        policiesSetInPlugin;
	private       boolean                        serviceDefSetInPlugin;
	private       Configuration                  atlasConfig;
	private       boolean                        enableDeltaBasedRefresh;
	private       ESBasedAuditRepository         auditRepository;


	public PolicyRefresher(RangerBasePlugin plugIn) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + plugIn.getServiceName() + ").PolicyRefresher()");
		}
		LOG.info("==> PolicyRefresher(serviceName=" + plugIn.getServiceName() + ").PolicyRefresher()");

		RangerPluginConfig pluginConfig   = plugIn.getConfig();
		String             propertyPrefix = pluginConfig.getPropertyPrefix();

		this.plugIn      = plugIn;
		this.serviceType = plugIn.getServiceType();
		this.serviceName = plugIn.getServiceName();
		this.cacheDir    = pluginConfig.get(propertyPrefix + ".policy.cache.dir");

		String appId         = StringUtils.isEmpty(plugIn.getAppId()) ? serviceType : plugIn.getAppId();
		String cacheFilename = String.format("%s_%s.json", appId, serviceName);

		cacheFilename = cacheFilename.replace(File.separatorChar,  '_');
		cacheFilename = cacheFilename.replace(File.pathSeparatorChar,  '_');

		this.cacheFileName = cacheFilename;

		rangerAdmin = getRangerAdminClient();

		Gson gson = null;
		try {
			gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").create();
		} catch(Throwable excp) {
			LOG.fatal("PolicyRefresher(): failed to create GsonBuilder object", excp);
		}

		RangerPluginContext pluginContext  = plugIn.getPluginContext();
		this.atlasAuthAdminClient 		   = pluginContext.getAtlasAuthAdminClient();
		this.gson                          = gson;
		this.rolesProvider                 = new RangerRolesProvider(getServiceType(), appId, getServiceName(), atlasAuthAdminClient, cacheDir, pluginConfig);
		this.userStoreProvider             = new RangerUserStoreProvider(getServiceType(), appId, getServiceName(), atlasAuthAdminClient,  cacheDir, pluginConfig);
		this.pollingIntervalMs             = pluginConfig.getLong(propertyPrefix + ".policy.pollIntervalMs", 30 * 1000);

		try {
			this.atlasConfig = ApplicationProperties.get();
			this.auditRepository = new ESBasedAuditRepository(atlasConfig);
			this.auditRepository.start();
			this.enableDeltaBasedRefresh = this.atlasConfig.getBoolean(DELTA_BASED_REFRESH, false);
		} catch (AtlasException e) {
			LOG.error("PolicyDelta: Error while reading atlas configuration", e);
			this.enableDeltaBasedRefresh = false;
		}

		setName("PolicyRefresher(serviceName=" + serviceName + ")-" + getId());

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").PolicyRefresher()");
		}
	}

	/**
	 * @return the plugIn
	 */
	public RangerBasePlugin getPlugin() {
		return plugIn;
	}

	/**
	 * @return the serviceType
	 */
	public String getServiceType() {
		return serviceType;
	}

	/**
	 * @return the serviceName
	 */
	public String getServiceName() {
		return serviceName;
	}

	/**
	 * @return the rangerAdmin
	 */
	public RangerAdminClient getRangerAdminClient() {
		return rangerAdmin;
	}

	public long getLastActivationTimeInMillis() {
		return lastActivationTimeInMillis;
	}

	public void setLastActivationTimeInMillis(long lastActivationTimeInMillis) {
		this.lastActivationTimeInMillis = lastActivationTimeInMillis;
	}

	public void startRefresher() {
		loadRoles();
		loadPolicy();
		loadUserStore();
		super.start();

		policyDownloadTimer = new Timer("policyDownloadTimer", true);

		try {
			policyDownloadTimer.schedule(new DownloaderTask(policyDownloadQueue), pollingIntervalMs, pollingIntervalMs);

			LOG.info("Scheduled policyDownloadRefresher to download policies every " + pollingIntervalMs + " milliseconds");

		} catch (IllegalStateException exception) {
			LOG.error("Error scheduling policyDownloadTimer:", exception);
			LOG.error("*** Policies will NOT be downloaded every " + pollingIntervalMs + " milliseconds ***");

			policyDownloadTimer = null;
		}

	}

	public void stopRefresher() {

		Timer policyDownloadTimer = this.policyDownloadTimer;

		this.policyDownloadTimer = null;

		if (policyDownloadTimer != null) {
			policyDownloadTimer.cancel();
		}

		if (super.isAlive()) {
			super.interrupt();

			boolean setInterrupted = false;
			boolean isJoined = false;

			while (!isJoined) {
				try {
					super.join();
					isJoined = true;
				} catch (InterruptedException excp) {
					LOG.warn("PolicyRefresher(serviceName=" + serviceName + "): error while waiting for thread to exit", excp);
					LOG.warn("Retrying Thread.join(). Current thread will be marked as 'interrupted' after Thread.join() returns");
					setInterrupted = true;
				}
			}
			if (setInterrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	public void run() {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").run()");
		}

		while(true) {
			DownloadTrigger trigger = null;
			try {
				trigger = policyDownloadQueue.take();

				loadRoles();
				loadPolicy();
				loadUserStore();
			} catch(InterruptedException excp) {
				LOG.info("PolicyRefresher(serviceName=" + serviceName + ").run(): interrupted! Exiting thread", excp);
				break;
			} finally {
				if (trigger != null) {
					trigger.signalCompletion();
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").run()");
		}
	}

	public void syncPoliciesWithAdmin(DownloadTrigger token) throws InterruptedException {
		policyDownloadQueue.put(token);
		token.waitForCompletion();
	}

	private void loadPolicy() {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").loadPolicy()");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "PolicyRefresher.loadPolicy(serviceName=" + serviceName + ")");
			long freeMemory = Runtime.getRuntime().freeMemory();
			long totalMemory = Runtime.getRuntime().totalMemory();
			PERF_POLICYENGINE_INIT_LOG.debug("In-Use memory: " + (totalMemory-freeMemory) + ", Free memory:" + freeMemory);
		}

		try {
			//load policy from PolicyAdmin
			ServicePolicies svcPolicies = loadPolicyfromPolicyAdmin();

			if (svcPolicies == null) {
				//if Policy fetch from Policy Admin Fails, load from cache
				if (!policiesSetInPlugin) {
					svcPolicies = loadFromCache();
				}
			}

			if (PERF_POLICYENGINE_INIT_LOG.isDebugEnabled()) {
				long freeMemory = Runtime.getRuntime().freeMemory();
				long totalMemory = Runtime.getRuntime().totalMemory();
				PERF_POLICYENGINE_INIT_LOG.debug("In-Use memory: " + (totalMemory - freeMemory) + ", Free memory:" + freeMemory);
			}

			if (svcPolicies != null) {
				plugIn.setPolicies(svcPolicies);
				policiesSetInPlugin = true;
				serviceDefSetInPlugin = false;
				setLastActivationTimeInMillis(System.currentTimeMillis());
				lastKnownVersion = svcPolicies.getPolicyVersion() != null ? svcPolicies.getPolicyVersion() : -1L;
				lastUpdatedTiemInMillis = svcPolicies.getPolicyUpdateTime() != null ? svcPolicies.getPolicyUpdateTime().getTime() : -1L;
			} else {
				if (!policiesSetInPlugin && !serviceDefSetInPlugin) {
					plugIn.setPolicies(null);
					serviceDefSetInPlugin = true;
				}
			}
		} catch (RangerServiceNotFoundException snfe) {
			if (!serviceDefSetInPlugin) {
				disableCache();
				plugIn.setPolicies(null);
				serviceDefSetInPlugin = true;
				setLastActivationTimeInMillis(System.currentTimeMillis());
				lastKnownVersion = -1;
				lastUpdatedTiemInMillis = -1;
			}
		} catch (Exception excp) {
			LOG.error("Encountered unexpected exception, ignoring..", excp);
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").loadPolicy()");
		}
	}

	private ServicePolicies loadPolicyfromPolicyAdmin() throws RangerServiceNotFoundException {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").loadPolicyfromPolicyAdmin()");
		}

		ServicePolicies svcPolicies = null;

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "PolicyRefresher.loadPolicyFromPolicyAdmin(serviceName=" + serviceName + ")");
		}

		try {

			if (serviceName.equals("atlas") && plugIn.getTypeRegistry() != null) {
				RangerRESTUtils restUtils = new RangerRESTUtils();
				CachePolicyTransformerImpl transformer = new CachePolicyTransformerImpl(plugIn.getTypeRegistry(), auditRepository);
				if (lastUpdatedTiemInMillis == -1) {
					svcPolicies = transformer.getPoliciesAll(serviceName,
							restUtils.getPluginId(serviceName, plugIn.getAppId()),
							lastUpdatedTiemInMillis, null);
				} else if (this.enableDeltaBasedRefresh) {
					svcPolicies = transformer.getPoliciesDelta(serviceName,
							restUtils.getPluginId(serviceName, plugIn.getAppId()),
							lastUpdatedTiemInMillis);
				} else {
					svcPolicies = atlasAuthAdminClient.getServicePoliciesIfUpdated(lastUpdatedTiemInMillis);
				}
			} else {
				svcPolicies = atlasAuthAdminClient.getServicePoliciesIfUpdated(lastUpdatedTiemInMillis);
			}

			boolean isUpdated = svcPolicies != null;

			if(isUpdated) {
				long newVersion = svcPolicies.getPolicyVersion() == null ? -1 : svcPolicies.getPolicyVersion().longValue();

				if(!StringUtils.equals(serviceName, svcPolicies.getServiceName())) {
					LOG.warn("PolicyRefresher(serviceName=" + serviceName + "): ignoring unexpected serviceName '" + svcPolicies.getServiceName() + "' in service-store");

					svcPolicies.setServiceName(serviceName);
				}

				LOG.info("PolicyRefresher(serviceName=" + serviceName + "): found updated version. lastKnownVersion=" + lastKnownVersion + "; newVersion=" + newVersion);

			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("PolicyRefresher(serviceName=" + serviceName + ").run(): no update found. lastKnownVersion=" + lastKnownVersion);
				}
			}
		} catch (Exception excp) {
			LOG.error("PolicyRefresher(serviceName=" + serviceName + "): failed to refresh policies. Will continue to use last known version of policies (" + lastKnownVersion + ")", excp);
			svcPolicies = null;
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").loadPolicyfromPolicyAdmin()");
		}

		return svcPolicies;
	}

	private ServicePolicies loadFromCache() {

		ServicePolicies policies = null;

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").loadFromCache()");
		}

		File cacheFile = cacheDir == null ? null : new File(cacheDir + File.separator + cacheFileName);

		if(cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
			Reader reader = null;

			RangerPerfTracer perf = null;

			if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "PolicyRefresher.loadFromCache(serviceName=" + serviceName + ")");
			}

			try {
				reader = new FileReader(cacheFile);

				policies = gson.fromJson(reader, ServicePolicies.class);

				if(policies != null) {
					if(!StringUtils.equals(serviceName, policies.getServiceName())) {
						LOG.warn("ignoring unexpected serviceName '" + policies.getServiceName() + "' in cache file '" + cacheFile.getAbsolutePath() + "'");

						policies.setServiceName(serviceName);
					}

					lastKnownVersion = policies.getPolicyVersion() == null ? -1 : policies.getPolicyVersion().longValue();
					lastUpdatedTiemInMillis = policies.getPolicyUpdateTime() == null ? -1 : policies.getPolicyUpdateTime().getTime();
				}
			} catch (Exception excp) {
				LOG.error("failed to load policies from cache file " + cacheFile.getAbsolutePath(), excp);
			} finally {
				RangerPerfTracer.log(perf);

				if(reader != null) {
					try {
						reader.close();
					} catch(Exception excp) {
						LOG.error("error while closing opened cache file " + cacheFile.getAbsolutePath(), excp);
					}
				}
			}
		} else {
			LOG.warn("cache file does not exist or not readable '" + (cacheFile == null ? null : cacheFile.getAbsolutePath()) + "'");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").loadFromCache()");
		}

		return policies;
	}
	public void saveToCache(ServicePolicies policies) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").saveToCache()");
		}

		if(policies != null) {
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
					perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "PolicyRefresher.saveToCache(serviceName=" + serviceName + ")");
				}

				Writer writer = null;

				try {
					writer = new FileWriter(cacheFile);

					gson.toJson(policies, writer);
				} catch (Exception excp) {
					LOG.error("failed to save policies to cache file '" + cacheFile.getAbsolutePath() + "'", excp);
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
			LOG.info("policies is null. Nothing to save in cache");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").saveToCache()");
		}
	}

	private void disableCache() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher.disableCache(serviceName=" + serviceName + ")");
		}

		File cacheFile = cacheDir == null ? null : new File(cacheDir + File.separator + cacheFileName);

		if(cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
			LOG.warn("Cleaning up local cache");
			String renamedCacheFile = cacheFile.getAbsolutePath() + "_" + System.currentTimeMillis();
			if (!cacheFile.renameTo(new File(renamedCacheFile))) {
				LOG.error("Failed to move " + cacheFile.getAbsolutePath() + " to " + renamedCacheFile);
			} else {
				LOG.warn("Moved " + cacheFile.getAbsolutePath() + " to " + renamedCacheFile);
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No local policy cache found. No need to disable it!");
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher.disableCache(serviceName=" + serviceName + ")");
		}
	}

	private void loadRoles() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").loadRoles()");
		}

		//Load the Ranger UserGroup Roles
		rolesProvider.loadUserGroupRoles(plugIn);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").loadRoles()");
		}
	}

	private void loadUserStore() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").loadGroups()");
		}

		//Load the Ranger UserGroup Roles
		userStoreProvider.loadUserStore(plugIn);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").loadRoles()");
		}
	}
}

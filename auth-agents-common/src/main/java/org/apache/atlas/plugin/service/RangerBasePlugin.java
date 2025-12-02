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

package org.apache.atlas.plugin.service;

import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.atlas.authorizer.store.UsersStore;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertexService;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.audit.provider.AuditHandler;
import org.apache.atlas.audit.provider.AuditProviderFactory;
import org.apache.atlas.authorization.config.RangerPluginConfig;
import org.apache.atlas.authorization.utils.RangerUtil;
import org.apache.atlas.plugin.conditionevaluator.RangerScriptExecutionContext;
import org.apache.atlas.plugin.contextenricher.RangerContextEnricher;
import org.apache.atlas.plugin.contextenricher.RangerTagEnricher;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.model.RangerServiceDef;
import org.apache.atlas.plugin.policyengine.RangerAccessRequest;
import org.apache.atlas.plugin.policyengine.RangerAccessResult;
import org.apache.atlas.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.atlas.plugin.policyengine.RangerPluginContext;
import org.apache.atlas.plugin.policyengine.RangerPolicyEngine;
import org.apache.atlas.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.atlas.plugin.policyengine.RangerResourceACLs;
import org.apache.atlas.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.atlas.plugin.store.ServiceDefsUtil;
import org.apache.atlas.plugin.util.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class RangerBasePlugin {
	private static final Log LOG = LogFactory.getLog(RangerBasePlugin.class);

	private final RangerPluginConfig          pluginConfig;
	private final RangerPluginContext         pluginContext;
	private final Map<String, LogHistory>     logHistoryList = new Hashtable<>();
	private final int                         logInterval    = 30000; // 30 seconds
	private final DownloadTrigger             accessTrigger  = new DownloadTrigger();
	private       PolicyRefresher             refresher;
	private       RangerPolicyEngine          policyEngine;
	private       RangerAuthContext           currentAuthContext;
	private       RangerAccessResultProcessor resultProcessor;
	private       RangerRoles                 roles;
	private       RangerUserStore             userStore;
	private final List<RangerChainedPlugin>   chainedPlugins;
	private 	  AtlasTypeRegistry 		  typeRegistry = null;
	private 	DynamicVertexService dynamicVertexService = null;
	private final UsersStore                  usersStore;
	private final PoliciesStore               policiesStore;


	public RangerBasePlugin(String serviceType, String appId) {
		this(new RangerPluginConfig(serviceType, null, appId, null, null, null));
	}

	public RangerBasePlugin(String serviceType, String serviceName, String appId) {
		this(new RangerPluginConfig(serviceType, serviceName, appId, null, null, null));
	}

	public RangerBasePlugin(String serviceType, String serviceName, AtlasTypeRegistry typeRegistry) {
		this(new RangerPluginConfig(serviceType, serviceName, null, null, null, null));
		this.typeRegistry = typeRegistry;
	}

	public RangerBasePlugin(String serviceType, String serviceName, AtlasTypeRegistry typeRegistry, DynamicVertexService dynamicVertexService) {
		this(new RangerPluginConfig(serviceType, serviceName, null, null, null, null));
		this.typeRegistry = typeRegistry;
		this.dynamicVertexService = dynamicVertexService;
	}

	public RangerBasePlugin(RangerPluginConfig pluginConfig) {
		this.pluginConfig  = pluginConfig;
		this.pluginContext = new RangerPluginContext(pluginConfig);
		this.usersStore = UsersStore.getInstance();
		this.policiesStore = PoliciesStore.getInstance();

		Set<String> superUsers         = toSet(pluginConfig.get(pluginConfig.getPropertyPrefix() + ".super.users"));
		Set<String> superGroups        = toSet(pluginConfig.get(pluginConfig.getPropertyPrefix() + ".super.groups"));
		Set<String> auditExcludeUsers  = toSet(pluginConfig.get(pluginConfig.getPropertyPrefix() + ".audit.exclude.users"));
		Set<String> auditExcludeGroups = toSet(pluginConfig.get(pluginConfig.getPropertyPrefix() + ".audit.exclude.groups"));
		Set<String> auditExcludeRoles  = toSet(pluginConfig.get(pluginConfig.getPropertyPrefix() + ".audit.exclude.roles"));
		Set<String> serviceAdmins      = toSet(pluginConfig.get(pluginConfig.getPropertyPrefix() + ".service.admins"));

		setSuperUsersAndGroups(superUsers, superGroups);
		setAuditExcludedUsersGroupsRoles(auditExcludeUsers, auditExcludeGroups, auditExcludeRoles);
		setIsFallbackSupported(pluginConfig.getBoolean(pluginConfig.getPropertyPrefix() + ".is.fallback.supported", false));
		setServiceAdmins(serviceAdmins);

		RangerScriptExecutionContext.init(pluginConfig);

		this.chainedPlugins = initChainedPlugins();
	}

	public static AuditHandler getAuditProvider(String serviceName) {
		AuditProviderFactory providerFactory = RangerBasePlugin.getAuditProviderFactory(serviceName);
		AuditHandler         ret             = providerFactory.getAuditProvider();

		return ret;
	}

	public String getServiceType() {
		return pluginConfig.getServiceType();
	}

	public String getAppId() {
		return pluginConfig.getAppId();
	}

	public RangerPluginConfig getConfig() {
		return pluginConfig;
	}

	public String getClusterName() {
		return pluginConfig.getClusterName();
	}

	public RangerPluginContext getPluginContext() {
		return pluginContext;
	}

	public RangerAuthContext getCurrentRangerAuthContext() { return currentAuthContext; }

	public List<RangerChainedPlugin> getChainedPlugins() { return chainedPlugins; }

	// For backward compatibility
	public RangerAuthContext createRangerAuthContext() { return currentAuthContext; }

	public RangerRoles getRoles() {
		return this.roles;
	}

	public void setRoles(RangerRoles roles) {
		this.roles = roles;
		this.usersStore.setAllRoles(roles);

		RangerPolicyEngine policyEngine = this.policyEngine;

		if (policyEngine != null) {
			policyEngine.setRoles(roles);
		}

		pluginContext.notifyAuthContextChanged();
	}

	public RangerUserStore getUserStore() {
		return this.userStore;
	}

	public void setUserStore(RangerUserStore userStore) {
		this.userStore = userStore;
		this.usersStore.setUserStore(userStore);
	}

	public void setAuditExcludedUsersGroupsRoles(Set<String> users, Set<String> groups, Set<String> roles) {
		pluginConfig.setAuditExcludedUsersGroupsRoles(users, groups, roles);
	}

	public void setSuperUsersAndGroups(Set<String> users, Set<String> groups) {
		pluginConfig.setSuperUsersGroups(users, groups);
	}

	public void setIsFallbackSupported(boolean isFallbackSupported) {
		pluginConfig.setIsFallbackSupported(isFallbackSupported);
	}

	public void setServiceAdmins(Set<String> users) {
		pluginConfig.setServiceAdmins(users);
	}

	public RangerServiceDef getServiceDef() {
		RangerPolicyEngine policyEngine = this.policyEngine;

		return policyEngine != null ? policyEngine.getServiceDef() : null;
	}

	public int getServiceDefId() {
		RangerServiceDef serviceDef = getServiceDef();

		return serviceDef != null && serviceDef.getId() != null ? serviceDef.getId().intValue() : -1;
	}

	public String getServiceName() {
		return pluginConfig.getServiceName();
	}

	public AtlasTypeRegistry getTypeRegistry() {
		return typeRegistry;
	}

	public void setTypeRegistry(AtlasTypeRegistry typeRegistry) {
		this.typeRegistry = typeRegistry;
	}

	public void init() {
		cleanup();

		AuditProviderFactory providerFactory = AuditProviderFactory.getInstance();

		if (!providerFactory.isInitDone()) {
			if (pluginConfig.getProperties() != null) {
				providerFactory.init(pluginConfig.getProperties(), getAppId());
			} else {
				LOG.error("Audit subsystem is not initialized correctly. Please check audit configuration. ");
				LOG.error("No authorization audits will be generated. ");
			}
		}

		if (!pluginConfig.getPolicyEngineOptions().disablePolicyRefresher) {
			refresher = new PolicyRefresher(this, dynamicVertexService);
			LOG.info("Created PolicyRefresher Thread(" + refresher.getName() + ")");
			refresher.setDaemon(true);
			refresher.startRefresher();
		}

		for (RangerChainedPlugin chainedPlugin : chainedPlugins) {
			chainedPlugin.init();
		}
	}

	public long getPoliciesVersion() {
		RangerPolicyEngine policyEngine = this.policyEngine;
		Long               ret          = policyEngine != null ? policyEngine.getPolicyVersion() : null;

		return ret != null ? ret : -1L;
	}

	public long getTagsVersion() {
		RangerTagEnricher tagEnricher = getTagEnricher();
		Long              ret         = tagEnricher != null ? tagEnricher.getServiceTagsVersion() : null;

		return ret != null ? ret : -1L;
	}

	public long getRolesVersion() {
		RangerPolicyEngine policyEngine = this.policyEngine;
		Long               ret          = policyEngine != null ? policyEngine.getRoleVersion() : null;

		return ret != null ? ret : -1L;
	}

	public void setPolicies(ServicePolicies policies) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> setPolicies(" + policies + ")");
		}

		// guard against catastrophic failure during policy engine Initialization or
		try {
			RangerPolicyEngine oldPolicyEngine = this.policyEngine;
			ServicePolicies    servicePolicies = null;
			boolean            isNewEngineNeeded = true;
			boolean            usePolicyDeltas = false;

			if (policies == null) {
				policies = getDefaultSvcPolicies();

				if (policies == null) {
					LOG.error("Could not get default Service Policies. Keeping old policy-engine!");
					isNewEngineNeeded = false;
				}
			} else {
				Boolean hasPolicyDeltas = RangerPolicyDeltaUtil.hasPolicyDeltas(policies);

				if (hasPolicyDeltas == null) {
					LOG.info("Downloaded policies do not require policy change !! [" + (policies.getPolicies() != null ? policies.getPolicies().size() : 0) + "]");

					if (this.policyEngine == null) {

						LOG.info("There are no material changes, and current policy-engine is null! Creating a policy-engine with default service policies");
						ServicePolicies defaultSvcPolicies = getDefaultSvcPolicies();

						if (defaultSvcPolicies == null) {
							LOG.error("Could not get default Service Policies. Keeping old policy-engine! This is a FATAL error as the old policy-engine is null!");
							isNewEngineNeeded = false;
							throw new RuntimeException("PolicyRefresher("+policies.getServiceName()+").setPolicies: fetched service policies contains no policies or delta and current policy engine is null");
						} else {
							defaultSvcPolicies.setPolicyVersion(policies.getPolicyVersion());
							policies = defaultSvcPolicies;
							isNewEngineNeeded = true;
						}
					} else {
						LOG.info("Keeping old policy-engine!");
						isNewEngineNeeded = false;
					}
				} else {
					if (hasPolicyDeltas.equals(Boolean.TRUE)) {
						// Rebuild policies from deltas
						RangerPolicyEngineImpl policyEngine = (RangerPolicyEngineImpl) oldPolicyEngine;

						servicePolicies = ServicePolicies.applyDelta(policies, policyEngine);

						if (servicePolicies != null) {
							usePolicyDeltas = true;
						} else {
							LOG.error("Could not apply deltas=" + Arrays.toString(policies.getPolicyDeltas().toArray()));
							LOG.warn("Keeping old policy-engine!");
							isNewEngineNeeded = false;
						}
					} else {
						if (policies.getPolicies() == null) {
							policies.setPolicies(new ArrayList<>());
						}
					}
				}
			}

			if (isNewEngineNeeded) {
				RangerPolicyEngine newPolicyEngine      = null;
				boolean            isPolicyEngineShared = false;

				if (!usePolicyDeltas) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Creating engine from policies");
					}

					newPolicyEngine = new RangerPolicyEngineImpl(policies, pluginContext, roles);
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("policy-deltas are not null");
					}

					if (CollectionUtils.isNotEmpty(policies.getPolicyDeltas()) || MapUtils.isNotEmpty(policies.getSecurityZones())) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Non empty policy-deltas found. Cloning engine using policy-deltas");
						}

						if (oldPolicyEngine != null) {
							// Create new evaluator for the updated policies
							newPolicyEngine = new RangerPolicyEngineImpl(servicePolicies, pluginContext, roles);
						}

						if (newPolicyEngine != null) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Applied policyDeltas=" + Arrays.toString(policies.getPolicyDeltas().toArray()) + ")");
							}

							isPolicyEngineShared = true;
						} else {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Failed to apply policyDeltas=" + Arrays.toString(policies.getPolicyDeltas().toArray()) + "), Creating engine from policies");
								LOG.debug("Creating new engine from servicePolicies:[" + servicePolicies + "]");
							}

							newPolicyEngine = new RangerPolicyEngineImpl(servicePolicies, pluginContext, roles);
						}
					} else {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Empty policy-deltas. No need to change policy engine");
						}
					}
				}

				if (newPolicyEngine != null) {
					if (!isPolicyEngineShared) {
						newPolicyEngine.setUseForwardedIPAddress(pluginConfig.isUseForwardedIPAddress());
						newPolicyEngine.setTrustedProxyAddresses(pluginConfig.getTrustedProxyAddresses());
					}

					this.policyEngine       = newPolicyEngine;
					this.currentAuthContext = pluginContext.getAuthContext();

					pluginContext.notifyAuthContextChanged();

					if (oldPolicyEngine != null && oldPolicyEngine != newPolicyEngine) {
						((RangerPolicyEngineImpl) oldPolicyEngine).releaseResources(!isPolicyEngineShared);
					}

					if (this.refresher != null) {
						this.refresher.saveToCache(usePolicyDeltas ? servicePolicies : policies);
					}
					LOG.info("New RangerPolicyEngine created with policy count:"+ (usePolicyDeltas? servicePolicies.getPolicies().size() : policies.getPolicies().size()));

					List<RangerPolicy> abacPolicies = policies.getAbacPolicies() != null ? policies.getAbacPolicies().getPolicies() : new ArrayList<>();
					if (usePolicyDeltas) {
						abacPolicies = servicePolicies.getAbacPolicies() != null ? servicePolicies.getAbacPolicies().getPolicies() : new ArrayList<>();
					}
					this.policiesStore.setAbacPolicies(abacPolicies);
					this.policiesStore.setResourcePolicies(this.policyEngine.getResourcePolicies());
					this.policiesStore.setTagPolicies(this.policyEngine.getTagPolicies());
					LOG.info("PolicyRefresher: ABAC_AUTH: abac policies set: " + abacPolicies.size());
				}

			} else {
				LOG.warn("Leaving current policy engine as-is");
				LOG.warn("Policies are not saved to cache. policyVersion in the policy-cache may be different than in Ranger-admin, even though the policies are the same!");
				LOG.warn("Ranger-PolicyVersion:[" + (policies != null ? policies.getPolicyVersion() : -1L) + "], Cached-PolicyVersion:[" + (this.policyEngine != null ? this.policyEngine.getPolicyVersion() : -1L) + "]");
			}

		} catch (Exception e) {
			LOG.error("setPolicies: Failed to set policies, didn't set policies", e);
			throw e;
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== setPolicies(" + policies + ")");
		}
	}

	public void cleanup() {
		PolicyRefresher refresher = this.refresher;
		this.refresher    = null;

		RangerPolicyEngine policyEngine = this.policyEngine;
		this.policyEngine    = null;

		if (refresher != null) {
			refresher.stopRefresher();
		}

		if (policyEngine != null) {
			((RangerPolicyEngineImpl) policyEngine).releaseResources(true);
		}
	}

	public void setResultProcessor(RangerAccessResultProcessor resultProcessor) {
		this.resultProcessor = resultProcessor;
	}

	public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
		return isAccessAllowed(request, resultProcessor);
	}

	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests) {
		return isAccessAllowed(requests, resultProcessor);
	}

	public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		RangerAccessResult ret          = null;
		RangerPolicyEngine policyEngine = this.policyEngine;

		if (policyEngine != null) {
			ret = policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ACCESS, null);
		}

		if (ret != null) {
			for (RangerChainedPlugin chainedPlugin : chainedPlugins) {
				RangerAccessResult chainedResult = chainedPlugin.isAccessAllowed(request);

				if (chainedResult != null) {
					updateResultFromChainedResult(ret, chainedResult);
				}
			}

		}

		if (policyEngine != null) {
			policyEngine.evaluateAuditPolicies(ret);
		}

		if (resultProcessor != null) {
			resultProcessor.processResult(ret);
		}

		return ret;
	}

	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor) {
		Collection<RangerAccessResult> ret          = null;
		RangerPolicyEngine             policyEngine = this.policyEngine;

		if (policyEngine != null) {
			ret = policyEngine.evaluatePolicies(requests, RangerPolicy.POLICY_TYPE_ACCESS, null);
		}

		if (CollectionUtils.isNotEmpty(ret)) {
			for (RangerChainedPlugin chainedPlugin : chainedPlugins) {
				Collection<RangerAccessResult> chainedResults = chainedPlugin.isAccessAllowed(requests);

				if (CollectionUtils.isNotEmpty(chainedResults)) {
					Iterator<RangerAccessResult> iterRet            = ret.iterator();
					Iterator<RangerAccessResult> iterChainedResults = chainedResults.iterator();

					while (iterRet.hasNext() && iterChainedResults.hasNext()) {
						RangerAccessResult result        = iterRet.next();
						RangerAccessResult chainedResult = iterChainedResults.next();

						if (result != null && chainedResult != null) {
							updateResultFromChainedResult(result, chainedResult);
						}
					}
				}
			}
		}

		if (policyEngine != null && CollectionUtils.isNotEmpty(ret)) {
			for (RangerAccessResult result : ret) {
				policyEngine.evaluateAuditPolicies(result);
			}
		}

		if (resultProcessor != null) {
			resultProcessor.processResults(ret);
		}

		return ret;
	}

	public RangerAccessResult getAssetAccessors(RangerAccessRequest request) {
		RangerAccessResult ret          = null;
		RangerPolicyEngine policyEngine = this.policyEngine;

		if (policyEngine != null) {
			ret = policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ACCESS, null);
		}

		return ret;
	}

	public RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;
		RangerAccessResult ret          = null;

		if(policyEngine != null) {
			ret = policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_DATAMASK, resultProcessor);

			policyEngine.evaluateAuditPolicies(ret);
		}

		return ret;
	}

	public RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;
		RangerAccessResult ret          = null;

		if(policyEngine != null) {
			ret = policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ROWFILTER, resultProcessor);

			policyEngine.evaluateAuditPolicies(ret);
		}

		return ret;
	}

	public Set<String> getRolesFromUserAndGroups(String user, Set<String> groups) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.getRolesFromUserAndGroups(user, groups);
		}

		return null;
	}

	private RangerServiceDef getDefaultServiceDef() {
		RangerServiceDef ret = null;

		if (StringUtils.isNotBlank(getServiceType())) {
			try {
				ret = ServiceDefsUtil.instance().getEmbeddedServiceDef(getServiceType());
			} catch (Exception exp) {
				LOG.error("Could not get embedded service-def for " + getServiceType());
			}
		}
		return ret;
	}

	private ServicePolicies getDefaultSvcPolicies() {
		ServicePolicies  ret        = null;
		RangerServiceDef serviceDef = getServiceDef();

		if (serviceDef == null) {
			serviceDef = getDefaultServiceDef();
		}

		if (serviceDef != null) {
			ret = new ServicePolicies();

			ret.setServiceDef(serviceDef);
			ret.setServiceName(getServiceName());
			ret.setPolicies(new ArrayList<RangerPolicy>());
		}

		return ret;
	}

	private Set<String> toSet(String value) {
		return StringUtils.isNotBlank(value) ? RangerUtil.toSet(value) : Collections.emptySet();
	}

	static private final class LogHistory {
		long lastLogTime;
		int counter;
	}

	public RangerTagEnricher getTagEnricher() {
		RangerTagEnricher ret         = null;
		RangerAuthContext authContext = getCurrentRangerAuthContext();

		if (authContext != null) {
			Map<RangerContextEnricher, Object> contextEnricherMap = authContext.getRequestContextEnrichers();

			if (MapUtils.isNotEmpty(contextEnricherMap)) {
				Set<RangerContextEnricher> contextEnrichers = contextEnricherMap.keySet();

				for (RangerContextEnricher enricher : contextEnrichers) {
					if (enricher instanceof RangerTagEnricher) {
						ret = (RangerTagEnricher) enricher;

						break;
					}
				}
			}
		}
		return ret;
	}

	private List<RangerChainedPlugin> initChainedPlugins() {
		List<RangerChainedPlugin> ret                      = new ArrayList<>();
		String                    chainedServicePropPrefix = pluginConfig.getPropertyPrefix() + ".chained.services";

		for (String chainedService : RangerUtil.toList(pluginConfig.get(chainedServicePropPrefix))) {
			if (StringUtils.isBlank(chainedService)) {
				continue;
			}

			String className = pluginConfig.get(chainedServicePropPrefix + "." + chainedService + ".impl");

			if (StringUtils.isBlank(className)) {
				LOG.error("Ignoring chained service " + chainedService + ": no impl class specified");

				continue;
			}

			try {
				@SuppressWarnings("unchecked")
				Class<RangerChainedPlugin> pluginClass   = (Class<RangerChainedPlugin>) Class.forName(className);
				RangerChainedPlugin        chainedPlugin = pluginClass.getConstructor(RangerBasePlugin.class, String.class).newInstance(this, chainedService);

				ret.add(chainedPlugin);
			} catch (Throwable t) {
				LOG.error("initChainedPlugins(): error instantiating plugin impl " + className, t);
			}
		}

		return ret;
	}

	private void updateResultFromChainedResult(RangerAccessResult result, RangerAccessResult chainedResult) {
		boolean overrideResult = false;

		if (chainedResult.getIsAccessDetermined()) { // only if chained-result is definitive
			// override if result is not definitive or chained-result is by a higher priority policy
			overrideResult = !result.getIsAccessDetermined() || chainedResult.getPolicyPriority() > result.getPolicyPriority();

			if (!overrideResult) {
				// override if chained-result is from the same policy priority, and if denies access
				if (chainedResult.getPolicyPriority() == result.getPolicyPriority() && !chainedResult.getIsAllowed()) {
					// let's not override if result is already denied
					if (result.getIsAllowed()) {
						overrideResult = true;
					}
				}
			}
		}

		if (overrideResult) {
			result.setIsAllowed(chainedResult.getIsAllowed());
			result.setIsAccessDetermined(chainedResult.getIsAccessDetermined());
			result.setPolicyId(chainedResult.getPolicyId());
			result.setPolicyVersion(chainedResult.getPolicyVersion());
			result.setPolicyPriority(chainedResult.getPolicyPriority());
			result.setZoneName(chainedResult.getZoneName());
		}

		if (!result.getIsAuditedDetermined() && chainedResult.getIsAuditedDetermined()) {
			result.setIsAudited(chainedResult.getIsAudited());
			result.setAuditPolicyId(chainedResult.getAuditPolicyId());
		}
	}

	private static void overrideACLs(final RangerResourceACLs chainedResourceACLs, RangerResourceACLs baseResourceACLs, final RangerRolesUtil.ROLES_FOR userType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.overrideACLs(isUser=" + userType.name() + ")");
		}
		Map<String, Map<String, RangerResourceACLs.AccessResult>> chainedACLs = null;
		Map<String, Map<String, RangerResourceACLs.AccessResult>> baseACLs    = null;

		switch (userType) {
			case USER:
				chainedACLs = chainedResourceACLs.getUserACLs();
				baseACLs    = baseResourceACLs.getUserACLs();
				break;
			case GROUP:
				chainedACLs = chainedResourceACLs.getGroupACLs();
				baseACLs    = baseResourceACLs.getGroupACLs();
				break;
			case ROLE:
				chainedACLs = chainedResourceACLs.getRoleACLs();
				baseACLs    = baseResourceACLs.getRoleACLs();
				break;
			default:
				break;
		}

		for (Map.Entry<String, Map<String, RangerResourceACLs.AccessResult>> chainedPermissionsMap : chainedACLs.entrySet()) {
			String                                       name               = chainedPermissionsMap.getKey();
			Map<String, RangerResourceACLs.AccessResult> chainedPermissions = chainedPermissionsMap.getValue();
			Map<String, RangerResourceACLs.AccessResult> basePermissions    = baseACLs.get(name);

			for (Map.Entry<String, RangerResourceACLs.AccessResult> chainedPermission : chainedPermissions.entrySet()) {
				String chainedAccessType                            = chainedPermission.getKey();
				RangerResourceACLs.AccessResult chainedAccessResult = chainedPermission.getValue();
				RangerResourceACLs.AccessResult baseAccessResult    = basePermissions == null ? null : basePermissions.get(chainedAccessType);

				final boolean useChainedAccessResult;

				if (baseAccessResult == null) {
					useChainedAccessResult = true;
				} else {
					if (chainedAccessResult.getPolicy().getPolicyPriority() > baseAccessResult.getPolicy().getPolicyPriority()) {
						useChainedAccessResult = true;
					} else if (chainedAccessResult.getPolicy().getPolicyPriority().equals(baseAccessResult.getPolicy().getPolicyPriority())) {
						if (chainedAccessResult.getResult() == baseAccessResult.getResult()) {
							useChainedAccessResult = true;
						} else {
							useChainedAccessResult = chainedAccessResult.getResult() == RangerPolicyEvaluator.ACCESS_DENIED;
						}
					} else { // chainedAccessResult.getPolicy().getPolicyPriority() < baseAccessResult.getPolicy().getPolicyPriority()
						useChainedAccessResult = false;
					}
				}

				final RangerResourceACLs.AccessResult finalAccessResult = useChainedAccessResult ? chainedAccessResult : baseAccessResult;

				switch (userType) {
					case USER:
						baseResourceACLs.setUserAccessInfo(name, chainedAccessType, finalAccessResult.getResult(), finalAccessResult.getPolicy());
						break;
					case GROUP:
						baseResourceACLs.setGroupAccessInfo(name, chainedAccessType, finalAccessResult.getResult(), finalAccessResult.getPolicy());
						break;
					case ROLE:
						baseResourceACLs.setRoleAccessInfo(name, chainedAccessType, finalAccessResult.getResult(), finalAccessResult.getPolicy());
						break;
					default:
						break;
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.mergeACLsOneWay(isUser=" + userType.name() + ")");
		}
	}

	private static AuditProviderFactory getAuditProviderFactory(String serviceName) {
		AuditProviderFactory ret = AuditProviderFactory.getInstance();

		if (ret == null || !ret.isInitDone()) {
			LOG.error("AuditProviderFactory not configured properly, will not log authz events");
		}

		return ret;
	}
}

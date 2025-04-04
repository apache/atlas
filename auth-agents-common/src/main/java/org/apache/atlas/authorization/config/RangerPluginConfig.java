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

package org.apache.atlas.authorization.config;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.atlas.authorization.utils.RangerUtil;
import org.apache.atlas.plugin.policyengine.RangerPolicyEngineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class RangerPluginConfig extends RangerConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPluginConfig.class);

    private static final char RANGER_TRUSTED_PROXY_IPADDRESSES_SEPARATOR_CHAR = ',';

    private final String                    serviceType;
    private final String                    serviceName;
    private final String                    appId;
    private final String                    clusterName;
    private final String                    clusterType;
    private final RangerPolicyEngineOptions policyEngineOptions;
    private final boolean                   useForwardedIPAddress;
    private final String[]                  trustedProxyAddresses;
    private final String                    propertyPrefix;
    private       boolean                   isFallbackSupported;
    private       Set<String>               auditExcludedUsers  = Collections.emptySet();
    private       Set<String>               auditExcludedGroups = Collections.emptySet();
    private       Set<String>               auditExcludedRoles  = Collections.emptySet();
    private       Set<String>               superUsers          = Collections.emptySet();
    private       Set<String>               superGroups         = Collections.emptySet();
    private       Set<String>               serviceAdmins       = Collections.emptySet();


    public RangerPluginConfig(String serviceType, String serviceName, String appId, String clusterName, String clusterType, RangerPolicyEngineOptions policyEngineOptions) {
        super();

        this.serviceType    = serviceType;
        this.appId          = StringUtils.isEmpty(appId) ? serviceType : appId;
        this.propertyPrefix = "atlas.plugin." + serviceType;
        this.serviceName    = StringUtils.isEmpty(serviceName) ? this.get(propertyPrefix + ".service.name") : serviceName;

        addResourcesForServiceName(this.serviceType, this.serviceName);

        String trustedProxyAddressString = this.get(propertyPrefix + ".trusted.proxy.ipaddresses");

        if (RangerUtil.isEmpty(clusterName)) {
            clusterName = this.get(propertyPrefix + ".access.cluster.name", "");

            if (RangerUtil.isEmpty(clusterName)) {
                clusterName = this.get(propertyPrefix + ".ambari.cluster.name", "");
            }
        }

        if (RangerUtil.isEmpty(clusterType)) {
            clusterType = this.get(propertyPrefix + ".access.cluster.type", "");

            if (RangerUtil.isEmpty(clusterType)) {
                clusterType = this.get(propertyPrefix + ".ambari.cluster.type", "");
            }
        }

        this.clusterName           = clusterName;
        this.clusterType           = clusterType;
        this.useForwardedIPAddress = this.getBoolean(propertyPrefix + ".use.x-forwarded-for.ipaddress", false);
        this.trustedProxyAddresses = StringUtils.split(trustedProxyAddressString, RANGER_TRUSTED_PROXY_IPADDRESSES_SEPARATOR_CHAR);

        if (trustedProxyAddresses != null) {
            for (int i = 0; i < trustedProxyAddresses.length; i++) {
                trustedProxyAddresses[i] = trustedProxyAddresses[i].trim();
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(propertyPrefix + ".use.x-forwarded-for.ipaddress:" + useForwardedIPAddress);
            LOG.debug(propertyPrefix + ".trusted.proxy.ipaddresses:[" + StringUtils.join(trustedProxyAddresses, ", ") + "]");
        }

        if (useForwardedIPAddress && StringUtils.isBlank(trustedProxyAddressString)) {
            LOG.warn("Property " + propertyPrefix + ".use.x-forwarded-for.ipaddress" + " is set to true, and Property " + propertyPrefix + ".trusted.proxy.ipaddresses" + " is not set");
            LOG.warn("Ranger plugin will trust RemoteIPAddress and treat first X-Forwarded-Address in the access-request as the clientIPAddress");
        }

        if (policyEngineOptions == null) {
            policyEngineOptions = new RangerPolicyEngineOptions();

            policyEngineOptions.configureForPlugin(this, propertyPrefix);
        }

        this.policyEngineOptions = policyEngineOptions;

        LOG.info(policyEngineOptions.toString());
    }

    public String getServiceType() {
        return serviceType;
    }

    public String getAppId() {
        return appId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getClusterType() {
        return clusterType;
    }

    public boolean isUseForwardedIPAddress() {
        return useForwardedIPAddress;
    }

    public String[] getTrustedProxyAddresses() {
        return trustedProxyAddresses;
    }

    public String getPropertyPrefix() {
        return propertyPrefix;
    }

    public boolean getIsFallbackSupported() {
        return isFallbackSupported;
    }

    public void setIsFallbackSupported(boolean isFallbackSupported) {
        this.isFallbackSupported = isFallbackSupported;
    }

    public RangerPolicyEngineOptions getPolicyEngineOptions() {
        return policyEngineOptions;
    }

    public void setAuditExcludedUsersGroupsRoles(Set<String> users, Set<String> groups, Set<String> roles) {
        auditExcludedUsers  = CollectionUtils.isEmpty(users) ? Collections.emptySet() : new HashSet<>(users);
        auditExcludedGroups = CollectionUtils.isEmpty(groups) ? Collections.emptySet() : new HashSet<>(groups);
        auditExcludedRoles  = CollectionUtils.isEmpty(roles) ? Collections.emptySet() : new HashSet<>(roles);

        if (LOG.isDebugEnabled()) {
            LOG.debug("auditExcludedUsers=" + auditExcludedUsers + ", auditExcludedGroups=" + auditExcludedGroups + ", auditExcludedRoles=" + auditExcludedRoles);
        }
    }

    public void setSuperUsersGroups(Set<String> users, Set<String> groups) {
        superUsers  = CollectionUtils.isEmpty(users) ? Collections.emptySet() : new HashSet<>(users);
        superGroups = CollectionUtils.isEmpty(groups) ? Collections.emptySet() : new HashSet<>(groups);

        if (LOG.isDebugEnabled()) {
            LOG.debug("superUsers=" + superUsers + ", superGroups=" + superGroups);
        }
    }

    public void setServiceAdmins(Set<String> users) {
        serviceAdmins  = CollectionUtils.isEmpty(users) ? Collections.emptySet() : new HashSet<>(users);
    }

    public boolean isAuditExcludedUser(String userName) {
        return auditExcludedUsers.contains(userName);
    }

    public boolean hasAuditExcludedGroup(Set<String> userGroups) {
        return userGroups != null && userGroups.size() > 0 && auditExcludedGroups.size() > 0 && CollectionUtils.containsAny(userGroups, auditExcludedGroups);
    }

    public boolean hasAuditExcludedRole(Set<String> userRoles) {
        return userRoles != null && userRoles.size() > 0 && auditExcludedRoles.size() > 0 && CollectionUtils.containsAny(userRoles, auditExcludedRoles);
    }

    public boolean isSuperUser(String userName) {
        return superUsers.contains(userName);
    }

    public boolean hasSuperGroup(Set<String> userGroups) {
        return userGroups != null && userGroups.size() > 0 && superGroups.size() > 0 && CollectionUtils.containsAny(userGroups, superGroups);
    }

    public boolean isServiceAdmin(String userName) {
        return serviceAdmins.contains(userName);
    }

    // load service specific config overrides, if config files are available
    private void addResourcesForServiceName(String serviceType, String serviceName) {
        if (StringUtils.isNotBlank(serviceType) && StringUtils.isNotBlank(serviceName)) {
            String serviceAuditCfg    = "atlas-" + serviceName + "-audit.xml";
            String serviceSecurityCfg = "atlas-" + serviceName + "-security.xml";
            String serviceSslCfg      = "atlas-" + serviceName + "-policymgr-ssl.xml";

            addResourceIfReadable(serviceAuditCfg);
            addResourceIfReadable(serviceSecurityCfg);
            addResourceIfReadable(serviceSslCfg);
        }
    }
}

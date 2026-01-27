/**
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.authorizer;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAccessRequest;
import org.apache.atlas.authorize.AtlasAccessResult;
import org.apache.atlas.authorize.AtlasAccessorResponse;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasAuthorizationException;
import org.apache.atlas.authorize.AtlasAuthorizer;
import org.apache.atlas.authorize.AtlasAuthorizerFactory;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.authorize.AtlasSearchResultScrubRequest;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.authorize.AtlasTypesDefFilterRequest;
import org.apache.atlas.authorizer.authorizers.ListAuthorizer;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics.MetricRecorder;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.servlet.http.HttpServletRequest;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.SERVICE_DEF_ATLAS;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.isABACAuthorizerEnabled;
import static org.apache.atlas.constants.RangerAtlasConstants.READ_RESTRICTION_LEVEL_FULL;
import static org.apache.atlas.repository.Constants.*;

public class AtlasAuthorizationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorizationUtils.class);
    public static final String READ_RESTRICTION_LEVEL = AtlasConfiguration.READ_RESTRICTION_LEVEL.getString();

    public static boolean isFullRestrictionConfigured() {
        return READ_RESTRICTION_LEVEL.equals(READ_RESTRICTION_LEVEL_FULL);
    }

    public static void verifyAccess(AtlasAdminAccessRequest request, Object... errorMsgParams) throws AtlasBaseException {
        if (! isAccessAllowed(request)) {
            String message = (errorMsgParams != null && errorMsgParams.length > 0) ? StringUtils.join(errorMsgParams) : "";

            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, request.getUser(), message);
        }
    }

    public static void verifyAccess(AtlasTypeAccessRequest request, Object... errorMsgParams) throws AtlasBaseException {
        if (! isAccessAllowed(request)) {
            String message = (errorMsgParams != null && errorMsgParams.length > 0) ? StringUtils.join(errorMsgParams) : "";

            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, request.getUser(), message);
        }
    }

    public static void verifyUpdateEntityAccess(AtlasTypeRegistry typeRegistry, AtlasEntityHeader entityHeader, String message) throws AtlasBaseException {
        if (!SKIP_UPDATE_AUTH_CHECK_TYPES.contains(entityHeader.getTypeName())) {
            AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, entityHeader);
            verifyAccess(request, message);
        }
    }

    public static void verifyDeleteEntityAccess(AtlasTypeRegistry typeRegistry, AtlasEntityHeader entityHeader, String message) throws AtlasBaseException {
        if (!SKIP_DELETE_AUTH_CHECK_TYPES.contains(entityHeader.getTypeName())) {
            AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_DELETE, entityHeader);
            verifyAccess(request, message);
        }
    }

    public static void verifyAccess(AtlasEntityAccessRequest request, Object... errorMsgParams) throws AtlasBaseException {
        if (! isAccessAllowed(request)) {
            String message = (errorMsgParams != null && errorMsgParams.length > 0) ? StringUtils.join(errorMsgParams) : "";

            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, request.getUser(), message);
        }
    }

    public static void verifyAccess(AtlasRelationshipAccessRequest request, Object... errorMsgParams) throws AtlasBaseException {
        if (!isAccessAllowed(request)) {
            String message = (errorMsgParams != null && errorMsgParams.length > 0) ? StringUtils.join(errorMsgParams) : "";
            if (StringUtils.isEmpty(message)){
                String end1Type = request.getEnd1Entity() != null ? request.getEnd1Entity().getTypeName() : null;
                String end2Type = request.getEnd2Entity() != null ? request.getEnd2Entity().getTypeName() : null;
                
                boolean isTermToAsset = ATLAS_GLOSSARY_TERM_ENTITY_TYPE.equals(end1Type) &&
                                        end2Type != null &&
                                        !end2Type.equals(ATLAS_GLOSSARY_TERM_ENTITY_TYPE) &&
                                        !end2Type.equals(ATLAS_GLOSSARY_ENTITY_TYPE) &&
                                        !end2Type.equals(ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE) &&
                                        !end2Type.equals(LINK_ENTITY_TYPE) &&
                                        !end2Type.equals(README_ENTITY_TYPE);

                boolean isAssetToTerm = ATLAS_GLOSSARY_TERM_ENTITY_TYPE.equals(end2Type) &&
                                        end1Type != null &&
                                        !end1Type.equals(ATLAS_GLOSSARY_TERM_ENTITY_TYPE) &&
                                        !end1Type.equals(ATLAS_GLOSSARY_ENTITY_TYPE) &&
                                        !end1Type.equals(ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE) &&
                                        !end1Type.equals(LINK_ENTITY_TYPE) &&
                                        !end1Type.equals(README_ENTITY_TYPE);
                
                if (isTermToAsset || isAssetToTerm) {
                    AtlasEntityHeader assetEntity = isTermToAsset ? request.getEnd2Entity() : request.getEnd1Entity();
                    String assetName = (String) assetEntity.getAttribute(NAME);
                    message = String.format("update on asset '%s'", assetName);
                }
                
                Map<String, String> errorMap = new HashMap<>();
                errorMap.put("action", request.getAction().toString());
                errorMap.put("end1", request.getEnd1Entity().getGuid());
                errorMap.put("end2", request.getEnd2Entity().getGuid());

                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, errorMap, request.getUser(), message);

            } else {
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, request.getUser(), message);
            }
        }
    }

    public static void scrubSearchResults(AtlasSearchResultScrubRequest request, boolean suppressLogs) throws AtlasBaseException {
        String userName = getCurrentUserName();

        if (StringUtils.isNotEmpty(userName)) {
            try {
                AtlasAuthorizer authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();

                request.setUser(userName, getCurrentUserGroups());
                request.setClientIPAddress(RequestContext.get().getClientIPAddress());
                request.setForwardedAddresses(RequestContext.get().getForwardedAddresses());
                request.setRemoteIPAddress(RequestContext.get().getClientIPAddress());

                boolean isScrubAuditEnabled = !suppressLogs;

                authorizer.scrubSearchResults(request, isScrubAuditEnabled);
            } catch (AtlasAuthorizationException e) {
                LOG.error("Unable to obtain AtlasAuthorizer", e);
            }
        }
    }

    public static boolean isAccessAllowed(AtlasAdminAccessRequest request) {
        MetricRecorder metric = RequestContext.get().startMetricRecord("isAccessAllowed");

        boolean ret      = false;
        String  userName = getCurrentUserName();

        if (StringUtils.isNotEmpty(userName)) {
            try {
                AtlasAuthorizer authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();

                request.setUser(userName, getCurrentUserGroups());
                request.setClientIPAddress(RequestContext.get().getClientIPAddress());
                request.setForwardedAddresses(RequestContext.get().getForwardedAddresses());
                request.setRemoteIPAddress(RequestContext.get().getClientIPAddress());
                AtlasAccessResult atlasPoliciesResult = authorizer.isAccessAllowed(request);

                ret = atlasPoliciesResult.isAllowed();
            } catch (AtlasAuthorizationException e) {
                LOG.error("Unable to obtain AtlasAuthorizer", e);
            }
        } else {
            ret = true;
        }

        RequestContext.get().endMetricRecord(metric);

        return ret;
    }

    public static boolean isAccessAllowed(AtlasEntityAccessRequest request) {
        return isAccessAllowed(request, true);
    }

    public static boolean isAccessAllowed(AtlasEntityAccessRequest request, boolean isAuditLogEnabled) {
        MetricRecorder metric = RequestContext.get().startMetricRecord("isAccessAllowed");

        String  userName = getCurrentUserName();

        if (StringUtils.isNotEmpty(userName) && !RequestContext.get().isImportInProgress()) {
            try {
                AtlasAuthorizer authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();

                request.setUser(getCurrentUserName(), getCurrentUserGroups());
                request.setClientIPAddress(RequestContext.get().getClientIPAddress());
                request.setForwardedAddresses(RequestContext.get().getForwardedAddresses());
                request.setRemoteIPAddress(RequestContext.get().getClientIPAddress());
                AtlasAccessResult atlasPoliciesResult =  authorizer.isAccessAllowed(request, isAuditLogEnabled);

                RequestContext.get().endMetricRecord(metric);
                if (!isABACAuthorizerEnabled()) {
                    return atlasPoliciesResult.isAllowed();
                }

                metric = RequestContext.get().startMetricRecord("isAccessAllowed.abac");
                AtlasAccessResult finalResult = new AtlasAccessResult(false);

                try {
                    // if priority is override, then it's an explicit deny as implicit deny won't have priority set to override
                    if (!atlasPoliciesResult.isAllowed() && atlasPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                        finalResult = atlasPoliciesResult;
                        return finalResult.isAllowed();
                    }

                    AtlasAccessResult abacPoliciesResult = ABACAuthorizerUtils.isAccessAllowed(request.getEntity(), request.getAction());

                    /* reference - https://docs.google.com/spreadsheets/d/1npyX1cpm8-a8LwzmObgf8U1hZh6bO7FF8cpXjHMMQ08/edit?usp=sharing
                     * Result of Atlas policy engine and ABAC evaluator is merged with below priority
                     * Decision hierarchy (highest to lowest precedence):
                     * 1. Override priority with explicit deny
                     * 2. Override priority allow
                     * 3. Explicit deny (normal priority)
                     * 4. Normal priority allow
                     * 5. Implicit deny
                     */
                    if (!atlasPoliciesResult.isAllowed()) {
                        // Atlas DENY
                        if (atlasPoliciesResult.isExplicitDeny()) {
                            // Matrix row: DENY + DENY (explicit) case
                            if (abacPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                                finalResult = abacPoliciesResult; // ABAC override DENY or ALLOW
                            } else {
                                finalResult = atlasPoliciesResult; // Atlas DENY takes precedence
                            }
                        } else {
                            finalResult = abacPoliciesResult; // Not explicit deny by Atlas, use ABAC result
                        }
                    } else {
                        // Atlas ALLOW
                        if (atlasPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                            // Matrix rows: ALLOW (override) vs ABAC deny/allow
                            if (abacPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE && !abacPoliciesResult.isAllowed()) {
                                finalResult = abacPoliciesResult; // ABAC override DENY wins
                            } else {
                                finalResult = atlasPoliciesResult; // Atlas override ALLOW wins
                            }
                        } else {
                            // Atlas ALLOW (normal)
                            if (abacPoliciesResult.isExplicitDeny()) {
                                finalResult = abacPoliciesResult; // ABAC explicit DENY wins
                            } else if (abacPoliciesResult.isAllowed() && abacPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                                finalResult = abacPoliciesResult; // ABAC override ALLOW wins
                            } else {
                                finalResult = atlasPoliciesResult; // Atlas normal ALLOW wins
                            }
                        }
                    }

                    return finalResult.isAllowed();
                } finally {
                    // log final result audit
                    NewAtlasAuditHandler auditHandler = new NewAtlasAuditHandler(request, SERVICE_DEF_ATLAS);
                    try {
                        finalResult.setEnforcer("merged_auth");
                        auditHandler.processResult(finalResult, request);
                    } finally {
                        auditHandler.flushAudit();
                    }

                    RequestContext.get().endMetricRecord(metric);
                }

            } catch (AtlasAuthorizationException e) {
                LOG.error("Unable to obtain AtlasAuthorizer", e);
            }
        } else {
            return true;
        }

        LOG.warn("ABAC_AUTH: authorizer returning false by default, no case matched");
        return false;
    }

    public static boolean isAccessAllowed(AtlasTypeAccessRequest request) {
        MetricRecorder metric = RequestContext.get().startMetricRecord("isAccessAllowed");

        boolean ret      = false;
        String  userName = getCurrentUserName();

        if (StringUtils.isNotEmpty(userName) && !RequestContext.get().isImportInProgress()) {
            try {
                AtlasAuthorizer authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();

                request.setUser(getCurrentUserName(), getCurrentUserGroups());
                request.setClientIPAddress(RequestContext.get().getClientIPAddress());
                request.setForwardedAddresses(RequestContext.get().getForwardedAddresses());
                request.setRemoteIPAddress(RequestContext.get().getClientIPAddress());
                AtlasAccessResult atlasPoliciesResult = authorizer.isAccessAllowed(request);

                ret = atlasPoliciesResult.isAllowed();
            } catch (AtlasAuthorizationException e) {
                LOG.error("Unable to obtain AtlasAuthorizer", e);
            }
        } else {
            ret = true;
        }

        RequestContext.get().endMetricRecord(metric);

        return ret;
    }

    public static boolean isAccessAllowed(AtlasRelationshipAccessRequest request) {
        MetricRecorder metric = RequestContext.get().startMetricRecord("isAccessAllowed");

        String  userName = getCurrentUserName();

        if (StringUtils.isNotEmpty(userName) && !RequestContext.get().isImportInProgress()) {
            try {
                AtlasAuthorizer authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();

                request.setUser(getCurrentUserName(), getCurrentUserGroups());
                request.setClientIPAddress(RequestContext.get().getClientIPAddress());
                request.setForwardedAddresses(RequestContext.get().getForwardedAddresses());
                request.setRemoteIPAddress(RequestContext.get().getClientIPAddress());
                AtlasAccessResult atlasPoliciesResult = authorizer.isAccessAllowed(request);

                RequestContext.get().endMetricRecord(metric);
                if (!isABACAuthorizerEnabled()) {
                    return atlasPoliciesResult.isAllowed();
                }

                metric = RequestContext.get().startMetricRecord("isAccessAllowed.abac");
                AtlasAccessResult finalResult = new AtlasAccessResult(false);

                try {
                    if (!atlasPoliciesResult.isAllowed() && atlasPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                        // Deny with higher priority
                        finalResult = atlasPoliciesResult;
                        return finalResult.isAllowed();
                    }

                    AtlasAccessResult abacPoliciesResult = ABACAuthorizerUtils.isAccessAllowed(request.getRelationshipType(),
                            request.getEnd1Entity(),
                            request.getEnd2Entity(),
                            request.getAction());

                    // reference - https://docs.google.com/spreadsheets/d/1npyX1cpm8-a8LwzmObgf8U1hZh6bO7FF8cpXjHMMQ08/edit?usp=sharing
                    if (!atlasPoliciesResult.isAllowed()) {
                        // Atlas DENY
                        if (atlasPoliciesResult.isExplicitDeny()) {
                            finalResult = abacPoliciesResult.isAllowed() && abacPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE
                                ? abacPoliciesResult : atlasPoliciesResult; // explicit deny unless abac allow with higher priority
                        } else {
                            finalResult = abacPoliciesResult; // not explicit, so whatever is the second authorizer result
                        }
                    } else {
                        if (atlasPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                            // Atlas allow with higher priority
                            finalResult = !abacPoliciesResult.isAllowed() && abacPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE
                                ? abacPoliciesResult : atlasPoliciesResult; // atlas wins unless allow abac denies with higher priority
                        } else {
                            // Atlas allows, so check if abac denies
                            if (abacPoliciesResult.isExplicitDeny()) {
                                finalResult = abacPoliciesResult;
                            } else {
                                finalResult = atlasPoliciesResult;
                            }
                        }
                    }
                    return finalResult.isAllowed();

                } finally {
                    // log final result audit
                    NewAtlasAuditHandler auditHandler = new NewAtlasAuditHandler(request, SERVICE_DEF_ATLAS);
                    try {
                        finalResult.setEnforcer("merged_auth");
                        auditHandler.processResult(finalResult, request);
                    } finally {
                        auditHandler.flushAudit();
                    }

                    RequestContext.get().endMetricRecord(metric);
                }

            } catch (AtlasAuthorizationException e) {
                LOG.error("Unable to obtain AtlasAuthorizer", e);
            }
        } else {
            return true;
        }

        return false;
    }

    public static AtlasAccessorResponse getAccessors(AtlasEntityAccessRequest request) {
        AtlasAccessorResponse ret = null;

        try {
            AtlasAuthorizer authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();
            setAuthInfo(request);

            ret = authorizer.getAccessors(request);
        } catch (AtlasAuthorizationException e) {
            LOG.error("Unable to obtain AtlasAuthorizer", e);
        }

        AtlasAccessorResponse abacAccessors = ABACAuthorizerUtils.getAccessors(request);
        if (abacAccessors != null) {
            ret.getUsers().addAll(abacAccessors.getUsers());
            ret.getRoles().addAll(abacAccessors.getRoles());
            ret.getGroups().addAll(abacAccessors.getGroups());

            ret.getDenyUsers().addAll(abacAccessors.getDenyUsers());
            ret.getDenyGroups().addAll(abacAccessors.getDenyGroups());
            ret.getDenyRoles().addAll(abacAccessors.getDenyRoles());
        }

        return ret;
    }

    public static AtlasAccessorResponse getAccessors(AtlasRelationshipAccessRequest request) {
        AtlasAccessorResponse ret = null;

        try {
            AtlasAuthorizer authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();
            setAuthInfo(request);

            ret = authorizer.getAccessors(request);
        } catch (AtlasAuthorizationException e) {
            LOG.error("Unable to obtain AtlasAuthorizer", e);
        }

        return ret;
    }

    public static AtlasAccessorResponse getAccessors(AtlasTypeAccessRequest request) {
        AtlasAccessorResponse ret = null;

        try {
            AtlasAuthorizer authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();
            setAuthInfo(request);

            ret = authorizer.getAccessors(request);
        } catch (AtlasAuthorizationException e) {
            LOG.error("Unable to obtain AtlasAuthorizer", e);
        }

        return ret;
    }

    public static Set<String> getRolesForCurrentUser() throws AtlasBaseException {
        Set<String> ret = new HashSet<>();

        try {
            AtlasAuthorizer authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();
            if (authorizer == null ) {
                throw new AtlasAuthorizationException("Authorizer is null");
            }

            ret = authorizer.getRolesForCurrentUser(getCurrentUserName(), getCurrentUserGroups());
        } catch (AtlasAuthorizationException e) {
            LOG.error("Unable to obtain AtlasAuthorizer", e);
        }

        return ret;
    }

    public static void filterTypesDef(AtlasTypesDefFilterRequest request) {
        MetricRecorder metric  = RequestContext.get().startMetricRecord("filterTypesDef");
        String        userName = getCurrentUserName();

        if (StringUtils.isNotEmpty(userName) && !RequestContext.get().isImportInProgress()) {
            try {
                AtlasAuthorizer authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();

                request.setUser(getCurrentUserName(), getCurrentUserGroups());
                request.setClientIPAddress(RequestContext.get().getClientIPAddress());
                request.setForwardedAddresses(RequestContext.get().getForwardedAddresses());
                request.setRemoteIPAddress(RequestContext.get().getClientIPAddress());

                authorizer.filterTypesDef(request);
            } catch (AtlasAuthorizationException e) {
                LOG.error("Unable to obtain AtlasAuthorizer", e);
            }
        }

        RequestContext.get().endMetricRecord(metric);
    }

    public static List<String> getForwardedAddressesFromRequest(HttpServletRequest httpServletRequest){
        String ipAddress = httpServletRequest.getHeader("X-FORWARDED-FOR");
        String[] forwardedAddresses = null ;

        if(!StringUtils.isEmpty(ipAddress)){
            forwardedAddresses = ipAddress.split(",");
        }
        return forwardedAddresses != null ? Arrays.asList(forwardedAddresses) : null;
    }

    public static String getRequestIpAddress(HttpServletRequest httpServletRequest) {
        String ret = "";

        try {
            InetAddress inetAddr = InetAddress.getByName(httpServletRequest.getRemoteAddr());

            ret = inetAddr.getHostAddress();
        } catch (UnknownHostException ex) {
            LOG.error("Failed to retrieve client IP address", ex);
        }

        return ret;
    }



    public static String getCurrentUserName() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();

        return auth != null ? auth.getName() : "";
    }

    public static Set<String> getCurrentUserGroups() {
        Set<String> ret = new HashSet<>();

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();

        if (auth != null) {
            for (GrantedAuthority c : auth.getAuthorities()) {
                ret.add(c.getAuthority());
            }
        }

        return ret;
    }

    private static void setAuthInfo(AtlasAccessRequest request) {
        request.setUser(getCurrentUserName(), getCurrentUserGroups());
        request.setClientIPAddress(RequestContext.get().getClientIPAddress());
        request.setForwardedAddresses(RequestContext.get().getForwardedAddresses());
        request.setRemoteIPAddress(RequestContext.get().getClientIPAddress());
    }

    public static Map<String, Object> getPreFilterDsl(String persona, String purpose, List<String> actions) {
        Map<String, Object> filterDsl = ListAuthorizer.getElasticsearchDSL(persona, purpose, actions);
        LOG.info("ABAC_AUTH: FULL_RESTRICTION: indexsearch query prefilter={}", filterDsl);
        return filterDsl;
    }
}

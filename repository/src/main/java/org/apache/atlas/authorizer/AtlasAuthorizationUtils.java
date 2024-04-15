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

import static org.apache.atlas.repository.Constants.SKIP_DELETE_AUTH_CHECK_TYPES;
import static org.apache.atlas.repository.Constants.SKIP_UPDATE_AUTH_CHECK_TYPES;

public class AtlasAuthorizationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorizationUtils.class);

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
                Map<String, String> errorMap = new HashMap<>();
                errorMap.put("action", request.getAction().toString());
                errorMap.put("end1", request.getEnd1Entity().getGuid());
                errorMap.put("end2", request.getEnd2Entity().getGuid());

                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, errorMap, request.getUser(), "");

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

                if (!atlasPoliciesResult.isAllowed() && atlasPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                    // 1
                    return false;
                }

                metric = RequestContext.get().startMetricRecord("isAccessAllowed.abac");
                AtlasAccessResult abacPoliciesResult = ABACAuthorizerUtils.isAccessAllowed(request.getEntity(), request.getAction());

                // reference - https://docs.google.com/spreadsheets/d/1npyX1cpm8-a8LwzmObgf8U1hZh6bO7FF8cpXjHMMQ08/edit?usp=sharing
                try {
                    if (!atlasPoliciesResult.isAllowed()) {
                        // 2
                        if (atlasPoliciesResult.isExplicitDeny()) {
                            return abacPoliciesResult.isAllowed() && abacPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE;
                        } else {
                            return abacPoliciesResult.isAllowed();
                        }
                    } else {
                        if (atlasPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                            //3
                            return !(!abacPoliciesResult.isAllowed() && abacPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE);
                        } else {
                            //4
                            if (abacPoliciesResult.isExplicitDeny()) {
                                return abacPoliciesResult.isAllowed();
                            } else {
                                return atlasPoliciesResult.isAllowed();
                            }
                        }
                    }
                } finally {
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

                if (!atlasPoliciesResult.isAllowed() && atlasPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                    // 1
                    return false;
                }

                metric = RequestContext.get().startMetricRecord("isAccessAllowed.abac");
                AtlasAccessResult abacPoliciesResult = ABACAuthorizerUtils.isAccessAllowed(request.getRelationshipType(),
                        request.getEnd1Entity(),
                        request.getEnd2Entity(),
                        request.getAction());

                // reference - https://docs.google.com/spreadsheets/d/1npyX1cpm8-a8LwzmObgf8U1hZh6bO7FF8cpXjHMMQ08/edit?usp=sharing
                try {
                    if (!atlasPoliciesResult.isAllowed()) {
                        // 2
                        if (atlasPoliciesResult.isExplicitDeny()) {
                            return abacPoliciesResult.isAllowed() && abacPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE;
                        } else {
                            return abacPoliciesResult.isAllowed();
                        }
                    } else {
                        if (atlasPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                            //3
                            return !(!abacPoliciesResult.isAllowed() && abacPoliciesResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE);
                        } else {
                            //4
                            if (abacPoliciesResult.isExplicitDeny()) {
                                return abacPoliciesResult.isAllowed();
                            } else {
                                return atlasPoliciesResult.isAllowed();
                            }
                        }
                    }
                } finally {
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
}

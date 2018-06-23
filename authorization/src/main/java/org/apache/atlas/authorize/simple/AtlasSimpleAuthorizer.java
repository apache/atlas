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

package org.apache.atlas.authorize.simple;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasAuthorizer;
import org.apache.atlas.authorize.AtlasAuthorizationException;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasSearchResultScrubRequest;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.authorize.simple.AtlasSimpleAuthzPolicy.*;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasFullTextResult;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class AtlasSimpleAuthorizer implements AtlasAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasSimpleAuthorizer.class);

    private final static String WILDCARD_ASTERISK = "*";

    private AtlasSimpleAuthzPolicy authzPolicy;


    public AtlasSimpleAuthorizer() {
    }

    @Override
    public void init() {
        LOG.info("==> SimpleAtlasAuthorizer.init()");

        InputStream inputStream = null;

        try {
            inputStream = ApplicationProperties.getFileAsInputStream(ApplicationProperties.get(), "atlas.authorizer.simple.authz.policy.file", "atlas-simple-authz-policy.json");

            authzPolicy = AtlasJson.fromJson(inputStream, AtlasSimpleAuthzPolicy.class);
        } catch (IOException | AtlasException e) {
            LOG.error("SimpleAtlasAuthorizer.init(): initialization failed", e);

            throw new RuntimeException(e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException excp) {
                    // ignore
                }
            }
        }

        LOG.info("<== SimpleAtlasAuthorizer.init()");
    }

    @Override
    public void cleanUp() {
        LOG.info("==> SimpleAtlasAuthorizer.cleanUp()");

        authzPolicy = null;

        LOG.info("<== SimpleAtlasAuthorizer.cleanUp()");
    }

    @Override
    public boolean isAccessAllowed(AtlasAdminAccessRequest request) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SimpleAtlasAuthorizer.isAccessAllowed({})", request);
        }

        boolean ret = false;

        Set<String> roles = getRoles(request.getUser(), request.getUserGroups());

        for (String role : roles) {
            List<AtlasAdminPermission> permissions = getAdminPermissionsForRole(role);

            if (permissions != null) {
                final String action = request.getAction() != null ? request.getAction().getType() : null;

                for (AtlasAdminPermission permission : permissions) {
                    if (isMatch(action, permission.getPrivileges())) {
                        ret = true;

                        break;
                    }
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== SimpleAtlasAuthorizer.isAccessAllowed({}): {}", request, ret);
        }

        return ret;
    }

    @Override
    public boolean isAccessAllowed(AtlasTypeAccessRequest request) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SimpleAtlasAuthorizer.isAccessAllowed({})", request);
        }

        boolean ret = false;

        Set<String> roles = getRoles(request.getUser(), request.getUserGroups());

        for (String role : roles) {
            List<AtlasTypePermission> permissions = getTypePermissionsForRole(role);

            if (permissions != null) {
                final String action       = request.getAction() != null ? request.getAction().getType() : null;
                final String typeCategory = request.getTypeDef() != null ? request.getTypeDef().getCategory().name() : null;
                final String typeName     = request.getTypeDef() != null ? request.getTypeDef().getName() : null;

                for (AtlasTypePermission permission : permissions) {
                    if (isMatch(action, permission.getPrivileges()) &&
                        isMatch(typeCategory, permission.getTypeCategories()) &&
                        isMatch(typeName, permission.getTypeNames())) {
                        ret = true;

                        break;
                    }
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== SimpleAtlasAuthorizer.isAccessAllowed({}): {}", request, ret);
        }

        return ret;
    }

    @Override
    public boolean isAccessAllowed(AtlasEntityAccessRequest request) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SimpleAtlasAuthorizer.isAccessAllowed({})", request);
        }

        final String      action         = request.getAction() != null ? request.getAction().getType() : null;
        final Set<String> entityTypes    = request.getEntityTypeAndAllSuperTypes();
        final String      entityId       = request.getEntityId();
        final String      classification = request.getClassification() != null ? request.getClassification().getTypeName() : null;
        final String      attribute      = request.getAttributeName();
        final Set<String> entClsToAuthz  = new HashSet<>(request.getEntityClassifications());
        final Set<String> roles          = getRoles(request.getUser(), request.getUserGroups());
        boolean hasEntityAccess          = false;
        boolean hasClassificationsAccess = false;

        for (String role : roles) {
            List<AtlasEntityPermission> permissions = getEntityPermissionsForRole(role);

            if (permissions != null) {
                for (AtlasEntityPermission permission : permissions) {
                    // match entity-type/entity-id/attribute
                    if (isMatchAny(entityTypes, permission.getEntityTypes()) && isMatch(entityId, permission.getEntityIds()) && isMatch(attribute, permission.getAttributes())) {
                        // match permission/classification
                        if (!hasEntityAccess) {
                            if (isMatch(action, permission.getPrivileges()) && isMatch(classification, permission.getClassifications())) {
                                hasEntityAccess = true;
                            }
                        }

                        // match entity-classifications
                        for (Iterator<String> iter = entClsToAuthz.iterator(); iter.hasNext();) {
                            String entityClassification = iter.next();

                            if (isMatchAny(request.getClassificationTypeAndAllSuperTypes(entityClassification), permission.getClassifications())) {
                                iter.remove();
                            }
                        }

                        hasClassificationsAccess = CollectionUtils.isEmpty(entClsToAuthz);

                        if (hasEntityAccess && hasClassificationsAccess) {
                            break;
                        }
                    }
                }
            }
        }

        boolean ret = hasEntityAccess && hasClassificationsAccess;

        if (LOG.isDebugEnabled()) {
            if (!ret) {
                LOG.debug("hasEntityAccess={}; hasClassificationsAccess={}, classificationsWithNoAccess={}", hasEntityAccess, hasClassificationsAccess, entClsToAuthz);
            }

            LOG.debug("<== SimpleAtlasAuthorizer.isAccessAllowed({}): {}", request, ret);
        }

        return ret;
    }

    @Override
    public void scrubSearchResults(AtlasSearchResultScrubRequest request) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SimpleAtlasAuthorizer.scrubSearchResults({})", request);
        }

        final AtlasSearchResult result = request.getSearchResult();

        if (CollectionUtils.isNotEmpty(result.getEntities())) {
            for (AtlasEntityHeader entity : result.getEntities()) {
                checkAccessAndScrub(entity, request);
            }
        }

        if (CollectionUtils.isNotEmpty(result.getFullTextResult())) {
            for (AtlasFullTextResult fullTextResult : result.getFullTextResult()) {
                if (fullTextResult != null) {
                    checkAccessAndScrub(fullTextResult.getEntity(), request);
                }
            }
        }

        if (MapUtils.isNotEmpty(result.getReferredEntities())) {
            for (AtlasEntityHeader entity : result.getReferredEntities().values()) {
                checkAccessAndScrub(entity, request);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== SimpleAtlasAuthorizer.scrubSearchResults({}): {}", request, result);
        }
    }

    private Set<String> getRoles(String userName, Set<String> userGroups) {
        Set<String> ret = new HashSet<>();

        if (authzPolicy != null) {
            if (userName != null && authzPolicy.getUserRoles() != null) {
                List<String> userRoles = authzPolicy.getUserRoles().get(userName);

                if (userRoles != null) {
                    ret.addAll(userRoles);
                }
            }

            if (userGroups != null && authzPolicy.getGroupRoles() != null) {
                for (String groupName : userGroups) {
                    List<String> groupRoles = authzPolicy.getGroupRoles().get(groupName);

                    if (groupRoles != null) {
                        ret.addAll(groupRoles);
                    }
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getRoles({}, {}): {}", userName, userGroups, ret);
        }

        return ret;
    }

    private List<AtlasAdminPermission> getAdminPermissionsForRole(String roleName) {
        List<AtlasAdminPermission> ret = null;

        if (authzPolicy != null && roleName != null) {
            AtlasAuthzRole role = authzPolicy.getRoles().get(roleName);

            ret = role != null ? role.getAdminPermissions() : null;
        }

        return ret;
    }

    private List<AtlasTypePermission> getTypePermissionsForRole(String roleName) {
        List<AtlasTypePermission> ret = null;

        if (authzPolicy != null && roleName != null) {
            AtlasAuthzRole role = authzPolicy.getRoles().get(roleName);

            ret = role != null ? role.getTypePermissions() : null;
        }

        return ret;
    }

    private List<AtlasEntityPermission> getEntityPermissionsForRole(String roleName) {
        List<AtlasEntityPermission> ret = null;

        if (authzPolicy != null && roleName != null) {
            AtlasAuthzRole role = authzPolicy.getRoles().get(roleName);

            ret = role != null ? role.getEntityPermissions() : null;
        }

        return ret;
    }

    private boolean isMatch(String value, List<String> patterns) {
        boolean ret = false;

        if (value == null) {
            ret = true;
        } if (CollectionUtils.isNotEmpty(patterns)) {
            for (String pattern : patterns) {
                if (isMatch(value, pattern)) {
                    ret = true;

                    break;
                }
            }
        }

        if (!ret && LOG.isDebugEnabled()) {
            LOG.debug("<== isMatch({}, {}): {}", value, patterns, ret);
        }

        return ret;
    }

    private boolean isMatchAny(Set<String> values, List<String> patterns) {
        boolean ret = false;

        if (CollectionUtils.isEmpty(values)) {
            ret = true;
        }if (CollectionUtils.isNotEmpty(patterns)) {
            for (String value : values) {
                if (isMatch(value, patterns)) {
                    ret = true;

                    break;
                }
            }
        }

        if (!ret && LOG.isDebugEnabled()) {
            LOG.debug("<== isMatchAny({}, {}): {}", values, patterns, ret);
        }

        return ret;
    }

    private boolean isMatch(String value, String pattern) {
        boolean ret;

        if (value == null) {
            ret = true;
        } else {
            ret = StringUtils.equalsIgnoreCase(value, pattern) || value.matches(pattern);
        }

        return ret;
    }

    private void checkAccessAndScrub(AtlasEntityHeader entity, AtlasSearchResultScrubRequest request) throws AtlasAuthorizationException {
        if (entity != null && request != null) {
            final AtlasEntityAccessRequest entityAccessRequest = new AtlasEntityAccessRequest(request.getTypeRegistry(), AtlasPrivilege.ENTITY_READ, entity, request.getUser(), request.getUserGroups());

            entityAccessRequest.setClientIPAddress(request.getClientIPAddress());

            if (!isAccessAllowed(entityAccessRequest)) {
                scrubEntityHeader(entity);
            }
        }
    }
}



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

package org.apache.atlas.authorization.atlas.authorizer;

import org.apache.atlas.authorize.AtlasAccessRequest;
import org.apache.atlas.authorize.AtlasAccessResult;
import org.apache.atlas.authorize.AtlasAccessorResponse;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasAuthorizationException;
import org.apache.atlas.authorize.AtlasAuthorizer;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.authorize.AtlasSearchResultScrubRequest;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.authorize.AtlasTypesDefFilterRequest;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.audit.model.AuthzAuditEvent;
import org.apache.atlas.plugin.audit.RangerDefaultAuditHandler;
import org.apache.atlas.plugin.contextenricher.RangerTagForEval;
import org.apache.atlas.plugin.model.RangerServiceDef;
import org.apache.atlas.plugin.model.RangerTag;
import org.apache.atlas.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.atlas.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.atlas.plugin.policyengine.RangerAccessResult;
import org.apache.atlas.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.atlas.plugin.service.RangerBasePlugin;
import org.apache.atlas.plugin.util.RangerPerfTracer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.authorization.atlas.authorizer.RangerAtlasAuthorizerUtil.*;
import static org.apache.atlas.constants.RangerAtlasConstants.*;


public class RangerAtlasAuthorizer implements AtlasAuthorizer {
    private static final Log LOG      = LogFactory.getLog(RangerAtlasAuthorizer.class);
    private static final Log PERF_LOG = RangerPerfTracer.getPerfLogger("atlasauth.request");

    private static volatile RangerBasePlugin atlasPlugin = null;
    private static volatile RangerGroupUtil groupUtil = null;

    static final Set<AtlasPrivilege> CLASSIFICATION_PRIVILEGES = new HashSet<AtlasPrivilege>() {{
        add(AtlasPrivilege.ENTITY_ADD_CLASSIFICATION);
        add(AtlasPrivilege.ENTITY_REMOVE_CLASSIFICATION);
        add(AtlasPrivilege.ENTITY_UPDATE_CLASSIFICATION);
    }};

    @Override
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerAtlasPlugin.init()");
        }

        RangerBasePlugin plugin = atlasPlugin;

        if (plugin == null) {
            synchronized (RangerAtlasPlugin.class) {
                plugin = atlasPlugin;

                if (plugin == null) {
                    plugin = new RangerAtlasPlugin();
                    plugin.init();

                    plugin.setResultProcessor(new RangerDefaultAuditHandler(plugin.getConfig()));

                    atlasPlugin = plugin;
                    groupUtil = new RangerGroupUtil(atlasPlugin.getUserStore());
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerAtlasPlugin.init()");
        }
    }

    @Override
    public void init(AtlasTypeRegistry typeRegistry) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerAtlasPlugin.init(typeRegistry)");
        }

        RangerBasePlugin plugin = atlasPlugin;

        if (plugin == null) {
            synchronized (RangerAtlasPlugin.class) {
                plugin = atlasPlugin;

                if (plugin == null) {
                    plugin = new RangerAtlasPlugin(typeRegistry);

                    plugin.init();

                    plugin.setResultProcessor(new RangerDefaultAuditHandler(plugin.getConfig()));

                    atlasPlugin = plugin;
                    groupUtil = new RangerGroupUtil(atlasPlugin.getUserStore());
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerAtlasPlugin.init(typeRegistry)");
        }
    }

    @Override
    public void cleanUp() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> cleanUp ");
        }
    }

    @Override
    public AtlasAccessResult isAccessAllowed(AtlasAdminAccessRequest request) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isAccessAllowed(" + request + ")");
        }

        final AtlasAccessResult    ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerAtlasAuthorizer.isAccessAllowed(" + request + ")");
            }

            String                   action         = request.getAction() != null ? request.getAction().getType() : null;
            RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl(Collections.singletonMap(RESOURCE_SERVICE, "*"));
            RangerAccessRequestImpl  rangerRequest  = new RangerAccessRequestImpl(rangerResource, action, request.getUser(), request.getUserGroups(), null);

            rangerRequest.setClientIPAddress(request.getClientIPAddress());
            rangerRequest.setAccessTime(request.getAccessTime());
            rangerRequest.setAction(action);
            rangerRequest.setForwardedAddresses(request.getForwardedAddresses());
            rangerRequest.setRemoteIPAddress(request.getRemoteIPAddress());


            ret = checkAccess(rangerRequest);
        } finally {
            RangerPerfTracer.log(perf);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isAccessAllowed(" + request + "): " + ret);
        }

        return ret;
    }

    @Override
    public AtlasAccessResult isAccessAllowed(AtlasEntityAccessRequest request, boolean isAuditEnabled) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isAccessAllowed(" + request + ")");
        }

        AtlasAccessResult       ret;
        RangerPerfTracer        perf         = null;
        RangerAtlasAuditHandler auditHandler = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerAtlasAuthorizer.isAccessAllowed(" + request + ")");
            }

            // not initializing audit handler, so that audits are not logged when entity details are NULL or EMPTY STRING
            if (isAuditEnabled && !(StringUtils.isEmpty(request.getEntityId()) && request.getClassification() == null && request.getEntity() == null)) {
                auditHandler = new RangerAtlasAuditHandler(request, getServiceDef());
            }

            ret = isAccessAllowed(request, auditHandler);
        } finally {
            RangerPerfTracer.log(perf);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isAccessAllowed(" + request + "): " + ret);
        }

        return ret;
    }

    @Override
    public AtlasAccessResult isAccessAllowed(AtlasTypeAccessRequest request) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isAccessAllowed(" + request + ")");
        }

        final AtlasAccessResult ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerAtlasAuthorizer.isAccessAllowed(" + request + ")");
            }

            final String typeName     = request.getTypeDef() != null ? request.getTypeDef().getName() : null;
            final String typeCategory = request.getTypeDef() != null && request.getTypeDef().getCategory() != null ? request.getTypeDef().getCategory().name() : null;
            final String action       = request.getAction() != null ? request.getAction().getType() : null;

            RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();

            rangerResource.setValue(RESOURCE_TYPE_NAME, typeName);
            rangerResource.setValue(RESOURCE_TYPE_CATEGORY, typeCategory);

            RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl(rangerResource, action, request.getUser(), request.getUserGroups(), null);
            rangerRequest.setClientIPAddress(request.getClientIPAddress());
            rangerRequest.setAccessTime(request.getAccessTime());
            rangerRequest.setAction(action);
            rangerRequest.setForwardedAddresses(request.getForwardedAddresses());
            rangerRequest.setRemoteIPAddress(request.getRemoteIPAddress());

            boolean isAuditDisabled = ACCESS_TYPE_TYPE_READ.equalsIgnoreCase(action);

            if (isAuditDisabled) {
                ret = checkAccess(rangerRequest, null);
            } else {
                ret = checkAccess(rangerRequest);
            }

        } finally {
            RangerPerfTracer.log(perf);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isAccessAllowed(" + request + "): " + ret);
        }

        return ret;
    }

    @Override
    public AtlasAccessResult isAccessAllowed(AtlasRelationshipAccessRequest request) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isAccessAllowed(" + request + ")");
        }

        AtlasAccessResult ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerAtlasAuthorizer.isAccessAllowed(" + request + ")");
            }

            final String      action                      = request.getAction() != null ? request.getAction().getType() : null;
            final Set<String> end1EntityTypeAndSuperTypes = request.getEnd1EntityTypeAndAllSuperTypes();
            final Set<AtlasClassification> end1Classifications         = new HashSet<AtlasClassification>(request.getEnd1EntityClassifications());
            final String      end1EntityId                = request.getEnd1EntityId();

            final Set<String> end2EntityTypeAndSuperTypes = request.getEnd2EntityTypeAndAllSuperTypes();
            final Set<AtlasClassification> end2Classifications         = new HashSet<AtlasClassification>(request.getEnd2EntityClassifications());
            final String      end2EntityId                = request.getEnd2EntityId();


            String relationShipType = request.getRelationshipType();

            RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();

            RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl(rangerResource, action, request.getUser(), request.getUserGroups(), null);
            rangerRequest.setClientIPAddress(request.getClientIPAddress());
            rangerRequest.setAccessTime(request.getAccessTime());
            rangerRequest.setAction(action);
            rangerRequest.setForwardedAddresses(request.getForwardedAddresses());
            rangerRequest.setRemoteIPAddress(request.getRemoteIPAddress());

            rangerResource.setValue(RESOURCE_RELATIONSHIP_TYPE, relationShipType);


            Set<String> classificationsWithSuperTypesEnd1 = new HashSet();

            for (AtlasClassification classificationToAuthorize : end1Classifications) {
                  classificationsWithSuperTypesEnd1.addAll(request.getClassificationTypeAndAllSuperTypes(classificationToAuthorize.getTypeName()));
            }

            rangerResource.setValue(RESOURCE_END_ONE_ENTITY_TYPE, end1EntityTypeAndSuperTypes);
            rangerResource.setValue(RESOURCE_END_ONE_ENTITY_CLASSIFICATION, classificationsWithSuperTypesEnd1);
            rangerResource.setValue(RESOURCE_END_ONE_ENTITY_ID, end1EntityId);


            Set<String> classificationsWithSuperTypesEnd2 = new HashSet();

            for (AtlasClassification classificationToAuthorize : end2Classifications) {
                classificationsWithSuperTypesEnd2.addAll(request.getClassificationTypeAndAllSuperTypes(classificationToAuthorize.getTypeName()));
            }

            rangerResource.setValue(RESOURCE_END_TWO_ENTITY_TYPE, end2EntityTypeAndSuperTypes);
            rangerResource.setValue(RESOURCE_END_TWO_ENTITY_CLASSIFICATION, classificationsWithSuperTypesEnd2);
            rangerResource.setValue(RESOURCE_END_TWO_ENTITY_ID, end2EntityId);

            ret = checkAccess(rangerRequest);

            if (!ret.isAllowed()) { // if resource based policy access not available fallback to check tag-based access.
                setClassificationsToRequestContext(end1Classifications, rangerRequest);
                ret = checkAccess(rangerRequest); // tag-based check with end1 classification
                LOG.info("End1 checkAccess " + ret);
                if (ret.isAllowed()) { //
                    setClassificationsToRequestContext(end2Classifications, rangerRequest);
                    ret = checkAccess(rangerRequest); // tag-based check with end2 classification
                    LOG.info("End2 checkAccess " + ret);
                }
            }

        } finally {
            RangerPerfTracer.log(perf);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isAccessAllowed(" + request + "): " + ret);
        }

        return ret;
    }

    @Override
    public AtlasAccessorResponse getAccessors(AtlasEntityAccessRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getAccessors(" + request + ")");
        }

        AtlasAccessorResponse ret = new AtlasAccessorResponse();
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerAtlasAuthorizer.getAccessors(" + request + ")");
            }

            final RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();
            final RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl();

            toRangerRequest(request, rangerRequest, rangerResource);
            rangerRequest.setAccessorsRequested(true);

            RangerAccessResult result = null;
            Set<AtlasClassification> tagNames = request.getEntityClassifications();
            if (CollectionUtils.isNotEmpty(tagNames)) {
                setClassificationsToRequestContext(tagNames, rangerRequest);

                // check authorization for each classification
                for (AtlasClassification classificationToAuthorize : tagNames) {
                    rangerResource.setValue(RESOURCE_ENTITY_CLASSIFICATION, request.getClassificationTypeAndAllSuperTypes(classificationToAuthorize.getTypeName()));

                    result = getAccessors(rangerRequest);
                    collectAccessors(result, ret);
                }
            } else {
                rangerResource.setValue(RESOURCE_ENTITY_CLASSIFICATION, ENTITY_NOT_CLASSIFIED);

                result = getAccessors(rangerRequest);
                collectAccessors(result, ret);
            }
        } finally {
            RangerPerfTracer.log(perf);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getAccessors(" + request + "): " + ret);
        }
        return ret;
    }

    @Override
    public AtlasAccessorResponse getAccessors(AtlasRelationshipAccessRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getAccessors(" + request + ")");
        }

        AtlasAccessorResponse ret = new AtlasAccessorResponse();
        RangerPerfTracer perf = null;
        RangerAccessResult result = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerAtlasAuthorizer.getAccessors(" + request + ")");
            }

            final String      action                = request.getAction() != null ? request.getAction().getType() : null;
            RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();
            RangerAccessRequestImpl rangerRequest   = new RangerAccessRequestImpl(rangerResource, action, request.getUser(),
                                                            request.getUserGroups(), null);
            rangerRequest.setAccessorsRequested(true);


            final Set<String> end1EntityTypeAndSuperTypes = request.getEnd1EntityTypeAndAllSuperTypes();
            final Set<AtlasClassification> end1Classifications         = new HashSet<AtlasClassification>(request.getEnd1EntityClassifications());
            final String      end1EntityId                = request.getEnd1EntityId();

            final Set<String> end2EntityTypeAndSuperTypes = request.getEnd2EntityTypeAndAllSuperTypes();
            final Set<AtlasClassification> end2Classifications         = new HashSet<AtlasClassification>(request.getEnd2EntityClassifications());
            final String      end2EntityId                = request.getEnd2EntityId();


            String relationShipType = request.getRelationshipType();


            rangerRequest.setClientIPAddress(request.getClientIPAddress());
            rangerRequest.setAccessTime(request.getAccessTime());
            rangerRequest.setAction(action);
            rangerRequest.setForwardedAddresses(request.getForwardedAddresses());
            rangerRequest.setRemoteIPAddress(request.getRemoteIPAddress());

            rangerResource.setValue(RESOURCE_RELATIONSHIP_TYPE, relationShipType);


            Set<String> classificationsWithSuperTypesEnd1 = new HashSet();

            for (AtlasClassification classificationToAuthorize : end1Classifications) {
                classificationsWithSuperTypesEnd1.addAll(request.getClassificationTypeAndAllSuperTypes(classificationToAuthorize.getTypeName()));
            }

            rangerResource.setValue(RESOURCE_END_ONE_ENTITY_TYPE, end1EntityTypeAndSuperTypes);
            rangerResource.setValue(RESOURCE_END_ONE_ENTITY_CLASSIFICATION, classificationsWithSuperTypesEnd1);
            rangerResource.setValue(RESOURCE_END_ONE_ENTITY_ID, end1EntityId);


            Set<String> classificationsWithSuperTypesEnd2 = new HashSet();

            for (AtlasClassification classificationToAuthorize : end2Classifications) {
                classificationsWithSuperTypesEnd2.addAll(request.getClassificationTypeAndAllSuperTypes(classificationToAuthorize.getTypeName()));
            }

            rangerResource.setValue(RESOURCE_END_TWO_ENTITY_TYPE, end2EntityTypeAndSuperTypes);
            rangerResource.setValue(RESOURCE_END_TWO_ENTITY_CLASSIFICATION, classificationsWithSuperTypesEnd2);
            rangerResource.setValue(RESOURCE_END_TWO_ENTITY_ID, end2EntityId);

            result = getAccessors(rangerRequest);
            collectAccessors(result, ret);

            // Check tag-based access.
            setClassificationsToRequestContext(end1Classifications, rangerRequest);
            RangerAccessResult resultEnd1 = getAccessors(rangerRequest); // tag-based accessors with end1 classification

            setClassificationsToRequestContext(end2Classifications, rangerRequest);
            RangerAccessResult resultEnd2 = getAccessors(rangerRequest); // tag-based accessors with end2 classification
            collectAccessors(resultEnd1, resultEnd2, ret);
        } finally {
            RangerPerfTracer.log(perf);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getAccessors(" + request + "): " + ret);
        }

        return ret;
    }

    @Override
    public AtlasAccessorResponse getAccessors(AtlasTypeAccessRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getAccessors(" + request + ")");
        }

        AtlasAccessorResponse ret = new AtlasAccessorResponse();
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerAtlasAuthorizer.getAccessors(" + request + ")");
            }

            final String action = request.getAction() != null ? request.getAction().getType() : null;

            RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();
            RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl(rangerResource, action, request.getUser(), request.getUserGroups(), null);
            rangerRequest.setAccessorsRequested(true);


            final String typeName     = request.getTypeDef() != null ? request.getTypeDef().getName() : null;
            final String typeCategory = request.getTypeDef() != null && request.getTypeDef().getCategory() != null ? request.getTypeDef().getCategory().name() : null;

            rangerResource.setValue(RESOURCE_TYPE_NAME, typeName);
            rangerResource.setValue(RESOURCE_TYPE_CATEGORY, typeCategory);

            rangerRequest.setClientIPAddress(request.getClientIPAddress());
            rangerRequest.setAccessTime(request.getAccessTime());
            rangerRequest.setAction(action);
            rangerRequest.setForwardedAddresses(request.getForwardedAddresses());
            rangerRequest.setRemoteIPAddress(request.getRemoteIPAddress());


            RangerAccessResult result = getAccessors(rangerRequest);
            collectAccessors(result, ret);

        } finally {
            RangerPerfTracer.log(perf);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getAccessors(" + request + "): " + ret);
        }
        return ret;
    }

    @Override
    public Set<String> getRolesForCurrentUser(String userName, Set<String> groups) {
        Set<String> ret = new HashSet<>();

        RangerBasePlugin plugin = atlasPlugin;

        ret = plugin.getRolesFromUserAndGroups(userName, groups);

        return ret;
    }

    @Override
    public void scrubSearchResults(AtlasSearchResultScrubRequest request) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> scrubSearchResults(" + request + ")");
        }

        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerAtlasAuthorizer.scrubSearchResults(" + request + ")");
            }

            final AtlasSearchResult result = request.getSearchResult();

            if (CollectionUtils.isNotEmpty(result.getEntities())) {
                for (AtlasEntityHeader entity : result.getEntities()) {
                    checkAccessAndScrub(entity, request);
                }
            }

            if (CollectionUtils.isNotEmpty(result.getFullTextResult())) {
                for (AtlasSearchResult.AtlasFullTextResult fullTextResult : result.getFullTextResult()) {
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
        } finally {
            RangerPerfTracer.log(perf);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== scrubSearchResults(): " + request);
        }
    }

    @Override
    public void scrubSearchResults(AtlasSearchResultScrubRequest request, boolean isScrubAuditEnabled) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled())
            LOG.debug("==> scrubSearchResults(" + request + " " + isScrubAuditEnabled);
        RangerPerfTracer perf = null;
        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG))
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerAtlasAuthorizer.scrubSearchResults(" + request + ")");
            AtlasSearchResult result = request.getSearchResult();
            if (CollectionUtils.isNotEmpty(result.getEntities())) {
                for (AtlasEntityHeader entity : result.getEntities()) {
                    checkAccessAndScrub(entity, request, isScrubAuditEnabled);
                }
            }
            if (CollectionUtils.isNotEmpty(result.getFullTextResult())) {
                for (AtlasSearchResult.AtlasFullTextResult fullTextResult : result.getFullTextResult()) {
                    if (fullTextResult != null)
                        checkAccessAndScrub(fullTextResult.getEntity(), request, isScrubAuditEnabled);
                }
            }
            if (MapUtils.isNotEmpty(result.getReferredEntities())) {
                for (AtlasEntityHeader entity : result.getReferredEntities().values()) {
                    checkAccessAndScrub(entity, request, isScrubAuditEnabled);
                }
            }
        } finally {
            RangerPerfTracer.log(perf);
        }
        if (LOG.isDebugEnabled())
            LOG.debug("<== scrubSearchResults(): " + request + " " + isScrubAuditEnabled);
    }

    @Override
    public void filterTypesDef(AtlasTypesDefFilterRequest request) throws AtlasAuthorizationException {

        AtlasTypesDef typesDef = request.getTypesDef();

        filterTypes(request, typesDef.getEnumDefs());
        filterTypes(request, typesDef.getStructDefs());
        filterTypes(request, typesDef.getEntityDefs());
        filterTypes(request, typesDef.getClassificationDefs());
        filterTypes(request, typesDef.getRelationshipDefs());
        filterTypes(request, typesDef.getBusinessMetadataDefs());

    }

    private void filterTypes(AtlasAccessRequest request, List<? extends AtlasBaseTypeDef> typeDefs)throws AtlasAuthorizationException {
        if (typeDefs != null) {
            for (ListIterator<? extends AtlasBaseTypeDef> iter = typeDefs.listIterator(); iter.hasNext();) {
                AtlasBaseTypeDef       typeDef     = iter.next();
                AtlasTypeAccessRequest typeRequest = new AtlasTypeAccessRequest(request.getAction(), typeDef, request.getUser(), request.getUserGroups());

                typeRequest.setClientIPAddress(request.getClientIPAddress());
                typeRequest.setForwardedAddresses(request.getForwardedAddresses());
                typeRequest.setRemoteIPAddress(request.getRemoteIPAddress());

                if (!isAccessAllowed(typeRequest).isAllowed()) {
                    iter.remove();
                }
            }
        }
    }


    private RangerServiceDef getServiceDef() {
        RangerBasePlugin plugin = atlasPlugin;

        return plugin != null ? plugin.getServiceDef() : null;
    }

    // deprecated: isAccessAllowed only checks for atlas and atlas_tag policies, use AuthorizationUtil.isAccessAllowed
    // to include the checks for abac policies as well.
    private AtlasAccessResult isAccessAllowed(AtlasEntityAccessRequest request, RangerAtlasAuditHandler auditHandler) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isAccessAllowed(" + request + ")");
        }
        AtlasAccessResult ret = new AtlasAccessResult(false);

        try {
            final String                   action         = request.getAction() != null ? request.getAction().getType() : null;
            final Set<String>              entityTypes    = request.getEntityTypeAndAllSuperTypes();
            final String                   entityId       = request.getEntityId();
            final String                   classification = request.getClassification() != null ? request.getClassification().getTypeName() : null;
            final RangerAccessRequestImpl  rangerRequest  = new RangerAccessRequestImpl();
            final RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();
            final String                   ownerUser      = request.getEntity() != null ? (String) request.getEntity().getAttribute(RESOURCE_ENTITY_OWNER) : null;

            rangerResource.setValue(RESOURCE_ENTITY_TYPE, entityTypes);
            rangerResource.setValue(RESOURCE_ENTITY_ID, entityId);
            rangerResource.setOwnerUser(ownerUser);
            rangerRequest.setAccessType(action);
            rangerRequest.setAction(action);
            rangerRequest.setUser(request.getUser());
            rangerRequest.setUserGroups(request.getUserGroups());
            rangerRequest.setClientIPAddress(request.getClientIPAddress());
            rangerRequest.setAccessTime(request.getAccessTime());
            rangerRequest.setResource(rangerResource);
            rangerRequest.setForwardedAddresses(request.getForwardedAddresses());
            rangerRequest.setRemoteIPAddress(request.getRemoteIPAddress());

            if (AtlasPrivilege.ENTITY_ADD_LABEL.equals(request.getAction()) || AtlasPrivilege.ENTITY_REMOVE_LABEL.equals(request.getAction())) {
                rangerResource.setValue(RESOURCE_ENTITY_LABEL, request.getLabel());
            } else if (AtlasPrivilege.ENTITY_UPDATE_BUSINESS_METADATA.equals(request.getAction())) {
                rangerResource.setValue(RESOURCE_ENTITY_BUSINESS_METADATA, request.getBusinessMetadata());
            } else if (StringUtils.isNotEmpty(classification) && CLASSIFICATION_PRIVILEGES.contains(request.getAction())) {
                rangerResource.setValue(RESOURCE_CLASSIFICATION, request.getClassificationTypeAndAllSuperTypes(classification));
            }

            if (CollectionUtils.isNotEmpty(request.getEntityClassifications())) {
                Set<AtlasClassification> entityClassifications = request.getEntityClassifications();
                Map<String, Object> contextOjb = rangerRequest.getContext();

                Set<RangerTagForEval> rangerTagForEval = getRangerServiceTag(entityClassifications);

                if (contextOjb == null) {
                    Map<String, Object> contextOjb1 = new HashMap<String, Object>();
                    contextOjb1.put("CLASSIFICATIONS", rangerTagForEval);
                    rangerRequest.setContext(contextOjb1);
                } else {
                    contextOjb.put("CLASSIFICATIONS", rangerTagForEval);
                    rangerRequest.setContext(contextOjb);
                }

                // check authorization for each classification
                for (AtlasClassification classificationToAuthorize : request.getEntityClassifications()) {
                    rangerResource.setValue(RESOURCE_ENTITY_CLASSIFICATION, request.getClassificationTypeAndAllSuperTypes(classificationToAuthorize.getTypeName()));

                    ret = checkAccess(rangerRequest, auditHandler);

                    if (!ret.isAllowed()) {
                        break;
                    }
                }
            } else {
                rangerResource.setValue(RESOURCE_ENTITY_CLASSIFICATION, ENTITY_NOT_CLASSIFIED );

                ret = checkAccess(rangerRequest, auditHandler);
            }

        } finally {
            if(auditHandler != null) {
                auditHandler.flushAudit();
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isAccessAllowed(" + request + "): " + ret);
        }

        return ret;
    }


    private void setClassificationsToRequestContext(Set<AtlasClassification> entityClassifications, RangerAccessRequestImpl rangerRequest) {
        Map<String, Object> contextOjb = rangerRequest.getContext();

        Set<RangerTagForEval> rangerTagForEval = getRangerServiceTag(entityClassifications);

        if (contextOjb == null) {
            Map<String, Object> contextOjb1 = new HashMap<String, Object>();
            contextOjb1.put("CLASSIFICATIONS", rangerTagForEval);
            rangerRequest.setContext(contextOjb1);
        } else {
            contextOjb.put("CLASSIFICATIONS", rangerTagForEval);
            rangerRequest.setContext(contextOjb);
        }
    }

    Set<RangerTagForEval> getRangerServiceTag(Set<AtlasClassification> classifications) {
        Set<RangerTagForEval> atlasClassificationSet = new HashSet<>();
        for (AtlasClassification classification : classifications) {
            RangerTag rangerTag = new RangerTag(null, classification.getTypeName(), getClassificationAttributes(classification), RangerTag.OWNER_SERVICERESOURCE);
            RangerTagForEval tagForEval = new RangerTagForEval(rangerTag, RangerPolicyResourceMatcher.MatchType.SELF);
            atlasClassificationSet.add(tagForEval);
        }
        return atlasClassificationSet;
    }

    private Map<String, String> getClassificationAttributes(AtlasClassification classification) {
        Map<String, Object> attributes = classification.getAttributes();
        final Map<String, String> result = new HashMap<String, String>();
        if(attributes!=null) {
            for (final Map.Entry<String, Object> entry : attributes.entrySet()) {
                result.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return result;
    }

    private AtlasAccessResult checkAccess(RangerAccessRequestImpl request) {
        AtlasAccessResult result = null;
        String userName = request.getUser();
        RangerBasePlugin plugin = atlasPlugin;

        if (plugin != null) {

            groupUtil.setUserStore(atlasPlugin.getUserStore());

            request.setUserGroups(groupUtil.getContainedGroups(userName));

            if (LOG.isDebugEnabled()) {
                LOG.debug("Setting UserGroup for user: " + userName + " Groups: " + groupUtil.getContainedGroups(userName));
            }

            RangerAccessResult rangerResult = plugin.isAccessAllowed(request);
            if (rangerResult != null) {
                result = new AtlasAccessResult(rangerResult.getIsAllowed(), rangerResult.getPolicyId(), rangerResult.getPolicyPriority());
            }

        } else {
            LOG.warn("RangerAtlasPlugin not initialized. Access blocked!!!");
        }

        if (result == null) {
            LOG.warn("checkAccess(): policy engine returned null result for user=" + userName + ". Defaulting to DENY.");
            result = new AtlasAccessResult(false);
        }

        return result;
    }

    private AtlasAccessResult checkAccess(RangerAccessRequestImpl request, RangerAtlasAuditHandler auditHandler) {
        AtlasAccessResult result = null;

        RangerBasePlugin plugin = atlasPlugin;
        String userName = request.getUser();

        if (plugin != null) {

            groupUtil.setUserStore(atlasPlugin.getUserStore());

            request.setUserGroups(groupUtil.getContainedGroups(userName));

            if (LOG.isDebugEnabled()) {
                LOG.debug("Setting UserGroup for user :" + userName + " Groups: " + groupUtil.getContainedGroups(userName));
            }

            RangerAccessResult rangerResult = plugin.isAccessAllowed(request, auditHandler);
            if (rangerResult != null) {
                result = new AtlasAccessResult(rangerResult.getIsAllowed(), rangerResult.getPolicyId(), rangerResult.getPolicyPriority());
            }

        } else {
            LOG.warn("RangerAtlasPlugin not initialized. Access blocked!!!");
        }

        if (result == null) {
            LOG.warn("checkAccess(): policy engine returned null result for user=" + userName + ". Defaulting to DENY.");
            result = new AtlasAccessResult(false);
        }

        return result;
    }

    private RangerAccessResult getAccessors(RangerAccessRequestImpl request) {
        RangerAccessResult result = null;

        RangerBasePlugin plugin = atlasPlugin;
        String userName = request.getUser();

        if (plugin != null) {

            groupUtil.setUserStore(atlasPlugin.getUserStore());
            request.setUserGroups(groupUtil.getContainedGroups(userName));

            if (LOG.isDebugEnabled()) {
                LOG.debug("Setting UserGroup for user :" + userName + " Groups: " + groupUtil.getContainedGroups(userName));
            }

            result = plugin.getAssetAccessors(request);

        } else {
            LOG.warn("RangerAtlasPlugin not initialized. Could not find Accessors!!!");
        }

        return result;
    }

    private void checkAccessAndScrub(AtlasEntityHeader entity, AtlasSearchResultScrubRequest request) throws AtlasAuthorizationException {
        checkAccessAndScrub(entity, request, false);
    }

    private void checkAccessAndScrub(AtlasEntityHeader entity, AtlasSearchResultScrubRequest request, boolean isScrubAuditEnabled) throws AtlasAuthorizationException {
        if (entity != null && request != null) {
            final AtlasEntityAccessRequest entityAccessRequest = new AtlasEntityAccessRequest(request.getTypeRegistry(), AtlasPrivilege.ENTITY_READ, entity, request.getUser(), request.getUserGroups());

            entityAccessRequest.setClientIPAddress(request.getClientIPAddress());
            entityAccessRequest.setForwardedAddresses(request.getForwardedAddresses());
            entityAccessRequest.setRemoteIPAddress(request.getRemoteIPAddress());

            boolean isEntityAccessAllowed  = AtlasAuthorizationUtils.isAccessAllowed(entityAccessRequest, isScrubAuditEnabled);
            if (!isEntityAccessAllowed) {
                scrubEntityHeader(entity, request.getTypeRegistry());
            }
        }
    }

    class RangerAtlasPlugin extends RangerBasePlugin {
        RangerAtlasPlugin() {
            super("atlas", "atlas");
        }

        RangerAtlasPlugin(AtlasTypeRegistry typeRegistry) {
            super("atlas", "atlas", typeRegistry);
        }
    }

    class RangerAtlasAuditHandler extends RangerDefaultAuditHandler {
        private final Map<String, AuthzAuditEvent> auditEvents;
        private final String                       resourcePath;
        private       boolean                      denyExists = false;

        public RangerAtlasAuditHandler(AtlasEntityAccessRequest request, RangerServiceDef serviceDef) {
            Collection<AtlasClassification> classifications    = request.getEntityClassifications();
            String             strClassifications = classifications == null ? "[]" : classifications.toString();

            if (request.getClassification() != null) {
                strClassifications += ("," + request.getClassification().getTypeName());
            }

            RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();

            rangerResource.setServiceDef(serviceDef);
            rangerResource.setValue(RESOURCE_ENTITY_TYPE, request.getEntityType());
            rangerResource.setValue(RESOURCE_ENTITY_CLASSIFICATION, strClassifications);
            rangerResource.setValue(RESOURCE_ENTITY_ID, request.getEntityId());

            if (AtlasPrivilege.ENTITY_ADD_LABEL.equals(request.getAction()) || AtlasPrivilege.ENTITY_REMOVE_LABEL.equals(request.getAction())) {
                rangerResource.setValue(RESOURCE_ENTITY_LABEL, "label=" + request.getLabel());
            } else if (AtlasPrivilege.ENTITY_UPDATE_BUSINESS_METADATA.equals(request.getAction())) {
                rangerResource.setValue(RESOURCE_ENTITY_BUSINESS_METADATA, "business-metadata=" + request.getBusinessMetadata());
            }

            auditEvents  = new HashMap<>();
            resourcePath = rangerResource.getAsString();
        }

        @Override
        public void processResult(RangerAccessResult result) {
            if (denyExists) { // nothing more to do, if a deny already encountered
                return;
            }

            AuthzAuditEvent auditEvent = super.getAuthzEvents(result);

            if (auditEvent != null) {
                // audit event might have list of entity-types and classification-types; overwrite with the values in original request
                if (resourcePath != null) {
                    auditEvent.setResourcePath(resourcePath);
                }

                if (!result.getIsAllowed()) {
                    denyExists = true;

                    auditEvents.clear();
                }

                auditEvents.put(auditEvent.getPolicyId() + auditEvent.getAccessType(), auditEvent);
            }
        }


        public void flushAudit() {
            if (auditEvents != null) {
                for (AuthzAuditEvent auditEvent : auditEvents.values()) {
                    logAuthzAudit(auditEvent);
                }
            }
        }
    }
}

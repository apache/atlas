/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas;

import org.apache.atlas.model.CassandraTagOperation;
import org.apache.atlas.model.ESDeferredOperation;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.service.metrics.MetricsRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfMetrics.MetricRecorder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.atlas.model.instance.AtlasObjectId.KEY_GUID;


public class RequestContext {
    private static final Logger METRICS = LoggerFactory.getLogger("METRICS");
    private static final Logger LOG = LoggerFactory.getLogger(RequestContext.class);

    private static final ThreadLocal<RequestContext> CURRENT_CONTEXT = new ThreadLocal<>();
    private static final Set<RequestContext>         ACTIVE_REQUESTS = new HashSet<>();
    private static final boolean                     isMetricsEnabled = METRICS.isDebugEnabled();

    private final long                                   requestTime          = System.currentTimeMillis();
    private final Map<String, AtlasEntityHeader>         updatedEntities      = new HashMap<>();
    private final Map<String, AtlasEntityHeader>         deletedEntities      = new HashMap<>();
    private final Map<String, AtlasEntityHeader>         restoreEntities      = new HashMap<>();
    private final Map<String, Map<String, Object>> allInternalAttributesMap     = new HashMap<>();


    private       Map<String, String>                    lexoRankCache        = null;
    private final Map<String, AtlasEntity>               entityCache          = new HashMap<>();
    private final Map<String, AtlasEntityHeader>         entityHeaderCache    = new HashMap<>();
    private final Map<String, AtlasEntityWithExtInfo>    entityExtInfoCache   = new HashMap<>();
    private final Map<String, AtlasEntity>               diffEntityCache      = new HashMap<>();
    private final Map<String, List<AtlasClassification>> addedPropagations    = new HashMap<>();
    private final Map<String, List<AtlasClassification>> removedPropagations  = new HashMap<>();
    private final Map<String, String>                    requestContextHeaders= new HashMap<>();
    private final Set<String>                            deletedEdgesIds      = new HashSet<>();
    private final Set<String>                            processGuidIds      = new HashSet<>();

    private       Map<String, String>                    evaluateEntityHeaderCache        = null;
    private final AtlasPerfMetrics metrics = isMetricsEnabled ? new AtlasPerfMetrics() : null;
    private final List<AtlasPerfMetrics.Metric> applicationMetrics = new ArrayList<>();
    private List<EntityGuidPair> entityGuidInRequest = null;
    private final Set<String> entitiesToSkipUpdate = new HashSet<>();
    private final Set<String> onlyCAUpdateEntities = new HashSet<>();
    private final Set<String> onlyBAUpdateEntities = new HashSet<>();
    private final List<AtlasTask> queuedTasks = new ArrayList<>();
    private final Set<String> relationAttrsForSearch = new HashSet<>();

    private static String USERNAME = "";
    private final Map<String, List<Object>> removedElementsMap = new HashMap<>();
    private final Map<String, List<Object>> newElementsCreatedMap = new HashMap<>();

    private final Map<String, Set<AtlasRelationship>> relationshipMutationMap = new HashMap<>();
    private final Set<String> edgeLabels = new HashSet<>();
    
    // Observability timing fields
    private final AtomicLong lineageCalcTime = new AtomicLong(0L);

    private String user;
    private Set<String> userGroups;
    private String clientIPAddress;
    private List<String> forwardedAddresses;
    private String clientOrigin;
    private String playbookName;
    private DeleteType deleteType = DeleteType.DEFAULT;
    private boolean isPurgeRequested = false;
    private int maxAttempts = 1;
    private int attemptCount = 1;
    private boolean isImportInProgress = false;
    private boolean     isInNotificationProcessing = false;


    private boolean     authorisedRemoveRelation = false;
    private boolean     isInTypePatching           = false;
    private boolean     createShellEntityForNonExistingReference = false;
    private boolean     skipFailedEntities = false;
    private boolean     allowDeletedRelationsIndexsearch = false;
    private boolean     includeMeanings = true;
    private boolean     includeClassifications = true;
    private boolean     includeRelationshipAttributes;

    private boolean     includeClassificationNames = false;
    private boolean     skipProcessEdgeRestoration = false;
    private String      currentTypePatchAction = "";
    private AtlasTask   currentTask;
    private String traceId;
    private final Map<AtlasObjectId, Object> relationshipEndToVertexIdMap = new HashMap<>();
    private boolean     allowDuplicateDisplayName;
    private MetricsRegistry metricsRegistry;
    private boolean skipAuthorizationCheck = false;
    private Set<String> deletedEdgesIdsForResetHasLineage = new HashSet<>(0);
    private String requestUri;
    private boolean delayTagNotifications = false;
    private boolean skipHasLineageCalculation = false;
    private boolean isInvokedByIndexSearch = false;
    private boolean isInvokedByLineage = false;
    private Map<AtlasClassification, Collection<Object>> deletedClassificationAndVertices = new HashMap<>();
    private Map<AtlasClassification, Collection<Object>> addedClassificationAndVertices = new HashMap<>();

    // Track Cassandra operations for rollback
    private final Map<String, Stack<CassandraTagOperation>> cassandraTagOperations = new HashMap<>();
    private final List<ESDeferredOperation> esDeferredOperations = new ArrayList<>();
    private static final String X_ATLAN_CLIENT_ORIGIN = "X-Atlan-Client-Origin";
    private static final String CLIENT_ORIGIN_PRODUCT = "product_webapp";

    Map<String, Object> tagsDiff = new HashMap<>();

    private RequestContext() {
    }

    //To handle gets from background threads where createContext() is not called
    //createContext called for every request in the filter
    public static RequestContext get() {
        RequestContext ret = CURRENT_CONTEXT.get();

        if (ret == null) {
            ret = new RequestContext();
            CURRENT_CONTEXT.set(ret);

            synchronized (ACTIVE_REQUESTS) {
                ACTIVE_REQUESTS.add(ret);
            }
        }

        return ret;
    }

    public static void clear() {
        RequestContext instance = CURRENT_CONTEXT.get();

        if (instance != null) {
            instance.clearCache();

            synchronized (ACTIVE_REQUESTS) {
                ACTIVE_REQUESTS.remove(instance);
            }
        }

        CURRENT_CONTEXT.remove();
    }

    public void clearCache() {
        this.updatedEntities.clear();
        this.deletedEntities.clear();
        this.entityCache.clear();
        this.entityHeaderCache.clear();
        this.entityExtInfoCache.clear();
        this.diffEntityCache.clear();
        this.addedPropagations.clear();
        this.removedPropagations.clear();
        this.entitiesToSkipUpdate.clear();
        this.onlyCAUpdateEntities.clear();
        this.onlyBAUpdateEntities.clear();
        this.relationAttrsForSearch.clear();
        this.includeRelationshipAttributes = false;
        this.queuedTasks.clear();
        this.newElementsCreatedMap.clear();
        this.removedElementsMap.clear();
        this.deletedEdgesIds.clear();
        this.processGuidIds.clear();
        this.deletedEdgesIdsForResetHasLineage.clear();
        this.requestContextHeaders.clear();
        this.relationshipEndToVertexIdMap.clear();
        this.relationshipMutationMap.clear();
        this.lexoRankCache = null;
        this.currentTask = null;
        this.skipAuthorizationCheck = false;
        this.delayTagNotifications = false;
        deletedClassificationAndVertices.clear();
        addedClassificationAndVertices.clear();
        esDeferredOperations.clear();
        this.cassandraTagOperations.clear();
        this.allInternalAttributesMap.clear();

        // Reset observability timing fields
        this.lineageCalcTime.set(0L);

        if (metrics != null && !metrics.isEmpty()) {
            METRICS.debug(metrics.toString());
            if (Objects.nonNull(this.metricsRegistry)){
                this.metricsRegistry.collect(traceId, this.requestUri, metrics, this.getClientOrigin());
            }
            metrics.clear();
        }
        if (CollectionUtils.isNotEmpty(applicationMetrics)) {
            if (Objects.nonNull(this.metricsRegistry)){
                this.metricsRegistry.collectApplicationMetrics(traceId, this.requestUri, applicationMetrics);
            }
            applicationMetrics.clear();
        }
        setTraceId(null);

        if (this.entityGuidInRequest != null) {
            this.entityGuidInRequest.clear();
        }
    }

    public void addApplicationMetrics(AtlasPerfMetrics.Metric metric) {
        this.applicationMetrics.add(metric);
    }

    public void clearEntityCache() {
        this.entityCache.clear();
    }

    public boolean isAuthorisedRemoveRelation() {
        return authorisedRemoveRelation;
    }

    public void setAuthorisedRemoveRelation(boolean authorisedRemoveRelation) {
        this.authorisedRemoveRelation = authorisedRemoveRelation;
    }
    public Set<String> getRelationAttrsForSearch() {
        return relationAttrsForSearch;
    }

    public void setRelationAttrsForSearch(Set<String> relationAttrsForSearch) {
        if (CollectionUtils.isNotEmpty(relationAttrsForSearch)){
            this.relationAttrsForSearch.addAll(relationAttrsForSearch);
        }
    }

    public boolean isIncludeRelationshipAttributes() {
        return includeRelationshipAttributes;
    }

    public void setIncludeRelationshipAttributes(boolean includeRelationshipAttributes) {
        this.includeRelationshipAttributes = includeRelationshipAttributes;
    }

    public Map<String, List<Object>> getRemovedElementsMap() {
        return removedElementsMap;
    }

    public Map<String, List<Object>> getNewElementsCreatedMap() {
        return newElementsCreatedMap;
    }

    public static String getCurrentUser() {
        RequestContext context = CURRENT_CONTEXT.get();
        String ret = context != null ? context.getUser() : null;
        if (StringUtils.isBlank(ret)) {
            try {
                ret = UserGroupInformation.getLoginUser().getShortUserName();
            } catch (Exception e) {
                ret = null;
            }
            if (StringUtils.isBlank(ret)){
                ret = System.getProperty("user.name");
                if (StringUtils.isBlank(ret)) {
                    ret = "atlas";
                }
            }
        }
        return ret;
    }

    public String getUser() {
        if (isImportInProgress) {
            if (StringUtils.isEmpty(USERNAME)){
                try {
                    USERNAME = ApplicationProperties.get().getString("atlas.migration.user.name", "");
                } catch (AtlasException e) {
                    LOG.error("Failed to find value for atlas.migration.user.name from properties");
                }
            }
            return USERNAME;
        }
        return user;
    }

    public Set<String> getUserGroups() {
        return userGroups;
    }

    public void setUser(String user, Set<String> userGroups) {
        this.user       = user;
        this.userGroups = userGroups;
    }

    public DeleteType getDeleteType() { return deleteType; }

    public void setDeleteType(DeleteType deleteType) { this.deleteType = (deleteType == null) ? DeleteType.DEFAULT : deleteType; }

    public String getClientIPAddress() {
        return clientIPAddress;
    }

    public void setClientIPAddress(String clientIPAddress) {
        this.clientIPAddress = clientIPAddress;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public boolean isImportInProgress() {
        return isImportInProgress;
    }

    public void setImportInProgress(boolean importInProgress) {
        isImportInProgress = importInProgress;
    }

    public boolean isPurgeRequested() { return isPurgeRequested; }

    public void setPurgeRequested(boolean isPurgeRequested) { this.isPurgeRequested = isPurgeRequested; }

    public boolean isInNotificationProcessing() {
        return isInNotificationProcessing;
    }

    public void setInNotificationProcessing(boolean inNotificationProcessing) {
        isInNotificationProcessing = inNotificationProcessing;
    }

    public boolean isInTypePatching() {
        return isInTypePatching;
    }

    public void setInTypePatching(boolean inTypePatching) {
        isInTypePatching = inTypePatching;
    }

    public boolean isCreateShellEntityForNonExistingReference() {
        return createShellEntityForNonExistingReference;
    }

    public void setCreateShellEntityForNonExistingReference(boolean createShellEntityForNonExistingReference) {
        this.createShellEntityForNonExistingReference = createShellEntityForNonExistingReference;
    }

    public boolean isSkipFailedEntities() {
        return skipFailedEntities;
    }

    public void setSkipFailedEntities(boolean skipFailedEntities) {
        this.skipFailedEntities = skipFailedEntities;
    }

    public boolean isAllowDeletedRelationsIndexsearch() {
        return allowDeletedRelationsIndexsearch;
    }

    public void setAllowDeletedRelationsIndexsearch(boolean allowDeletedRelationsIndexsearch) {
        this.allowDeletedRelationsIndexsearch = allowDeletedRelationsIndexsearch;
    }

    public void setAllowDuplicateDisplayName(boolean allowDuplicateDisplayName){
        this.allowDuplicateDisplayName = allowDuplicateDisplayName;
    }
    public boolean getAllowDuplicateDisplayName(){
        return allowDuplicateDisplayName;
    }

    public String getCurrentTypePatchAction() {
        return currentTypePatchAction;
    }

    public void setCurrentTypePatchAction(String currentTypePatchAction) {
        this.currentTypePatchAction = currentTypePatchAction;
    }

    public void recordEntityUpdate(AtlasEntityHeader entity) {
        if (entity != null && entity.getGuid() != null && ! entitiesToSkipUpdate.contains(entity.getGuid())) {
            updatedEntities.put(entity.getGuid(), entity);
        }
    }

    public void recordEntityUpdateForNonRelationshipAttributes(AtlasEntityHeader entity) {
        if (entity != null && entity.getGuid() != null) {
            updatedEntities.put(entity.getGuid(), entity);
        }
    }
    public void recordEntityToSkip(String guid) {
        if(! StringUtils.isEmpty(guid)) {
            entitiesToSkipUpdate.add(guid);
        }
    }

    public void recordEntityWithCustomAttributeUpdate(String guid) {
        if(! StringUtils.isEmpty(guid)) {
            onlyCAUpdateEntities.add(guid);
        }
    }

    public void recordEntityWithBusinessAttributeUpdate(String guid) {
        if(! StringUtils.isEmpty(guid)) {
            onlyBAUpdateEntities.add(guid);
        }
    }

    public boolean checkIfEntityIsForCustomAttributeUpdate(String guid) {
        return StringUtils.isNotEmpty(guid) && onlyCAUpdateEntities.contains(guid);
    }

    public boolean checkIfEntityIsForBusinessAttributeUpdate(String guid) {
        return StringUtils.isNotEmpty(guid) && onlyBAUpdateEntities.contains(guid);
    }

    public void recordEntityDelete(AtlasEntityHeader entity) {
        if (entity != null && entity.getGuid() != null) {
            deletedEntities.put(entity.getGuid(), entity);
        }
    }

    public void recordEntityRestore(AtlasEntityHeader entity) {
        if (entity != null && entity.getGuid() != null) {
            entity.setStatus(AtlasEntity.Status.ACTIVE);
            restoreEntities.put(entity.getGuid(), entity);
        }
    }

    public void recordAddedPropagation(String guid, AtlasClassification classification) {
        if (StringUtils.isNotEmpty(guid) && classification != null) {
            List<AtlasClassification> classifications = addedPropagations.get(guid);

            if (classifications == null) {
                classifications = new ArrayList<>();
            }

            classifications.add(classification);

            addedPropagations.put(guid, classifications);
        }
    }

    public boolean isDelayTagNotifications() {
        return delayTagNotifications;
    }

    public void setDelayTagNotifications(boolean delayTagNotifications) {
        this.delayTagNotifications = delayTagNotifications;
    }

    public Map<AtlasClassification, Collection<Object>> getDeletedClassificationAndVertices() {
        return deletedClassificationAndVertices;
    }

    public void setDeletedClassificationAndVertices(Map<AtlasClassification, Collection<Object>> deletedClassificationAndVertices) {
        this.deletedClassificationAndVertices = deletedClassificationAndVertices;
    }

    public void addDeletedClassificationAndVertices(AtlasClassification classification, Collection<Object> vertices) {
        this.deletedClassificationAndVertices.put(classification, vertices);
    }

    public Map<AtlasClassification, Collection<Object>> getAddedClassificationAndVertices() {
        return addedClassificationAndVertices;
    }

    public void setAddedClassificationAndVertices(Map<AtlasClassification, Collection<Object>> addedClassificationAndVertices) {
        this.addedClassificationAndVertices = addedClassificationAndVertices;
    }

    public void addAddedClassificationAndVertices(AtlasClassification classification, Collection<Object> vertices) {
        this.addedClassificationAndVertices.put(classification, vertices);
    }

    public Map<String, List<AtlasClassification>> getAndRemoveTagsDiff(String entityGuid) {
        return (Map<String, List<AtlasClassification>>) tagsDiff.remove(entityGuid);
    }

    public void addTagsDiff(String entityGuid, Map<String, List<AtlasClassification>> tagsDiff) {
        this.tagsDiff.put(entityGuid, tagsDiff);
    }

    public void addToDeletedEdgesIds(String edgeId) {
        deletedEdgesIds.add(edgeId);
    }

    public Set<String> getDeletedEdgesIds() {
        return deletedEdgesIds;
    }

    public void addToDeletedEdgesIdsForResetHasLineage(String edgeId) {
        deletedEdgesIdsForResetHasLineage.add(edgeId);
    }

    public Set<String> getDeletedEdgesIdsForResetHasLineage() {
        return deletedEdgesIdsForResetHasLineage;
    }

    public Set<String> getProcessGuidIds() {
        return processGuidIds;
    }

    public void addProcessGuidIds(String guid) {
        processGuidIds.add(guid);
    }


    public AtlasTask getCurrentTask() {
        return currentTask;
    }

    public void setCurrentTask(AtlasTask currentTask) {
        this.currentTask = currentTask;
    }

    public static int getActiveRequestsCount() {
        return ACTIVE_REQUESTS.size();
    }

    public boolean isSkipAuthorizationCheck() {
        return skipAuthorizationCheck;
    }

    public void setSkipAuthorizationCheck(boolean skipAuthorizationCheck) {
        this.skipAuthorizationCheck = skipAuthorizationCheck;
    }

    public static long earliestActiveRequestTime() {
        long ret = System.currentTimeMillis();

        synchronized (ACTIVE_REQUESTS) {
            for (RequestContext context : ACTIVE_REQUESTS) {
                if (ret > context.getRequestTime()) {
                    ret = context.getRequestTime();
                }
            }
        }

        return ret;
    }

    public void recordRemovedPropagation(String guid, AtlasClassification classification) {
        if (StringUtils.isNotEmpty(guid) && classification != null) {
            List<AtlasClassification> classifications = removedPropagations.get(guid);

            if (classifications == null) {
                classifications = new ArrayList<>();
            }

            classifications.add(classification);

            removedPropagations.put(guid, classifications);
        }
    }

    public Map<String, List<AtlasClassification>> getAddedPropagations() {
        return addedPropagations;
    }

    public Map<String, List<AtlasClassification>> getRemovedPropagations() {
        return removedPropagations;
    }

    /**
     * Adds the specified instance to the cache
     *
     */
    public void cache(AtlasEntityWithExtInfo entity) {
        if (entity != null && entity.getEntity() != null && entity.getEntity().getGuid() != null) {
            entityExtInfoCache.put(entity.getEntity().getGuid(), entity);
            entityCache.put(entity.getEntity().getGuid(), entity.getEntity());
        }
    }

    public void cache(AtlasEntity entity) {
        if (entity != null && entity.getGuid() != null) {
            entityCache.put(entity.getGuid(), entity);
        }
    }

    public void cacheDifferentialEntity(AtlasEntity entity) {
        if (entity != null && entity.getGuid() != null) {
            diffEntityCache.put(entity.getGuid(), entity);
        }
    }

    public void setEntityHeaderCache(String cacheKey, AtlasEntityHeader headerCache){
        if(headerCache != null && StringUtils.isNotEmpty(cacheKey)){
            entityHeaderCache.put(cacheKey, headerCache);
        }
    }

    public AtlasEntityHeader getCachedEntityHeader(String cacheKey){
        if(cacheKey == null){
            return null;
        }
        return entityHeaderCache.getOrDefault(cacheKey,null);
    }

    public AtlasEntity getDifferentialEntity(String guid) {
        return diffEntityCache.get(guid);
    }

    public Collection<AtlasEntity> getDifferentialEntities() { return diffEntityCache.values(); }

    public Map<String,AtlasEntity> getDifferentialEntitiesMap() { return diffEntityCache; }

    public Collection<AtlasEntityHeader> getUpdatedEntities() {
        return updatedEntities.values();
    }

    public Collection<AtlasEntityHeader> getDeletedEntities() {
        return deletedEntities.values();
    }

    public void clearRemovePropagations() {
        removedPropagations.clear();
    }

    public void clearAddedPropagations() {
        addedPropagations.clear();
    }

    public Collection<AtlasEntityHeader> getRestoredEntities() {
        return restoreEntities.values();
    }

    /**
     * Checks if an instance with the given guid is in the cache for this request.  Either returns the instance
     * or null if it is not in the cache.
     *
     * @param guid the guid to find
     * @return Either the instance or null if it is not in the cache.
     */
    public AtlasEntityWithExtInfo getEntityWithExtInfo(String guid) {
        return entityExtInfoCache.get(guid);
    }

    public AtlasEntity getEntity(String guid) {
        return entityCache.get(guid);
    }

    public long getRequestTime() {
        return requestTime;
    }

    public boolean isUpdatedEntity(String guid) {
        return updatedEntities.containsKey(guid);
    }

    public boolean isDeletedEntity(String guid) {
        return deletedEntities.containsKey(guid);
    }

    public boolean isRestoredEntity(String guid) {
        return restoreEntities.containsKey(guid);
    }

    public void addRequestContextHeader(String headerName, String headerValue) {
        if (StringUtils.isNotEmpty(headerName)) {
            requestContextHeaders.put(headerName, headerValue);
        }
    }

    public void setRequestContextHeaders(Map<String, String> requestContextHeaders) {
        if (requestContextHeaders != null) {
            this.requestContextHeaders.putAll(requestContextHeaders);
        }
    }

    public Map<String, String> getRequestContextHeaders() {
        return requestContextHeaders;
    }

    public MetricRecorder startMetricRecord(String name) { return metrics != null ? metrics.getMetricRecorder(name) : null; }

    public void endMetricRecord(MetricRecorder recorder) {
        if (metrics != null && recorder != null) {
            metrics.recordMetric(recorder);
        }
    }

    public void endMetricRecord(MetricRecorder recorder,long invocations){
        if (metrics != null && recorder != null) {
            metrics.recordMetric(recorder, invocations);
        }
    }

    public void recordEntityGuidUpdate(AtlasEntity entity, String guidInRequest) {
        recordEntityGuidUpdate(new EntityGuidPair(entity, guidInRequest));
    }

    public void recordEntityGuidUpdate(AtlasObjectId entity, String guidInRequest) {
        recordEntityGuidUpdate(new EntityGuidPair(entity, guidInRequest));
    }

    public void recordEntityGuidUpdate(Map entity, String guidInRequest) {
        recordEntityGuidUpdate(new EntityGuidPair(entity, guidInRequest));
    }

    public void recordEntityGuidUpdate(EntityGuidPair record) {
        if (entityGuidInRequest == null) {
            entityGuidInRequest = new ArrayList<>();
        }

        entityGuidInRequest.add(record);
    }

    public void resetEntityGuidUpdates() {
        if (entityGuidInRequest != null) {
            for (EntityGuidPair entityGuidPair : entityGuidInRequest) {
                entityGuidPair.resetEntityGuid();
            }
        }
    }

    public void queueTask(AtlasTask task) {
        queuedTasks.add(task);
    }

    public List<AtlasTask> getQueuedTasks() {
        return this.queuedTasks;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public void setIncludeMeanings(boolean includeMeanings) {
        this.includeMeanings = includeMeanings;
    }

    public boolean includeMeanings() {
        return this.includeMeanings;
    }

    public void setIncludeClassifications(boolean includeClassifications) {
        this.includeClassifications = includeClassifications;
    }

    public boolean includeClassifications() {
        return this.includeClassifications;
    }

    public void setMetricRegistry(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
    }

    public void setUri(String uri) {
        this.requestUri = uri;
    }

    public String getRequestUri() {
        return this.requestUri;
    }

    public boolean isIncludeClassificationNames() {
        return includeClassificationNames;
    }

    public void setIncludeClassificationNames(boolean includeClassificationNames) {
        this.includeClassificationNames = includeClassificationNames;
    }

    public String getClientOrigin() {
        return clientOrigin;
    }

    public void setClientOrigin(String clientOrigin) {
        this.clientOrigin = StringUtils.isEmpty(clientOrigin) ? "other" :clientOrigin;
    }

    public String getPlaybookName() {
        return playbookName;
    }

    public void setPlaybookName(String playbookName) {
        this.playbookName = StringUtils.isEmpty(playbookName) ? "NA" :playbookName;
    }

    public Map<String, String> getEvaluateEntityHeaderCache() {
        return evaluateEntityHeaderCache;
    }

    public void setEvaluateEntityHeaderCache(Map<String, String> evaluateEntityHeaderCache) {
        this.evaluateEntityHeaderCache = evaluateEntityHeaderCache;
    }

    public boolean isSkipProcessEdgeRestoration() {
        return skipProcessEdgeRestoration;
    }

    public void setSkipProcessEdgeRestoration(boolean skipProcessEdgeRestoration) {
        this.skipProcessEdgeRestoration = skipProcessEdgeRestoration;
    }

    public boolean skipHasLineageCalculation() {
        return skipHasLineageCalculation;
    }
    public void setSkipHasLineageCalculation(boolean skipHasLineageCalculation) {
        this.skipHasLineageCalculation = skipHasLineageCalculation;
    }

    public void setIsInvokedByIndexSearch(boolean isInvokedByIndexSearch) {
        this.isInvokedByIndexSearch = isInvokedByIndexSearch;
    }

    public boolean isInvokedByIndexSearch() {
        return isInvokedByIndexSearch;
    }

    public boolean isInvokedByProduct() {
        Map<String, String> requestContextHeaders = getRequestContextHeaders();
        if (MapUtils.isEmpty(requestContextHeaders)) {
            return false;
        }

        return CLIENT_ORIGIN_PRODUCT.equals(requestContextHeaders.get(X_ATLAN_CLIENT_ORIGIN)) ||
                CLIENT_ORIGIN_PRODUCT.equals(requestContextHeaders.get(X_ATLAN_CLIENT_ORIGIN.toLowerCase()));
    }

    public void setIsInvokedByLineage(boolean isInvokedByLineage) {
        this.isInvokedByLineage = isInvokedByLineage;
    }

    public boolean isInvokedByLineage() {
        return isInvokedByLineage;
    }

    public Map<String, Map<String, Object>> getAllInternalAttributesMap() {
        return allInternalAttributesMap;
    }

    public class EntityGuidPair {
        private final Object entity;
        private final String guid;

        public EntityGuidPair(AtlasEntity entity, String guid) {
            this.entity = entity;
            this.guid   = guid;
        }

        public EntityGuidPair(AtlasObjectId entity, String guid) {
            this.entity = entity;
            this.guid   = guid;
        }

        public EntityGuidPair(Map entity, String guid) {
            this.entity = entity;
            this.guid   = guid;
        }

        public void resetEntityGuid() {
            if (entity instanceof AtlasEntity) {
                ((AtlasEntity) entity).setGuid(guid);
            } else if (entity instanceof AtlasObjectId) {
                ((AtlasObjectId) entity).setGuid(guid);
            } else if (entity instanceof Map) {
                ((Map) entity).put(KEY_GUID, guid);
            }
        }
    }

    public List<String> getForwardedAddresses() {
        return forwardedAddresses;
    }

    public void setForwardedAddresses(List<String> forwardedAddresses) {
        this.forwardedAddresses = forwardedAddresses;
    }

    public void addRelationshipEndToVertexIdMapping(AtlasObjectId atlasObjectId, Object vertexId) {
        this.relationshipEndToVertexIdMap.put(atlasObjectId, vertexId);
    }

    public Map<AtlasObjectId, Object> getRelationshipEndToVertexIdMap() {
        return this.relationshipEndToVertexIdMap;
    }

    public void saveRelationshipsMutationContext(String event, AtlasRelationship relationship) {
        Set<AtlasRelationship> deletedRelationships = this.relationshipMutationMap.getOrDefault(event, new HashSet<>());
        deletedRelationships.add(relationship);
        this.relationshipMutationMap.put(event, deletedRelationships);
    }

    public void clearMutationContext(String event) {
        this.relationshipMutationMap.remove(event);
    }

    public Map<String, Set<AtlasRelationship>> getRelationshipMutationMap() {
        return relationshipMutationMap;
    }

    public Map<String, String> getLexoRankCache() {
        return lexoRankCache;
    }

    public void setLexoRankCache(Map<String, String> lexoRankCache) {
        this.lexoRankCache = lexoRankCache;
    }

    public void addEdgeLabel(String processEdgeLabel) {
        edgeLabels.add(processEdgeLabel);
    }

    public boolean isEdgeLabelAlreadyProcessed(String processEdgeLabel) {
        return edgeLabels.contains(processEdgeLabel);
    }

    public void addCassandraTagOperation(String entityGuid, CassandraTagOperation operation) {
        cassandraTagOperations.computeIfAbsent(entityGuid, k -> new Stack<>()).push(operation);
    }

    public Map<String, Stack<CassandraTagOperation>> getCassandraTagOperations() {
        return cassandraTagOperations;
    }

    public void addESDeferredOperation(ESDeferredOperation op) {
        esDeferredOperations.add(op);
    }

    public List<ESDeferredOperation> getESDeferredOperations() {
        return esDeferredOperations;
    }

    public void addLineageCalcTime(long additionalTime) {
        this.lineageCalcTime.addAndGet(additionalTime);
    }

    public long getLineageCalcTime() {
        return this.lineageCalcTime.get();
    }

}
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

package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasEntityHeaders;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasConfiguration.ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES;
import static org.apache.atlas.repository.store.graph.v2.EntityGraphMapper.validateProductStatus;

@Component
public class ClassificationAssociator {
    private static final Logger LOG = LoggerFactory.getLogger(ClassificationAssociator.class);

    private static TransactionInterceptHelper transactionInterceptHelper;
    private final EntityGraphRetriever entityGraphRetriever;

    @Inject
    public ClassificationAssociator(TransactionInterceptHelper transactionInterceptHelper, EntityGraphRetriever entityGraphRetriever) {
        ClassificationAssociator.transactionInterceptHelper = transactionInterceptHelper;
        this.entityGraphRetriever = entityGraphRetriever;
    }

    public static class Retriever {
        private final EntityAuditRepository auditRepository;
        private final EntityGraphRetriever entityRetriever;

        public Retriever(AtlasGraph graph, AtlasTypeRegistry typeRegistry, EntityAuditRepository auditRepository, EntityGraphRetriever entityRetriever) {
            this.entityRetriever = entityRetriever;
            this.auditRepository = auditRepository;
        }

        public Retriever(AtlasTypeRegistry typeRegistry, EntityAuditRepository auditRepository, EntityGraphRetriever entityRetriever) {
            this(AtlasGraphProvider.getGraphInstance(), typeRegistry, auditRepository, entityRetriever);
        }

        public AtlasEntityHeaders get(long fromTimestamp, long toTimestamp) throws AtlasBaseException {
            toTimestamp = incrementTimestamp(toTimestamp);
            Set<String> guids = auditRepository.getEntitiesWithTagChanges(fromTimestamp, toTimestamp);

            Map<String, AtlasEntityHeader> guidEntityHeaderMap = new HashMap<>();
            for (String guid : guids) {
                AtlasEntityHeader entityHeader = getEntityHeaderByGuid(guid);
                if (entityHeader == null) {
                    continue;
                }

                guidEntityHeaderMap.put(guid, entityHeader);
            }

            guids.clear();
            return new AtlasEntityHeaders(guidEntityHeaderMap);
        }

        private AtlasEntityHeader getEntityHeaderByGuid(String guid) {
            try {
                return entityRetriever.toAtlasEntityHeaderWithClassifications(guid);
            } catch (AtlasBaseException e) {
                LOG.error("Error fetching entity: {}", guid, e);
            }

            return null;
        }

        private long incrementTimestamp(long t) {
            return t + 1;
        }
    }

    public static class Updater {
        static final String ATTR_NAME_QUALIFIED_NAME = "qualifiedName";
        static final String STATUS_DONE = "(Done)";
        static final String STATUS_SKIPPED = "(Skipped)";
        static final String STATUS_PARTIAL = "(Partial)";

        private static final String PROCESS_FORMAT = "%s:%s:%s:%s -> %s:%s";
        public static final String PROCESS_ADD = "Add";
        public static final String PROCESS_UPDATE = "Update";
        public static final String PROCESS_DELETE = "Delete";
        public static final String PROCESS_NOOP = "NOOP";
        static final String JSONIFY_STRING_FORMAT = "\"%s\",";

        private final AtlasGraph graph;
        private final AtlasTypeRegistry typeRegistry;
        private final AtlasEntityStore entitiesStore;
        private final EntityGraphRetriever entityRetriever;
        private final EntityGraphMapper entityGraphMapper;
        private final IAtlasEntityChangeNotifier entityChangeNotifier;
        private final AtlasInstanceConverter instanceConverter;
        private final StringBuilder actionSummary = new StringBuilder();

        private static final boolean IGNORE_REL = ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES.getBoolean();

        public Updater(AtlasGraph graph, AtlasTypeRegistry typeRegistry, AtlasEntityStore entitiesStore,
                       EntityGraphMapper entityGraphMapper, IAtlasEntityChangeNotifier entityChangeNotifier,
                       AtlasInstanceConverter instanceConverter, EntityGraphRetriever entityRetriever) {
            this.graph = graph;
            this.typeRegistry = typeRegistry;
            this.entitiesStore = entitiesStore;
            this.entityGraphMapper = entityGraphMapper;
            this.entityChangeNotifier = entityChangeNotifier;
            this.instanceConverter = instanceConverter;
            this.entityRetriever = entityRetriever;
        }

        public Updater(AtlasTypeRegistry typeRegistry, AtlasEntityStore entitiesStore,
                       EntityGraphMapper entityGraphMapper, IAtlasEntityChangeNotifier entityChangeNotifier,
                       AtlasInstanceConverter instanceConverter, EntityGraphRetriever entityRetriever) {
            this(AtlasGraphProvider.getGraphInstance(), typeRegistry, entitiesStore, entityGraphMapper, entityChangeNotifier, instanceConverter, entityRetriever);
        }

        public void setClassifications(Map<String, AtlasEntityHeader> map, boolean overrideClassifications) throws AtlasBaseException {
            RequestContext.get().setDelayTagNotifications(true);

            for (String guid  : map.keySet()) {
                AtlasEntityHeader incomingEntityHeader = map.get(guid);
                String typeName = incomingEntityHeader.getTypeName();
                AtlasEntityHeader entityToBeChanged;
                AtlasVertex assetVertex = AtlasGraphUtilsV2.findByGuid(this.graph, guid);

                validateProductStatus(assetVertex);

                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
                if (entityType == null) {
                    throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), incomingEntityHeader.getTypeName());
                }

                entityToBeChanged = getByGuid(guid);

                if (entityToBeChanged == null) {
                    String qualifiedName = getUniqueAttributeName(entityType, incomingEntityHeader);
                    entityToBeChanged = getByUniqueAttributes(entityType, qualifiedName, incomingEntityHeader.getAttributes());
                }


                if (entityToBeChanged == null) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
                }
                Map<String, List<AtlasClassification>> operationListMap = new HashMap<>();
                if(overrideClassifications) {
                    operationListMap = computeChanges(incomingEntityHeader, entityToBeChanged);
                } else {
                    operationListMap = validateAndTransfer(incomingEntityHeader, entityToBeChanged);
                }
                try {
                    commitChanges(guid, typeName, operationListMap);
                } catch (AtlasBaseException e) {
                    e.setEntityGuid(guid);
                    throw e;
                }
            }

            //send Notifications & update __classificationText
            RequestContext.get().clearEntityCache();

            AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("commitChanges.notify");
            Map<AtlasClassification, Collection<Object>> deleted = RequestContext.get().getDeletedClassificationAndVertices();
            Set<AtlasVertex> allVertices = new HashSet<>();

            if (MapUtils.isNotEmpty(deleted)) {
                for (AtlasClassification deletedClassification: deleted.keySet()) {
                    Collection<Object> vertices =  deleted.get(deletedClassification);
                    List<AtlasEntity> propagatedEntities = new ArrayList<>();

                    for (Object obj: vertices) {
                        AtlasVertex vertex = (AtlasVertex) obj;

                        AtlasEntity entity;
                        entity = instanceConverter.getAndCacheEntity(GraphHelper.getGuid(vertex), IGNORE_REL);

                        allVertices.add(vertex);
                        propagatedEntities.add(entity);
                    }

                    entityChangeNotifier.onClassificationsDeletedFromEntities(propagatedEntities, Collections.singletonList(deletedClassification));
                }
            }

            Map<AtlasClassification, Collection<Object>> added = RequestContext.get().getAddedClassificationAndVertices();
            if (MapUtils.isNotEmpty(added)) {
                for (AtlasClassification addedClassification: added.keySet()) {
                    Collection<Object> vertices =  added.get(addedClassification);
                    List<AtlasEntity> propagatedEntities = new ArrayList<>();
                    Set<AtlasVertex> propagatedVertices = new HashSet<>();
                    for (Object obj: vertices) {
                        AtlasVertex vertex = (AtlasVertex) obj;

                        AtlasEntity entity;
                        entity = instanceConverter.getAndCacheEntity(GraphHelper.getGuid(vertex), IGNORE_REL);

                        allVertices.add(vertex);
                        propagatedVertices.add(vertex);
                        propagatedEntities.add(entity);
                    }
                    //new method to populate all primitive fields in kafka
                    entityChangeNotifier.onClassificationsAddedToEntities(propagatedEntities, Collections.singletonList(addedClassification), false);
                }
            }

            if (!FeatureFlagStore.isTagV2Enabled()) {
                entityGraphMapper.updateClassificationText(null, allVertices);
            }
            transactionInterceptHelper.intercept();

            RequestContext.get().endMetricRecord(recorder);
            RequestContext.get().setDelayTagNotifications(false);
        }

        private Map<String, List<AtlasClassification>> validateAndTransfer(AtlasEntityHeader incomingEntityHeader, AtlasEntityHeader entityToBeChanged) throws AtlasBaseException {
            Map<String, List<AtlasClassification>> operationListMap = new HashMap<>();

            Set<String> preExistingClassificationKeys = new HashSet<>();
            List<AtlasClassification> filteredRemoveClassifications = new ArrayList<>();

            ListOps<AtlasClassification> listOps = new ListOps<>();

            for (AtlasClassification classification : Optional.ofNullable(entityToBeChanged.getClassifications()).orElse(Collections.emptyList())) {
                if (entityToBeChanged.getGuid().equals(classification.getEntityGuid())) {
                    String key = generateClassificationComparisonKey(classification);
                    preExistingClassificationKeys.add(key);  // Track pre-existing keys
                }
            }

            for (AtlasClassification classification : Optional.ofNullable(incomingEntityHeader.getRemoveClassifications()).orElse(Collections.emptyList())) {
                if (entityToBeChanged.getGuid().equals(classification.getEntityGuid())) {
                    String key = generateClassificationComparisonKey(classification);
                    // If the classification doesn't exist in pre-existing keys, log it
                    if (!preExistingClassificationKeys.contains(key)) {
                        String typeName = key.split("\\|")[1];
                        LOG.info("Classification {} is not associated with entity {}", typeName, entityToBeChanged.getGuid());
                    } else {
                        filteredRemoveClassifications.add(classification);
                    }
                }
            }

            List<AtlasClassification> filteredClassifications = Optional.ofNullable(incomingEntityHeader.getAddOrUpdateClassifications())
                    .orElse(Collections.emptyList())
                    .stream()
                    .filter(classification -> classification.getEntityGuid().equals(entityToBeChanged.getGuid()))
                    .collect(Collectors.toList());

            List<AtlasClassification> incomingClassifications = listOps.filter(incomingEntityHeader.getGuid(), filteredClassifications);
            List<AtlasClassification> entityClassifications = listOps.filter(entityToBeChanged.getGuid(), entityToBeChanged.getClassifications());

            bucket(PROCESS_DELETE, operationListMap, filteredRemoveClassifications);
            bucket(PROCESS_UPDATE, operationListMap, listOps.intersect(incomingClassifications, entityClassifications));
            bucket(PROCESS_ADD, operationListMap, listOps.subtract(incomingClassifications, entityClassifications));

            return operationListMap;
        }

        private String generateClassificationComparisonKey(AtlasClassification classification) {
            return classification.getEntityGuid() + "|" + classification.getTypeName();
        }

        private void commitChanges(String entityGuid, String typeName, Map<String, List<AtlasClassification>> operationListMap) throws AtlasBaseException {
            if (MapUtils.isEmpty(operationListMap)) {
                return;
            }

            deleteClassifications(entityGuid, typeName, operationListMap.get(PROCESS_DELETE));
            updateClassifications(entityGuid, typeName, operationListMap.get(PROCESS_UPDATE));
            addClassifications(entityGuid, typeName, operationListMap.get(PROCESS_ADD));

            operationListMap.clear();
        }

        private Map<String, List<AtlasClassification>> computeChanges(AtlasEntityHeader incomingEntityHeader, AtlasEntityHeader entityToBeUpdated) throws AtlasBaseException {
            if (incomingEntityHeader == null || entityToBeUpdated == null) {
                return null;
            }

            ListOps<AtlasClassification> listOps = new ListOps<>();
            List<AtlasClassification> incomingClassifications = listOps.filter(incomingEntityHeader.getGuid(), incomingEntityHeader.getClassifications());
            List<AtlasClassification> entityClassifications = listOps.filter(entityToBeUpdated.getGuid(), entityToBeUpdated.getClassifications());

            if (CollectionUtils.isEmpty(incomingClassifications) && CollectionUtils.isEmpty(entityClassifications)) {
                return null;
            }

            Map<String, List<AtlasClassification>> operationListMap = new HashMap<>();

            bucket(PROCESS_DELETE, operationListMap, listOps.subtract(entityClassifications, incomingClassifications));
            bucket(PROCESS_UPDATE, operationListMap, listOps.intersect(incomingClassifications, entityClassifications));
            bucket(PROCESS_ADD, operationListMap, listOps.subtract(incomingClassifications, entityClassifications));

            return operationListMap;
        }



        private void bucket(String op, Map<String, List<AtlasClassification>> operationListMap, List<AtlasClassification> results) {
            if (CollectionUtils.isEmpty(results)) {
                return;
            }

            operationListMap.put(op, results);
        }

        private void addClassifications(String entityGuid, String typeName, List<AtlasClassification> list) throws AtlasBaseException {
            if (CollectionUtils.isEmpty(list)) {
                return;
            }
            String classificationNames = getClassificationNames(list);
            try {
                entitiesStore.addClassifications(entityGuid, list);
            } catch (AtlasBaseException e) {
                LOG.error("Failed to add classifications {}, entity with guid {}", classificationNames, entityGuid);
                throw e;
            }
        }

        private void updateClassifications(String entityGuid, String typeName, List<AtlasClassification> list) throws AtlasBaseException {
            if (CollectionUtils.isEmpty(list)) {
                return;
            }
            String classificationNames = getClassificationNames(list);
            try {
                entitiesStore.updateClassifications(entityGuid, list);
            } catch (AtlasBaseException e) {
                LOG.error("Failed to update classifications {}, entity with guid {}", classificationNames, entityGuid);
                throw e;
            }
        }

        private void deleteClassifications(String entityGuid, String typeName, List<AtlasClassification> list) throws AtlasBaseException {
            if (CollectionUtils.isEmpty(list)) {
                return;
            }

            for (AtlasClassification c : list) {
                try {
                    entitiesStore.deleteClassification(entityGuid, c.getTypeName());
                } catch (AtlasBaseException e) {
                    LOG.error("Failed to remove classification association between {}, entity with guid {}", c.getTypeName(), c.getEntityGuid());
                    throw e;
                }
            }
        }

        AtlasEntityHeader getByUniqueAttributes(AtlasEntityType entityType, String qualifiedName, Map<String, Object> attrValues) {
            try {
                AtlasVertex vertex = AtlasGraphUtilsV2.findByUniqueAttributes(this.graph, entityType, attrValues);
                if (vertex == null) {
                    return null;
                }

                return entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);
            } catch (AtlasBaseException e) {
                LOG.warn("{}:{} could not be processed!", entityType, qualifiedName);
                return null;
            } catch (Exception ex) {
                LOG.error("{}:{} could not be processed!", entityType, qualifiedName, ex);
                return null;
            }
        }

        AtlasEntityHeader getByGuid(String guid) throws AtlasBaseException {
            AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(this.graph, guid);
            if (vertex == null) {
                return null;
            }

            return entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);
        }

        private String getClassificationNames(List<AtlasClassification> list) {
            return list.stream().map(AtlasClassification::getTypeName).collect(Collectors.joining(", "));
        }

        private String getUniqueAttributeName(AtlasEntityType entityType, AtlasEntityHeader entityHeader) {
            String uniqueAttrName = ATTR_NAME_QUALIFIED_NAME;
            if (!entityHeader.getAttributes().containsKey(uniqueAttrName)) {
                uniqueAttrName = getUniqueAttributeName(entityType);
            }

            return uniqueAttrName;
        }

        private String getUniqueAttributeName(AtlasEntityType entityType) {
            return entityType.getUniqAttributes()
                    .entrySet()
                    .stream()
                    .findFirst()
                    .get().getKey();
        }

        private void summarize(String... s) {
            summarizeFormat(PROCESS_FORMAT, s);
        }

        private void summarizeFormat(String format, String... s) {
            summarize(String.format(format, s));
        }

        private void summarize(String s) {
            actionSummary.append(String.format(JSONIFY_STRING_FORMAT, s));
        }

        private String getJsonArray(StringBuilder actionSummary) {
            return "[" + StringUtils.removeEnd(actionSummary.toString(), ",") + "]";
        }
    }

    private static class ListOps<V extends AtlasClassification> {
        public List<V> intersect(List<V> lhs, List<V> rhs) {
            if (CollectionUtils.isEmpty(rhs)) {
                return null;
            }

            List<V> result = new ArrayList<>();
            for (V c : rhs) {
                V foundSame = findObjectFrom(lhs, c);
                V foundSameTypeName = findFrom(lhs, c);
                if ((foundSameTypeName != null) && (foundSame == null)) {
                    result.add(foundSameTypeName);
                }
            }
            return result;
        }

        public List<V> subtract(List<V> lhs, List<V> rhs) {
            if (CollectionUtils.isEmpty(lhs)) {
                return null;
            }

            List<V> result = new ArrayList<>();
            for (V c : lhs) {
                V found = findFrom(rhs, c);
                if (found == null) {
                    result.add(c);
                }
            }

            return result;
        }

        private V findObjectFrom(List<V> reference, V check) {
            return (V) CollectionUtils.find(reference, ox ->
                    ((V) ox).checkForUpdate(check));
        }

        private V findFrom(List<V> reference, V check) {
            return (V) CollectionUtils.find(reference, ox ->
                    ((V) ox).getTypeName().equals(check.getTypeName()));
        }

        public List<V> filter(String guid, List<V> list) {
            if (CollectionUtils.isEmpty(list)) {
                return list;
            }

            return list.stream().filter(x -> x != null &&
                                    (StringUtils.isEmpty(guid) || StringUtils.isEmpty(x.getEntityGuid()))
                                    || x.getEntityGuid().equals(guid)).collect(Collectors.toList());
        }
    }
}

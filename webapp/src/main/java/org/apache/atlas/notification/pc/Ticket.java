/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.notification.pc;

import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.notification.TopicPartitionOffsetResult;
import org.apache.atlas.notification.preprocessor.EntityPreprocessor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Ticket {
    private static final Logger LOG = LoggerFactory.getLogger(Ticket.class);
    private static final String QUALIFIED_NAME_NAMESPACE_SEPARATOR = "@";
    private static final String QUALIFIED_NAME_LHS_SEPARATOR       = ".";
    private static final String QUALIFIED_NAME_FORMAT              = "%s" + QUALIFIED_NAME_NAMESPACE_SEPARATOR + "%s";
    private static final Set<String> processTypes                  = new HashSet<>();

    final AtlasKafkaMessage<HookNotification> msg;

    private final Set<String> qualifiedNamesSet;
    private final Set<String> referencedSet;
    private final Set<String> allTypes;
    private final List<String> dependents;

    public static Set<String> getProcessNameTypes() {
        return processTypes;
    }

    public Ticket(AtlasKafkaMessage<HookNotification> msg) {
        this.msg = msg;
        this.qualifiedNamesSet = new HashSet<>();
        this.referencedSet = new HashSet<>();
        this.allTypes = new HashSet<>();

        try {
            updateMetaInfo(msg);
        } catch (Exception e) {
            LOG.error("Error fetching metadata.", e);
        }

        dependents = new ArrayList<>();
    }

    private void updateMetaInfo(AtlasKafkaMessage<HookNotification> msg) {
        HookNotification message = msg.getMessage();
        switch (message.getType()) {
            case ENTITY_CREATE_V2: {
                final HookNotification.EntityCreateRequestV2 createRequestV2 = (HookNotification.EntityCreateRequestV2) message;
                updateMetadata(createRequestV2.getEntities());
                break;
            }
            case ENTITY_FULL_UPDATE_V2: {
                final HookNotification.EntityUpdateRequestV2 updateRequest = (HookNotification.EntityUpdateRequestV2) message;
                updateMetadata(updateRequest.getEntities());
                break;
            }

            case ENTITY_PARTIAL_UPDATE_V2: {
                final HookNotification.EntityPartialUpdateRequestV2 partialUpdateRequest = (HookNotification.EntityPartialUpdateRequestV2) message;
                updateMetadata(partialUpdateRequest.getEntityId());

                fillQualifiedNames(qualifiedNamesSet, partialUpdateRequest.getEntity());
                fillRefs(referencedSet, qualifiedNamesSet, partialUpdateRequest.getEntity());
                break;
            }

            case ENTITY_DELETE_V2: {
                final HookNotification.EntityDeleteRequestV2 deleteRequest = (HookNotification.EntityDeleteRequestV2) message;
                for (AtlasObjectId objectId : deleteRequest.getEntities()) {
                    updateMetadata(objectId);
                }
                break;
            }
        }
    }

    private void updateMetadata(AtlasObjectId objectId) {
        if (objectId.getUniqueAttributes().values() instanceof Collection) {
            this.allTypes.add(objectId.getTypeName());

            for (Object o : objectId.getUniqueAttributes().values()) {
                if (o instanceof String) {
                    String qualifiedName = (String) o;
                    this.qualifiedNamesSet.add(qualifiedName);
                    addInferredQNames(this.referencedSet, qualifiedName, true);
                }
            }
        }
    }

    private void updateMetadata(AtlasEntity.AtlasEntitiesWithExtInfo entities) {
        addEntityQualifiedNames(this.qualifiedNamesSet, entities);
        getReferencedSet(this.referencedSet, this.qualifiedNamesSet, entities);
        addTypes(this.allTypes, entities);
    }

    public void addTypes(Set<String> ret, AtlasEntity.AtlasEntitiesWithExtInfo entities) {
        if (entities != null && CollectionUtils.isNotEmpty(entities.getEntities())) {
            for (AtlasEntity entity : entities.getEntities()) {
                ret.add(entity.getTypeName());
            }
        }

        if (entities != null && MapUtils.isNotEmpty(entities.getReferredEntities())) {
            for (AtlasEntity entity : entities.getReferredEntities().values()) {
                ret.add(entity.getTypeName());
            }
        }
    }

    public void getReferencedSet(Set<String> ret, Set<String> qualifiedNamesSet, AtlasEntity.AtlasEntitiesWithExtInfo entities) {
        if (entities != null && CollectionUtils.isNotEmpty(entities.getEntities())) {
            for (AtlasEntity entity : entities.getEntities()) {
                fetchReferences(ret, qualifiedNamesSet, entity);
            }
        }

        if (entities != null && MapUtils.isNotEmpty(entities.getReferredEntities())) {
            for (AtlasEntity entity : entities.getReferredEntities().values()) {
                fetchReferences(ret, qualifiedNamesSet, entity);
            }
        }
    }

    public static void fillQualifiedNames(Set<String> ret, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        if (entityWithExtInfo == null) {
            return;
        }

        fillQualifiedNames(ret, Collections.singleton(entityWithExtInfo.getEntity()));
        if (MapUtils.isNotEmpty(entityWithExtInfo.getReferredEntities())) {
            fillQualifiedNames(ret, entityWithExtInfo.getReferredEntities().values());
        }
    }

    public void fillRefs(Set<String> ret, Set<String> qualifiedNamesSet, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        if (entityWithExtInfo == null) {
            return;
        }

        fetchReferences(ret, qualifiedNamesSet, entityWithExtInfo.getEntity());
        if (MapUtils.isNotEmpty(entityWithExtInfo.getReferredEntities())) {
            for (AtlasEntity entity : entityWithExtInfo.getReferredEntities().values()) {
                fetchReferences(ret, qualifiedNamesSet, entity);
            }
        }
    }

    public static void addEntityQualifiedNames(Set<String> ret, AtlasEntity.AtlasEntitiesWithExtInfo entities) {
        if (entities == null) {
            return;
        }

        fillQualifiedNames(ret, entities.getEntities());
        if (MapUtils.isNotEmpty(entities.getReferredEntities())) {
            fillQualifiedNames(ret, entities.getReferredEntities().values());
        }
    }

    private void fetchReferences(Set<String> ret, Set<String> qualifiedNamesSet, AtlasEntity entity) {
        if (entity == null) {
            return;
        }

        boolean processReferenceFound = processTypes.contains(entity.getTypeName());
        if (MapUtils.isNotEmpty(entity.getRelationshipAttributes())) {
            for (Object o : entity.getRelationshipAttributes().values()) {
                if (o instanceof List) {
                    List list = (List) o;
                    for (Object ox : list) {
                        String qualifiedName = EntityPreprocessor.getQualifiedName(ox);

                        if (StringUtils.isNotEmpty(qualifiedName) && !qualifiedNamesSet.contains(qualifiedName)) {
                            ret.add(qualifiedName);

                            if (processReferenceFound) {
                                addInferredQNames(ret, qualifiedName);
                            }
                        }
                    }
                } else {
                    String qualifiedName = EntityPreprocessor.getQualifiedName(o);
                    if (StringUtils.isNotEmpty(qualifiedName) && !qualifiedNamesSet.contains(qualifiedName)) {
                        ret.add(qualifiedName);

                        if (processReferenceFound) {
                            addInferredQNames(ret, qualifiedName);
                        }
                    }
                }
            }
        }

        if (MapUtils.isNotEmpty(entity.getAttributes())) {
            for (Object o : entity.getAttributes().values()) {
                if (o instanceof List) {
                    List list = (List) o;
                    for (Object ox : list) {
                        String qualifiedName = EntityPreprocessor.getQualifiedName(ox);

                        if (StringUtils.isNotEmpty(qualifiedName) && !qualifiedNamesSet.contains(qualifiedName)) {
                            ret.add(qualifiedName);

                            if (processReferenceFound) {
                                addInferredQNames(ret, qualifiedName);
                            }
                        }
                    }
                } else {
                    String qualifiedName = EntityPreprocessor.getQualifiedName(o);

                    if (StringUtils.isNotEmpty(qualifiedName) && !qualifiedNamesSet.contains(qualifiedName)) {
                        ret.add(qualifiedName);

                        if (processReferenceFound) {
                            addInferredQNames(ret, qualifiedName);
                        }
                    }
                }
            }
        }

        if (processReferenceFound) {
            this.qualifiedNamesSet.addAll(this.referencedSet);
        }
    }

    private static void fillQualifiedNames(Set<String> ret, Collection<AtlasEntity> entityCollection) {
        if (CollectionUtils.isNotEmpty(entityCollection)) {
            for (AtlasEntity entity : entityCollection) {
                String qualifiedName = EntityPreprocessor.getQualifiedName(entity);
                if (StringUtils.isEmpty(qualifiedName)) {
                    continue;
                }

                ret.add(qualifiedName);
            }
        }
    }
    private static void addInferredQNames(Set<String> ret, String qualifiedName) {
        addInferredQNames(ret, qualifiedName, false);
    }

    private static void addInferredQNames(Set<String> ret, String qualifiedName, boolean inferAll) {
        if (StringUtils.isEmpty(qualifiedName) || !StringUtils.contains(qualifiedName, QUALIFIED_NAME_LHS_SEPARATOR)) {
            return;
        }

        String namespace = StringUtils.substringAfter(qualifiedName, QUALIFIED_NAME_NAMESPACE_SEPARATOR);
        String shortened = qualifiedName;
        while (StringUtils.isNotEmpty(shortened) && StringUtils.contains(shortened, QUALIFIED_NAME_LHS_SEPARATOR)) {
            shortened = StringUtils.substringBeforeLast(shortened, QUALIFIED_NAME_LHS_SEPARATOR);
            if (inferAll || shortened.contains(".")) {
                ret.add(String.format(QUALIFIED_NAME_FORMAT, shortened, namespace));
            }
        }
    }

    public boolean isMessageHandled() {
        return CollectionUtils.isNotEmpty(this.allTypes)
                || CollectionUtils.isNotEmpty(this.qualifiedNamesSet)
                || CollectionUtils.isNotEmpty(this.referencedSet);
    }

    public String getKey() {
        return getKey(msg.getTopicPartition().topic(), msg.getTopicPartition().partition(), msg.getOffset());
    }

    public static String getKey(String topic, long partition, long offset) {
        // TODO: Remove before commit
//        return String.format("%s-%s-%s", topic, partition, offset);
        return String.format("%s", offset);
    }

    public AtlasKafkaMessage<HookNotification> getMessage() {
        return msg;
    }

    public Set<String> getQualifiedNamesSet() {
        return this.qualifiedNamesSet;
    }

    public Set<String> getReferencedSet() {
        return this.referencedSet;
    }

    public Set<String> getTypes() {
        return this.allTypes;
    }

    public void addDependents(Collection<String> dependents) {
        this.dependents.addAll(dependents);
    }

    public List<String> getDependents() {
        return this.dependents;
    }

    public TopicPartitionOffsetResult createTopicPartitionOffsetResult() {
        return new TopicPartitionOffsetResult(this.msg.getTopicPartition(), this.msg.getOffset());
    }
}

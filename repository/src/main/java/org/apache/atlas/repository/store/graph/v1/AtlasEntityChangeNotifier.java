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
package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.graph.FullTextMapperV2;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;


@Component
public class AtlasEntityChangeNotifier {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityChangeNotifier.class);

    private final Set<EntityChangeListener> entityChangeListeners;
    private final AtlasInstanceConverter    instanceConverter;

    @Inject
    private FullTextMapperV2 fullTextMapperV2;

    @Inject
    public AtlasEntityChangeNotifier(Set<EntityChangeListener> entityChangeListeners,
                                     AtlasInstanceConverter    instanceConverter) {
        this.entityChangeListeners = entityChangeListeners;
        this.instanceConverter     = instanceConverter;
    }

    public void onEntitiesMutated(EntityMutationResponse entityMutationResponse, boolean isImport) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(entityChangeListeners) || instanceConverter == null) {
            return;
        }

        List<AtlasEntityHeader> createdEntities          = entityMutationResponse.getCreatedEntities();
        List<AtlasEntityHeader> updatedEntities          = entityMutationResponse.getUpdatedEntities();
        List<AtlasEntityHeader> partiallyUpdatedEntities = entityMutationResponse.getPartialUpdatedEntities();
        List<AtlasEntityHeader> deletedEntities          = entityMutationResponse.getDeletedEntities();

        // complete full text mapping before calling toITypedReferenceable(), from notifyListners(), to
        // include all vertex updates in the current graph-transaction
        doFullTextMapping(createdEntities);
        doFullTextMapping(updatedEntities);
        doFullTextMapping(partiallyUpdatedEntities);

        notifyListeners(createdEntities, EntityOperation.CREATE, isImport);
        notifyListeners(updatedEntities, EntityOperation.UPDATE, isImport);
        notifyListeners(partiallyUpdatedEntities, EntityOperation.PARTIAL_UPDATE, isImport);
        notifyListeners(deletedEntities, EntityOperation.DELETE, isImport);
    }

    public void onClassificationAddedToEntity(String entityId, List<AtlasClassification> classifications) throws AtlasBaseException {
        // Only new classifications need to be used for a partial full text string which can be
        // appended to the existing fullText
        updateFullTextMapping(entityId, classifications);

        ITypedReferenceableInstance entity = toITypedReferenceable(entityId);
        List<ITypedStruct>          traits = toITypedStructs(classifications);

        if (entity == null || CollectionUtils.isEmpty(traits)) {
            return;
        }

        for (EntityChangeListener listener : entityChangeListeners) {
            try {
                listener.onTraitsAdded(entity, traits);
            } catch (AtlasException e) {
                throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_FAILED, e, getListenerName(listener), "TraitAdd");
            }
        }
    }

    public void onClassificationDeletedFromEntity(String entityId, List<String> traitNames) throws AtlasBaseException {
        // Since the entity has already been modified in the graph, we need to recursively remap the entity
        doFullTextMapping(entityId);

        ITypedReferenceableInstance entity = toITypedReferenceable(entityId);

        if (entity == null || CollectionUtils.isEmpty(traitNames)) {
            return;
        }

        for (EntityChangeListener listener : entityChangeListeners) {
            try {
                listener.onTraitsDeleted(entity, traitNames);
            } catch (AtlasException e) {
                throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_FAILED, e, getListenerName(listener), "TraitDelete");
            }
        }
    }

    public void onClassificationUpdatedToEntity(String entityId, List<AtlasClassification> classifications) throws AtlasBaseException {
        // Since the classification attributes are updated in the graph, we need to recursively remap the entityText
        doFullTextMapping(entityId);

        ITypedReferenceableInstance entity = toITypedReferenceable(entityId);
        List<ITypedStruct>          traits = toITypedStructs(classifications);

        if (entity == null || CollectionUtils.isEmpty(traits)) {
            return;
        }

        for (EntityChangeListener listener : entityChangeListeners) {
            try {
                listener.onTraitsUpdated(entity, traits);
            } catch (AtlasException e) {
                throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_FAILED, e, getListenerName(listener), "TraitUpdate");
            }
        }
    }

    private String getListenerName(EntityChangeListener listener) {
        return listener.getClass().getSimpleName();
    }

    private void notifyListeners(List<AtlasEntityHeader> entityHeaders, EntityOperation operation, boolean isImport) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(entityHeaders)) {
            return;
        }

        List<ITypedReferenceableInstance> typedRefInsts = toITypedReferenceable(entityHeaders);

        for (EntityChangeListener listener : entityChangeListeners) {
            try {
                switch (operation) {
                    case CREATE:
                        listener.onEntitiesAdded(typedRefInsts, isImport);
                        break;
                    case UPDATE:
                    case PARTIAL_UPDATE:
                        listener.onEntitiesUpdated(typedRefInsts, isImport);
                        break;
                    case DELETE:
                        listener.onEntitiesDeleted(typedRefInsts, isImport);
                        break;
                }
            } catch (AtlasException e) {
                throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_FAILED, e, getListenerName(listener), operation.toString());
            }
        }
    }

    private List<ITypedReferenceableInstance> toITypedReferenceable(List<AtlasEntityHeader> entityHeaders) throws AtlasBaseException {
        List<ITypedReferenceableInstance> ret = new ArrayList<>(entityHeaders.size());

        for (AtlasEntityHeader entityHeader : entityHeaders) {
            ret.add(instanceConverter.getITypedReferenceable(entityHeader.getGuid()));
        }

        return ret;
    }

    private ITypedReferenceableInstance toITypedReferenceable(String entityId) throws AtlasBaseException {
        ITypedReferenceableInstance ret = null;

        if (StringUtils.isNotEmpty(entityId)) {
            ret = instanceConverter.getITypedReferenceable(entityId);
        }

        return ret;
    }

    private List<ITypedStruct> toITypedStructs(List<AtlasClassification> classifications) throws AtlasBaseException {
        List<ITypedStruct> ret = null;

        if (classifications != null) {
            ret = new ArrayList<>(classifications.size());

            for (AtlasClassification classification : classifications) {
                if (classification != null) {
                    ret.add(instanceConverter.getTrait(classification));
                }
            }
        }

        return ret;
    }

    private void doFullTextMapping(List<AtlasEntityHeader> atlasEntityHeaders) {
        if (CollectionUtils.isEmpty(atlasEntityHeaders)) {
            return;
        }

        try {
            if(!AtlasRepositoryConfiguration.isFullTextSearchEnabled()) {
                return;
            }
        } catch (AtlasException e) {
            LOG.warn("Unable to determine if FullText is disabled. Proceeding with FullText mapping");
        }

        for (AtlasEntityHeader atlasEntityHeader : atlasEntityHeaders) {
            String      guid        = atlasEntityHeader.getGuid();
            AtlasVertex atlasVertex = AtlasGraphUtilsV1.findByGuid(guid);

            if(atlasVertex == null) {
                continue;
            }

            try {
                String fullText = fullTextMapperV2.getIndexTextForEntity(guid);

                GraphHelper.setProperty(atlasVertex, Constants.ENTITY_TEXT_PROPERTY_KEY, fullText);
            } catch (AtlasBaseException e) {
                LOG.error("FullText mapping failed for Vertex[ guid = {} ]", guid, e);
            }
        }
    }

    private void updateFullTextMapping(String entityId, List<AtlasClassification> classifications) {
        try {
            if(!AtlasRepositoryConfiguration.isFullTextSearchEnabled()) {
                return;
            }
        } catch (AtlasException e) {
            LOG.warn("Unable to determine if FullText is disabled. Proceeding with FullText mapping");
        }

        if (StringUtils.isEmpty(entityId) || CollectionUtils.isEmpty(classifications)) {
            return;
        }

        AtlasVertex atlasVertex = AtlasGraphUtilsV1.findByGuid(entityId);
        if(atlasVertex == null) {
            return;
        }

        if (atlasVertex == null) {
            LOG.warn("updateFullTextMapping(): no entity exists with guid {}", entityId);
            return;
        }

        try {
            String classificationFullText = fullTextMapperV2.getIndexTextForClassifications(entityId, classifications);
            String existingFullText = (String) GraphHelper.getProperty(atlasVertex, Constants.ENTITY_TEXT_PROPERTY_KEY);

            String newFullText = existingFullText + " " + classificationFullText;
            GraphHelper.setProperty(atlasVertex, Constants.ENTITY_TEXT_PROPERTY_KEY, newFullText);
        } catch (AtlasBaseException e) {
            LOG.error("FullText mapping failed for Vertex[ guid = {} ]", entityId, e);
        }
    }

    private void doFullTextMapping(String guid) {
        AtlasEntityHeader entityHeader = new AtlasEntityHeader();
        entityHeader.setGuid(guid);

        doFullTextMapping(Collections.singletonList(entityHeader));
    }
}

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

package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.util.AtlasEntityUtils;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.type.AtlasBusinessMetadataType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.HashMap;

import static org.apache.atlas.repository.graph.GraphHelper.getCustomAttributes;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_ADD;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_DELETE;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_UPDATE;

public class AtlasEntityComparator {
    private final AtlasTypeRegistry    typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final Map<String, String>  guidRefMap;
    private BulkRequestContext context;

    public AtlasEntityComparator(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, Map<String, String> guidRefMap,
                                 BulkRequestContext creteOrUpdateContext) {
        this.typeRegistry                 = typeRegistry;
        this.entityRetriever              = entityRetriever;
        this.guidRefMap                   = guidRefMap;
        this.context                      = creteOrUpdateContext;
    }

    public AtlasEntityDiffResult getDiffResult(AtlasEntity updatedEntity, AtlasEntity storedEntity, boolean findOnlyFirstDiff) throws AtlasBaseException {
        return getDiffResult(updatedEntity, storedEntity, null, findOnlyFirstDiff);
    }

    public AtlasEntityDiffResult getDiffResult(AtlasEntity updatedEntity, AtlasVertex storedVertex, boolean findOnlyFirstDiff) throws AtlasBaseException {
        return getDiffResult(updatedEntity, null, storedVertex, findOnlyFirstDiff);
    }

    private AtlasEntityDiffResult getDiffResult(AtlasEntity updatedEntity, AtlasEntity storedEntity, AtlasVertex storedVertex, boolean findOnlyFirstDiff) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getDiffResult");
        try{
        AtlasEntity                              diffEntity                       = new AtlasEntity(updatedEntity.getTypeName());
        AtlasEntityType                          entityType                       = typeRegistry.getEntityTypeByName(updatedEntity.getTypeName());
        Map<String, AtlasAttribute>              entityTypeAttributes             = entityType.getAllAttributes();
        Map<String, Map<String, AtlasAttribute>> entityTypeRelationshipAttributes = entityType.getRelationshipAttributes();

        int     sectionsWithDiff                = 0;
        boolean hasDiffInAttributes             = false;
        boolean hasDiffInRelationshipAttributes = false;
        boolean hasDiffInCustomAttributes       = false;
        boolean hasDiffInBusinessAttributes     = false;

        diffEntity.setGuid(updatedEntity.getGuid());

        if (MapUtils.isNotEmpty(updatedEntity.getAttributes())) { // check for attribute value change
            for (Map.Entry<String, Object> entry : updatedEntity.getAttributes().entrySet()) {
                String         attrName  = entry.getKey();
                AtlasAttribute attribute = entityTypeAttributes.get(attrName);

                if (attribute == null) { // no such attribute
                    continue;
                }

                TypeCategory category = attribute.getAttributeType().getTypeCategory();
                boolean isDefaultValueNotNull = !attribute.getAttributeDef().getIsDefaultValueNull();
                Object newVal;

                if (entry.getValue() == null && isDefaultValueNotNull) {
                    switch (category) {
                        case PRIMITIVE:
                            newVal = attribute.getAttributeType().createDefaultValue();
                            break;
                        case ARRAY:
                            newVal = new ArrayList<>();
                            break;
                        case MAP:
                            newVal = MapUtils.EMPTY_MAP;
                            break;
                        default:
                            newVal = entry.getValue();
                    }
                } else {
                    newVal = entry.getValue();
                }

                Object currVal = (storedEntity != null) ? storedEntity.getAttribute(attrName) : entityRetriever.getEntityAttribute(storedVertex, attribute);

                if (!attribute.getAttributeType().areEqualValues(currVal, newVal, guidRefMap)) {
                    hasDiffInAttributes = true;

                    diffEntity.setAttribute(attrName, newVal);

                    if (findOnlyFirstDiff) {
                        return new AtlasEntityDiffResult(diffEntity, true, false, false);
                    }
                }
            }

            if (hasDiffInAttributes) {
                sectionsWithDiff++;
            }
        }

        if (MapUtils.isNotEmpty(updatedEntity.getRelationshipAttributes())) { // check for relationship-attribute value change
            for (Map.Entry<String, Object> entry : updatedEntity.getRelationshipAttributes().entrySet()) {
                String attrName = entry.getKey();

                if (!entityTypeRelationshipAttributes.containsKey(attrName)) {  // no such attribute
                    continue;
                }

                Object         newVal           = entry.getValue();
                String         relationshipType = AtlasEntityUtil.getRelationshipType(newVal);
                AtlasAttribute attribute        = entityType.getRelationshipAttribute(attrName, relationshipType);
                Object         currVal          = (storedEntity != null) ? storedEntity.getRelationshipAttribute(attrName) : entityRetriever.getEntityAttribute(storedVertex, attribute);

                if (!attribute.getAttributeType().areEqualValues(currVal, newVal, guidRefMap)) {
                    hasDiffInRelationshipAttributes = true;

                    diffEntity.setRelationshipAttribute(attrName, newVal);

                    if (findOnlyFirstDiff) {
                        return new AtlasEntityDiffResult(diffEntity, true, false, false);
                    }
                }
            }

            if (hasDiffInRelationshipAttributes) {
                sectionsWithDiff++;
            }
        }

        if (context.isReplaceClassifications() || context.isReplaceTags() || context.isAppendTags()) {
            List<AtlasClassification> newVal  = updatedEntity.getClassifications();
            List<AtlasClassification> currVal;
            if (storedEntity != null) {
                currVal = storedEntity.getClassifications();
            } else {
                if (DynamicConfigStore.isTagV2Enabled()) {
                    currVal = entityRetriever.getDirectClassifications(storedVertex);
                } else {
                    currVal = entityRetriever.getAllClassifications_V1(storedVertex);
                }
            }

            if (context.isReplaceClassifications()) {
                if (!Objects.equals(currVal, newVal)) {
                    diffEntity.setClassifications(newVal);

                    sectionsWithDiff++;

                    if (findOnlyFirstDiff) {
                        return new AtlasEntityDiffResult(diffEntity, true, false, false);
                    }
                }
            } else {

                Map<String, List<AtlasClassification>> diff;

                if (context.isReplaceTags()) {
                    diff = AtlasEntityUtils.getTagsDiffForReplace(updatedEntity.getGuid(),
                            updatedEntity.getClassifications(),
                            currVal);
                } else {
                    diff = AtlasEntityUtils.getTagsDiffForAppend(updatedEntity.getGuid(),
                            updatedEntity.getAddOrUpdateClassifications(),
                            currVal,
                            updatedEntity.getRemoveClassifications());
                }

                if (MapUtils.isNotEmpty(diff) && (diff.containsKey(PROCESS_DELETE) || diff.containsKey(PROCESS_UPDATE) || diff.containsKey(PROCESS_ADD))) {
                    sectionsWithDiff++;
                    RequestContext.get().addTagsDiff(updatedEntity.getGuid(), diff);

                    if (findOnlyFirstDiff) {
                        return new AtlasEntityDiffResult(diffEntity, true, false, false);
                    }
                }
            }
        }

        if (updatedEntity.getCustomAttributes() != null) {
            // event coming from hook does not have custom attributes, such events must not remove existing attributes
            // UI sends empty object in case of intended removal.
            Map<String, String> newCustomAttributes  = updatedEntity.getCustomAttributes();
            Map<String, String> currCustomAttributes = (storedEntity != null) ? storedEntity.getCustomAttributes() : getCustomAttributes(storedVertex);

            if (!Objects.equals(currCustomAttributes, newCustomAttributes)) {
                diffEntity.setCustomAttributes(newCustomAttributes);

                hasDiffInCustomAttributes = true;
                sectionsWithDiff++;

                if (findOnlyFirstDiff && sectionsWithDiff > 1) {
                    return new AtlasEntityDiffResult(diffEntity, true, false, false);
                }
            }
        }

        if (context.isReplaceBusinessAttributes()) {
            Map<String, Map<String, Object>> newBusinessMetadata = updatedEntity.getBusinessAttributes() == null
                    ? getBusinessMetadataFromEntityAttribute(updatedEntity, entityType)
                    : updatedEntity.getBusinessAttributes();
            Map<String, Map<String, Object>> currBusinessMetadata = (storedEntity != null)
                    ? storedEntity.getBusinessAttributes()
                    : entityRetriever.getBusinessMetadata(storedVertex);

            if (!Objects.equals(currBusinessMetadata, newBusinessMetadata)) {
                diffEntity.setBusinessAttributes(newBusinessMetadata);

                hasDiffInBusinessAttributes = true;
                sectionsWithDiff++;

                if (findOnlyFirstDiff && sectionsWithDiff > 1) {
                    return new AtlasEntityDiffResult(diffEntity, true, false, false);
                }
            }
        }

        AtlasEntity.Status newStatus  = updatedEntity.getStatus();
        if (newStatus != null && newStatus.equals(AtlasEntity.Status.ACTIVE)) {
            String currStatus = GraphHelper.getStateAsString(storedVertex);
            if (currStatus != null && !currStatus.equals(newStatus.name())) {
                sectionsWithDiff++;
                diffEntity.setStatus(AtlasEntity.Status.ACTIVE);
            }
        }
            return new AtlasEntityDiffResult(diffEntity, sectionsWithDiff > 0, sectionsWithDiff == 1 && hasDiffInCustomAttributes, sectionsWithDiff == 1 && hasDiffInBusinessAttributes);
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    public Map<String, Map<String, Object>> getBusinessMetadataFromEntityAttribute(AtlasEntity entity, AtlasEntityType entityType) {
        Map<String, Map<String, Object>> businessMetadata = new HashMap<>();

        if (entity == null || entity.getAttributes() == null || entityType == null) {
            return businessMetadata;
        }

        for (String attrName : entity.getAttributes().keySet()) {
            AtlasAttribute attributeDefinition = entityType.getAttribute(attrName);

            if (attributeDefinition instanceof AtlasBusinessMetadataType.AtlasBusinessAttribute) {
                Object entityAttrValue = entity.getAttribute(attrName);

                if (entityAttrValue != null) {
                    String bmTypeName = attributeDefinition.getDefinedInDef().getName();
                    Map<String, Object> bmAttributes = businessMetadata.computeIfAbsent(bmTypeName, k -> new HashMap<>());
                    bmAttributes.put(attributeDefinition.getName(), entityAttrValue);
                }
            }
        }

        return businessMetadata;
    }
    public static class AtlasEntityDiffResult {
        private final AtlasEntity diffEntity;
        private final boolean     hasDifference;
        private final boolean     hasDifferenceOnlyInCustomAttributes;
        private final boolean     hasDifferenceOnlyInBusinessAttributes;

        AtlasEntityDiffResult(AtlasEntity diffEntity, boolean hasDifference, boolean hasDifferenceOnlyInCustomAttributes, boolean hasDifferenceOnlyInBusinessAttributes) {
            this.diffEntity                            = diffEntity;
            this.hasDifference                         = hasDifference;
            this.hasDifferenceOnlyInCustomAttributes   = hasDifferenceOnlyInCustomAttributes;
            this.hasDifferenceOnlyInBusinessAttributes = hasDifferenceOnlyInBusinessAttributes;
        }

        public AtlasEntity getDiffEntity() {
            return diffEntity;
        }

        public boolean hasDifference() {
            return hasDifference;
        }

        public boolean hasDifferenceOnlyInCustomAttributes() {
            return hasDifferenceOnlyInCustomAttributes;
        }

        public boolean hasDifferenceOnlyInBusinessAttributes() {
            return hasDifferenceOnlyInBusinessAttributes;
        }
    }
}

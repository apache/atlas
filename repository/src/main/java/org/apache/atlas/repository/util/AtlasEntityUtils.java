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

package org.apache.atlas.repository.util;

import org.apache.atlas.RequestContext;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_ADD;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_DELETE;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_NOOP;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_UPDATE;

public final class AtlasEntityUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityUtils.class);

    private AtlasEntityUtils() {
    }

    public static String getQualifiedName(AtlasEntity entity) {
        return getStringAttribute(entity, QUALIFIED_NAME);
    }

    public static String getName(AtlasEntity entity) {
        return getStringAttribute(entity, NAME);
    }

    public static List<String> getListAttribute(AtlasStruct entity, String attrName) {
        List<String> ret = new ArrayList<>();

        Object valueObj = entity.getAttribute(attrName);
        if (valueObj != null) {
            ret = (List<String>) valueObj;
        }

        return ret;
    }

    public static String getStringAttribute(AtlasEntity entity, String attrName) {
        Object obj = entity.getAttribute(attrName);
        return obj == null ? null : (String) obj;
    }

    public static String getStringAttribute(AtlasEntityHeader entity, String attrName) {
        Object obj = entity.getAttribute(attrName);
        return obj == null ? null : (String) obj;
    }

    public static Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    public static Map<String, List<AtlasClassification>> getTagsDiffForReplace(String entityGuid,
                                                                               List<AtlasClassification> newTags,
                                                                               List<AtlasClassification> currentTags) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTagsDiffForReplace");

        try {
            List<AtlasClassification> toAdd = new ArrayList<>(0);
            List<AtlasClassification> toUpdate = new ArrayList<>(0);
            List<AtlasClassification> toRemove = new ArrayList<>(0);

            Map<String, AtlasClassification> preserveTagWithKeys = pruneAndGetPropagatedTags(entityGuid, currentTags);
            Map<String, AtlasClassification> currentTagWithKeys = getMapWithTagKeys(entityGuid, currentTags);
            Map<String, AtlasClassification> newTagWithKeys = getMapWithTagKeys(entityGuid, newTags);

            List<String> keysToAdd = (List<String>) CollectionUtils.subtract(newTagWithKeys.keySet(), currentTagWithKeys.keySet());
            List<String> keysToRemove = (List<String>) CollectionUtils.subtract(currentTagWithKeys.keySet(), newTagWithKeys.keySet());
            List<String> keysCommon = (List<String>) CollectionUtils.intersection(currentTagWithKeys.keySet(), newTagWithKeys.keySet());


            List<String> keysToUpdate = keysCommon.stream().filter(key -> !newTagWithKeys.get(key).checkForUpdate(currentTagWithKeys.get(key))).collect(Collectors.toList());
            List<String> keysUnChanged = keysCommon.stream().filter(key -> newTagWithKeys.get(key).checkForUpdate(currentTagWithKeys.get(key))).collect(Collectors.toList());

            keysToAdd.forEach(key -> toAdd.add(newTagWithKeys.get(key)));
            keysToRemove.forEach(key -> toRemove.add(currentTagWithKeys.get(key)));
            keysToUpdate.forEach(key -> toUpdate.add(newTagWithKeys.get(key)));
            keysUnChanged.forEach(key -> preserveTagWithKeys.put(key, currentTagWithKeys.get(key)));


            Map<String, List<AtlasClassification>> operationListMap = new HashMap<>(0);
            bucket(PROCESS_DELETE, operationListMap, toRemove);
            bucket(PROCESS_UPDATE, operationListMap, toUpdate);
            bucket(PROCESS_ADD, operationListMap, toAdd);
            bucket(PROCESS_NOOP, operationListMap, new ArrayList<>(preserveTagWithKeys.values()));

            return operationListMap;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static Map<String, List<AtlasClassification>> getTagsDiffForAppend(String entityGuid,
                                                                               List<AtlasClassification> newTags,
                                                                               List<AtlasClassification> currentTags,
                                                                               List<AtlasClassification> tagsToRemove) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTagsDiffForAppend");

        try {
            Map<String, AtlasClassification> preserveTagWithKeys = pruneAndGetPropagatedTags(entityGuid, currentTags);
            Map<String, AtlasClassification> currentTagWithKeys = getMapWithTagKeys(entityGuid, currentTags);
            Map<String, AtlasClassification> newTagWithKeys = getMapWithTagKeys(entityGuid, newTags);
            Map<String, AtlasClassification> removeTagWithKeys = getMapWithTagKeys(entityGuid, tagsToRemove);

            List<AtlasClassification> toAdd = new ArrayList<>(0);
            List<AtlasClassification> toUpdate = new ArrayList<>(0);
            List<AtlasClassification> toRemove = new ArrayList<>(0);

            removeTagWithKeys.keySet().forEach(key -> {
                if (currentTagWithKeys.containsKey(key)) {
                    toRemove.add(removeTagWithKeys.get(key));
                    newTagWithKeys.remove(key); // performs dedup across addOrUpdate & remove tags list
                    currentTagWithKeys.remove(key); // to maintain NOOP list
                } else {
                    //ignoring the tag as it was not already present on the asset
                }
            });

            for (String newTagKey: newTagWithKeys.keySet()) {
                AtlasClassification newTag = newTagWithKeys.get(newTagKey);

                if (currentTagWithKeys.containsKey(newTagKey)) {
                    boolean hasDiff = !newTag.checkForUpdate(currentTagWithKeys.get(newTagKey));
                    if (hasDiff) {
                        toUpdate.add(newTag);
                        currentTagWithKeys.remove(newTagKey);
                    }
                } else {
                    toAdd.add(newTag);
                }
            }

            preserveTagWithKeys.putAll(currentTagWithKeys);

            Map<String, List<AtlasClassification>> operationListMap = new HashMap<>(0);
            bucket(PROCESS_DELETE, operationListMap, toRemove);
            bucket(PROCESS_UPDATE, operationListMap, toUpdate);
            bucket(PROCESS_ADD, operationListMap, toAdd);
            bucket(PROCESS_NOOP, operationListMap, new ArrayList<>(preserveTagWithKeys.values()));

            return operationListMap;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private static String generateClassificationComparisonKey(AtlasClassification classification) {
        return classification.getEntityGuid() + "|" + classification.getTypeName();
    }

    private static void bucket(String op, Map<String, List<AtlasClassification>> operationListMap, List<AtlasClassification> results) {
        if (CollectionUtils.isEmpty(results)) {
            return;
        }

        operationListMap.put(op, results);
    }

    private static Map<String, AtlasClassification> pruneAndGetPropagatedTags(String entityGuid, List<AtlasClassification> tags) {
        Map<String, AtlasClassification> tagsWithKey = new HashMap<>(0);

        if (CollectionUtils.isEmpty(tags)) {
            return tagsWithKey;
        }

        List<AtlasClassification> copyOfTags = new ArrayList<>(tags);
        copyOfTags.forEach(tag -> {
            if (StringUtils.isNotEmpty(tag.getEntityGuid()) && !entityGuid.equals(tag.getEntityGuid())) {
                tags.remove(tag);
                tagsWithKey.put(generateClassificationComparisonKey(tag), tag);
            }
        });

        return tagsWithKey;
    }

    private static Map<String, AtlasClassification> getMapWithTagKeys(String entityGuid, List<AtlasClassification> tags) {
        Map<String, AtlasClassification> tagsWithKey = new HashMap<>(0);

        Optional.ofNullable(tags).orElse(Collections.emptyList()).forEach(x -> {
            if (StringUtils.isEmpty(x.getEntityGuid()) || entityGuid.equals(x.getEntityGuid())) {
                x.setEntityGuid(entityGuid);
                tagsWithKey.put(generateClassificationComparisonKey(x), x);
            }
        });

        return tagsWithKey;
    }

    /**
     * Filters out attributes that are not defined in the entity's TypeDef or have mismatched value types.
     * This prevents invalid/unknown attributes from being published to ES audits and Kafka notifications,
     * avoiding mapping conflicts and field explosion issues (MS-529, MS-501).
     *
     * @param entity       the entity whose attributes should be filtered
     * @param typeRegistry the type registry to look up entity types
     * @return map of removed attributes (for restoration after serialization), or null if nothing was removed
     */
    public static Map<String, Object> filterInvalidAttributes(AtlasEntity entity, AtlasTypeRegistry typeRegistry) {
        Map<String, Object> removedAttributes = null;
        Map<String, Object> entityAttributes  = entity.getAttributes();

        if (MapUtils.isEmpty(entityAttributes)) {
            return null;
        }

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

        if (entityType == null) {
            LOG.warn("filterInvalidAttributes(): unknown type {}. Skipping validation.", entity.getTypeName());
            return null;
        }

        Set<String> attrNames = new HashSet<>(entityAttributes.keySet());

        for (String attrName : attrNames) {
            AtlasAttribute attribute = entityType.getAttribute(attrName);
            Object         attrValue = entityAttributes.get(attrName);

            if (attribute == null) {
                if (removedAttributes == null) {
                    removedAttributes = new HashMap<>();
                }

                entityAttributes.remove(attrName);
                removedAttributes.put(attrName, attrValue);

                LOG.warn("filterInvalidAttributes(): invalid attribute {}.{}. Removed.",
                         entity.getTypeName(), attrName);
            } else if (attrValue != null && !attribute.getAttributeType().isValidValue(attrValue)) {
                if (removedAttributes == null) {
                    removedAttributes = new HashMap<>();
                }

                entityAttributes.remove(attrName);
                removedAttributes.put(attrName, attrValue);

                LOG.warn("filterInvalidAttributes(): type mismatch for attribute {}.{}: expected={}, actual={}. Removed.",
                         entity.getTypeName(), attrName, attribute.getAttributeType().getTypeName(),
                         attrValue.getClass().getSimpleName());
            }
        }

        return removedAttributes;
    }
}

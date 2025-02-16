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
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
            Map<String, List<AtlasClassification>> operationListMap = new HashMap<>();

            if (CollectionUtils.isEmpty(newTags)) {
                if (!CollectionUtils.isEmpty(currentTags)) {
                    // Remove all existing tags
                    bucket(PROCESS_DELETE, operationListMap, currentTags);
                }
                return operationListMap;
            }

            List<AtlasClassification> toAdd = new ArrayList<>();
            List<AtlasClassification> toUpdate = new ArrayList<>();
            List<AtlasClassification> toRemove = new ArrayList<>();
            List<AtlasClassification> toPreserve = new ArrayList<>();

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
            keysUnChanged.forEach(key -> toPreserve.add(currentTagWithKeys.get(key)));


            bucket(PROCESS_DELETE, operationListMap, toRemove);
            bucket(PROCESS_UPDATE, operationListMap, toUpdate);
            bucket(PROCESS_ADD, operationListMap, toAdd);
            bucket(PROCESS_NOOP, operationListMap, toPreserve);

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
            Map<String, AtlasClassification> currentTagWithKeys = getMapWithTagKeys(entityGuid, currentTags);
            Map<String, AtlasClassification> newTagWithKeys = getMapWithTagKeys(entityGuid, newTags);
            Map<String, AtlasClassification> removeTagWithKeys = getMapWithTagKeys(entityGuid, tagsToRemove);

            List<AtlasClassification> toAdd = new ArrayList<>();
            List<AtlasClassification> toUpdate = new ArrayList<>();
            List<AtlasClassification> toRemove = new ArrayList<>();

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

            Map<String, List<AtlasClassification>> operationListMap = new HashMap<>();
            bucket(PROCESS_DELETE, operationListMap, toRemove);
            bucket(PROCESS_UPDATE, operationListMap, toUpdate);
            bucket(PROCESS_ADD, operationListMap, toAdd);
            bucket(PROCESS_NOOP, operationListMap, new ArrayList<>(currentTagWithKeys.values()));

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

    private static Map<String, AtlasClassification> getMapWithTagKeys(String entityGuid, List<AtlasClassification> tags) {
        Map<String, AtlasClassification> tagsWithKey = new HashMap<>();

        Optional.ofNullable(tags).orElse(Collections.emptyList()).forEach(x -> {
            if (StringUtils.isEmpty(x.getEntityGuid())) {
                x.setEntityGuid(entityGuid);
            }
            tagsWithKey.put(generateClassificationComparisonKey(x), x);
        });

        return tagsWithKey;
    }
}

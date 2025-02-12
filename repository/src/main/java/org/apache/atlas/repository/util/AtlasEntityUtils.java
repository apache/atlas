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
import org.apache.atlas.repository.store.graph.v2.ClassificationAssociator;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
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

    public static Map<String, List<AtlasClassification>> validateAndGetTagsDiff(String entityGuid,
                                                                                List<AtlasClassification> newTags,
                                                                                List<AtlasClassification> currentTags,
                                                                                List<AtlasClassification> tagsToRemove) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateAndGetTagsDiff");

        try {
            Map<String, List<AtlasClassification>> operationListMap = new HashMap<>();
            Set<String> preExistingClassificationKeys = new HashSet<>();
            List<AtlasClassification> filteredRemoveClassifications = new ArrayList<>();

            ClassificationAssociator.ListOps<AtlasClassification> listOps = new ClassificationAssociator.ListOps<>();

            for (AtlasClassification classification : Optional.ofNullable(currentTags).orElse(Collections.emptyList())) {
                if (entityGuid.equals(classification.getEntityGuid())) {
                    String key = generateClassificationComparisonKey(classification);
                    preExistingClassificationKeys.add(key);  // Track pre-existing keys
                }
            }

            for (AtlasClassification classification : Optional.ofNullable(tagsToRemove).orElse(Collections.emptyList())) {
                if (entityGuid.equals(classification.getEntityGuid())) {
                    String key = generateClassificationComparisonKey(classification);
                    // If the classification doesn't exist in pre-existing keys, log it
                    if (!preExistingClassificationKeys.contains(key)) {
                        String typeName = key.split("\\|")[1];
                        LOG.info("Classification {} is not associated with entity {}", typeName, entityGuid);
                    } else {
                        filteredRemoveClassifications.add(classification);
                    }
                }
            }

            List<AtlasClassification> filteredClassifications = Optional.ofNullable(newTags)
                    .orElse(Collections.emptyList())
                    .stream()
                    .filter(classification -> classification.getEntityGuid().equals(entityGuid))
                    .collect(Collectors.toList());

            List<AtlasClassification> incomingClassifications = listOps.filter(entityGuid, filteredClassifications);
            List<AtlasClassification> entityClassifications = listOps.filter(entityGuid, currentTags);

            bucket(PROCESS_DELETE, operationListMap, filteredRemoveClassifications);
            bucket(PROCESS_UPDATE, operationListMap, listOps.intersect(incomingClassifications, entityClassifications));
            bucket(PROCESS_ADD, operationListMap, listOps.subtract(incomingClassifications, entityClassifications));

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
}

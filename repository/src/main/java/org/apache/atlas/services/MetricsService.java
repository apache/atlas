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
package org.apache.atlas.services;

import org.apache.atlas.annotation.AtlasService;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.metrics.AtlasMetrics;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.discovery.SearchProcessor.AND_STR;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;

@AtlasService
public class MetricsService {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

    // Query Category constants
    public static final String TYPE    = "type";
    public static final String ENTITY  = "entity";
    public static final String TAG     = "tag";
    public static final String GENERAL = "general";

    // Query names
    protected static final String METRIC_TYPE_COUNT        = TYPE + "Count";
    protected static final String METRIC_TYPE_UNUSED_COUNT = TYPE + "UnusedCount";
    protected static final String METRIC_ENTITY_COUNT      = ENTITY + "Count";
    protected static final String METRIC_ENTITY_DELETED    = ENTITY + "Deleted";
    protected static final String METRIC_ENTITY_ACTIVE     = ENTITY + "Active";
    protected static final String METRIC_TAG_COUNT         = TAG + "Count";
    protected static final String METRIC_ENTITIES_PER_TAG  = TAG + "Entities";

    public static final String METRIC_COLLECTION_TIME                = "collectionTime";

    private final AtlasGraph        atlasGraph;
    private final AtlasTypeRegistry typeRegistry;
    private final String            indexSearchPrefix = AtlasGraphUtilsV2.getIndexSearchPrefix();

    @Inject
    public MetricsService(final AtlasGraph graph, final AtlasTypeRegistry typeRegistry) {
        this.atlasGraph   = graph;
        this.typeRegistry = typeRegistry;

    }

    @SuppressWarnings("unchecked")
    public AtlasMetrics getMetrics() {
        AtlasMetrics metrics = new AtlasMetrics();

        metrics.addMetric(GENERAL, METRIC_TYPE_COUNT, getAllTypesCount());
        metrics.addMetric(GENERAL, METRIC_TAG_COUNT, getAllTagsCount());

        Map<String, Long> activeCountMap  = new HashMap<>();
        Map<String, Long> deletedCountMap = new HashMap<>();

        // metrics for classifications
        Collection<String> classificationDefNames = typeRegistry.getAllClassificationDefNames();

        if (classificationDefNames != null) {
            for (String classificationDefName : classificationDefNames) {
                activeCountMap.put(classificationDefName, getTypeCount(classificationDefName, ACTIVE));
            }
        }

        // metrics for entities
        Collection<String> entityDefNames = typeRegistry.getAllEntityDefNames();

        if (entityDefNames != null) {
            for (String entityDefName : entityDefNames) {
                activeCountMap.put(entityDefName, getTypeCount(entityDefName, ACTIVE));
                deletedCountMap.put(entityDefName, getTypeCount(entityDefName, DELETED));
            }
        }

        Map<String, Long> activeEntityCount  = new HashMap<>();
        Map<String, Long> deletedEntityCount = new HashMap<>();
        long              unusedTypeCount    = 0;
        long              totalEntities      = 0;

        for (String entityDefName : typeRegistry.getAllEntityDefNames()) {
            Long activeCount  = activeCountMap.get(entityDefName);
            Long deletedCount = deletedCountMap.get(entityDefName);

            if (activeCount > 0) {
                activeEntityCount.put(entityDefName, activeCount);
                totalEntities += activeCount.longValue();
            }

            if (deletedCount > 0) {
                deletedEntityCount.put(entityDefName, deletedCount);
                totalEntities += deletedCount.longValue();
            }

            if (activeCount == 0 && deletedCount == 0) {
                unusedTypeCount++;
            }
        }

        metrics.addMetric(GENERAL, METRIC_TYPE_UNUSED_COUNT, unusedTypeCount);
        metrics.addMetric(GENERAL, METRIC_ENTITY_COUNT, totalEntities);
        metrics.addMetric(ENTITY, METRIC_ENTITY_ACTIVE, activeEntityCount);
        metrics.addMetric(ENTITY, METRIC_ENTITY_DELETED, deletedEntityCount);

        Map<String, Long> taggedEntityCount = new HashMap<>();

        for (String classificationName : typeRegistry.getAllClassificationDefNames()) {
            Long count = activeCountMap.get(classificationName);

            if (count > 0) {
                taggedEntityCount.put(classificationName, count);
            }
        }

        metrics.addMetric(TAG, METRIC_ENTITIES_PER_TAG, taggedEntityCount);

        // Miscellaneous metrics
        long collectionTime = System.currentTimeMillis();

        metrics.addMetric(GENERAL, METRIC_COLLECTION_TIME, collectionTime);

        return metrics;
    }

    private Long getTypeCount(String typeName, Status status) {
        String indexQuery = indexSearchPrefix + "\"" + ENTITY_TYPE_PROPERTY_KEY + "\" : (%s)" + AND_STR +
                            indexSearchPrefix + "\"" + STATE_PROPERTY_KEY       + "\" : (%s)";

        indexQuery = String.format(indexQuery, typeName, status.name());

        return atlasGraph.indexQuery(VERTEX_INDEX, indexQuery).vertexTotals();
    }

    private int getAllTypesCount() {
        Collection<String> allTypeNames = typeRegistry.getAllTypeNames();

        return CollectionUtils.isNotEmpty(allTypeNames) ? allTypeNames.size() : 0;
    }

    private int getAllTagsCount() {
        Collection<String> allTagNames = typeRegistry.getAllClassificationDefNames();

        return CollectionUtils.isNotEmpty(allTagNames) ? allTagNames.size() : 0;
    }
}
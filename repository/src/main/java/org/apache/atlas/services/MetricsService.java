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
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.metrics.AtlasMetrics;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.atlas.util.AtlasMetricJVMUtil;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.discovery.SearchProcessor.AND_STR;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeName;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.getIndexSearchPrefix;

@AtlasService
public class MetricsService {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

    // Query Category constants
    public static final String TYPE    = "type";
    public static final String ENTITY  = "entity";
    public static final String TAG     = "tag";
    public static final String GENERAL = "general";
    public static final String SYSTEM  = "system";

    // Query names
    protected static final String METRIC_COLLECTION_TIME   = "collectionTime";
    protected static final String METRIC_STATS             = "stats";
    protected static final String METRIC_TYPE_COUNT        = TYPE + "Count";
    protected static final String METRIC_TYPE_UNUSED_COUNT = TYPE + "UnusedCount";
    protected static final String METRIC_ENTITY_COUNT      = ENTITY + "Count";
    protected static final String METRIC_ENTITY_DELETED    = ENTITY + "Deleted";
    protected static final String METRIC_ENTITY_ACTIVE     = ENTITY + "Active";
    protected static final String METRIC_ENTITY_SHELL      = ENTITY + "Shell";
    protected static final String METRIC_TAG_COUNT         = TAG + "Count";
    protected static final String METRIC_ENTITIES_PER_TAG  = TAG + "Entities";
    protected static final String METRIC_RUNTIME           = "runtime";
    protected static final String METRIC_MEMORY            = "memory";
    protected static final String METRIC_OS                = "os";

    private final AtlasGraph        atlasGraph;
    private final AtlasTypeRegistry typeRegistry;
    private final AtlasMetricsUtil  metricsUtil;
    private final String            indexSearchPrefix = AtlasGraphUtilsV2.getIndexSearchPrefix();

    @Inject
    public MetricsService(final AtlasGraph graph, final AtlasTypeRegistry typeRegistry, AtlasMetricsUtil metricsUtil) {
        this.atlasGraph   = graph;
        this.typeRegistry = typeRegistry;
        this.metricsUtil  = metricsUtil;
    }

    @SuppressWarnings("unchecked")
    @GraphTransaction
    public AtlasMetrics getMetrics() {
        Collection<String> entityDefNames         = typeRegistry.getAllEntityDefNames();
        Collection<String> classificationDefNames = typeRegistry.getAllClassificationDefNames();
        Map<String, Long>  activeEntityCount      = new HashMap<>();
        Map<String, Long>  deletedEntityCount     = new HashMap<>();
        Map<String, Long>  taggedEntityCount      = new HashMap<>();
        long               unusedTypeCount        = 0;
        long               totalEntities          = 0;

        if (entityDefNames != null) {
            for (String entityDefName : entityDefNames) {
                long activeCount  = getTypeCount(entityDefName, ACTIVE);
                long deletedCount = getTypeCount(entityDefName, DELETED);

                if (activeCount > 0) {
                    activeEntityCount.put(entityDefName, activeCount);
                    totalEntities += activeCount;
                }

                if (deletedCount > 0) {
                    deletedEntityCount.put(entityDefName, deletedCount);
                    totalEntities += deletedCount;
                }

                if (activeCount == 0 && deletedCount == 0) {
                    unusedTypeCount++;
                }
            }
        }

        if (classificationDefNames != null) {
            for (String classificationDefName : classificationDefNames) {
                long count = getTypeCount(classificationDefName, ACTIVE);

                if (count > 0) {
                    taggedEntityCount.put(classificationDefName, count);
                }
            }
        }

        AtlasMetrics metrics = new AtlasMetrics();

        metrics.addMetric(GENERAL, METRIC_COLLECTION_TIME, System.currentTimeMillis());
        metrics.addMetric(GENERAL, METRIC_STATS, metricsUtil.getStats()); //add atlas server stats
        metrics.addMetric(GENERAL, METRIC_TYPE_COUNT, getAllTypesCount());
        metrics.addMetric(GENERAL, METRIC_TAG_COUNT, getAllTagsCount());
        metrics.addMetric(GENERAL, METRIC_TYPE_UNUSED_COUNT, unusedTypeCount);
        metrics.addMetric(GENERAL, METRIC_ENTITY_COUNT, totalEntities);

        metrics.addMetric(ENTITY, METRIC_ENTITY_ACTIVE, activeEntityCount);
        metrics.addMetric(ENTITY, METRIC_ENTITY_DELETED, deletedEntityCount);
        metrics.addMetric(ENTITY, METRIC_ENTITY_SHELL, getShellEntityCount());

        metrics.addMetric(TAG, METRIC_ENTITIES_PER_TAG, taggedEntityCount);
        metrics.addMetric(SYSTEM, METRIC_MEMORY, AtlasMetricJVMUtil.getMemoryDetails());
        metrics.addMetric(SYSTEM, METRIC_OS, AtlasMetricJVMUtil.getSystemInfo());
        metrics.addMetric(SYSTEM, METRIC_RUNTIME, AtlasMetricJVMUtil.getRuntimeInfo());

        return metrics;
    }

    private long getTypeCount(String typeName, Status status) {
        Long   ret        = null;
        String indexQuery = indexSearchPrefix + "\"" + ENTITY_TYPE_PROPERTY_KEY + "\" : (%s)" + AND_STR +
                            indexSearchPrefix + "\"" + STATE_PROPERTY_KEY       + "\" : (%s)";

        indexQuery = String.format(indexQuery, typeName, status.name());

        try {
            ret = atlasGraph.indexQuery(VERTEX_INDEX, indexQuery).vertexTotals();
        }catch (Exception e){
            LOG.error("Failed fetching using indexQuery: " + e.getMessage());
        }

        return ret == null ? 0L : ret;
    }

    private Map<String, Long> getShellEntityCount() {
        Map<String, Long> ret            = new HashMap<>();
        String            idxQueryString = getIndexSearchPrefix() + "\"" + IS_INCOMPLETE_PROPERTY_KEY + "\" : " + INCOMPLETE_ENTITY_VALUE.intValue();
        AtlasIndexQuery   idxQuery       = atlasGraph.indexQuery(VERTEX_INDEX, idxQueryString);

        try {
            Iterator<AtlasIndexQuery.Result<Object, Object>> results = idxQuery.vertices();

            while (results != null && results.hasNext()) {
                AtlasVertex entityVertex = results.next().getVertex();
                String      typeName     = getTypeName(entityVertex);

                if (!ret.containsKey(typeName)) {
                    ret.put(typeName, 1L);
                } else {
                    ret.put(typeName, ret.get(typeName) + 1);
                }
            }
        } catch (Throwable t) {
            LOG.warn("getShellEntityCount(): Returned empty result", t);
        } finally {
            atlasGraph.commit();
        }

        return ret;
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
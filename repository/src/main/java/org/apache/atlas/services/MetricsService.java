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

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.annotation.AtlasService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.metrics.AtlasMetrics;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery;
import static org.apache.atlas.util.AtlasGremlinQueryProvider.INSTANCE;

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
    protected static final String METRIC_TYPE_ENTITIES     = TYPE + "Entities";

    protected static final String METRIC_ENTITY_COUNT    = ENTITY + "Count";
    protected static final String METRIC_ENTITY_DELETED  = ENTITY + "Deleted";
    protected static final String METRIC_ENTITY_ACTIVE   = ENTITY + "Active";
    protected static final String METRIC_TAGGED_ENTITIES = ENTITY + "Tagged";
    protected static final String METRIC_TAGS_PER_ENTITY = ENTITY + "Tags";

    protected static final String METRIC_TAG_COUNT        = TAG + "Count";
    protected static final String METRIC_ENTITIES_PER_TAG = TAG + "Entities";

    public static final String METRIC_QUERY_PREFIX                   = "atlas.metric.query.";
    public static final String METRIC_QUERY_CACHE_TTL                = "atlas.metric.query.cache.ttlInSecs";
    public static final String METRIC_QUERY_GREMLIN_TYPES_BATCH_SIZE = "atlas.metric.query.gremlin.typesBatchSize";

    public static final int DEFAULT_CACHE_TTL_IN_SECS  = 900;
    public static final int DEFAULT_GREMLIN_BATCH_SIZE = 25;

    public static final String METRIC_COLLECTION_TIME = "collectionTime";

    private static Configuration             configuration        = null;
    private static AtlasGremlinQueryProvider gremlinQueryProvider = null;

    private final AtlasGraph        atlasGraph;
    private final AtlasTypeRegistry typeRegistry;
    private final int               cacheTTLInSecs;
    private final int               gremlinBatchSize;

    private AtlasMetrics cachedMetrics       = null;
    private long         cacheExpirationTime = 0;


    @Inject
    public MetricsService(final Configuration configuration, final AtlasGraph graph, final AtlasTypeRegistry typeRegistry) {
        this(configuration, graph, typeRegistry, INSTANCE);
    }

    @VisibleForTesting
    MetricsService(Configuration configuration, AtlasGraph graph, AtlasTypeRegistry typeRegistry, AtlasGremlinQueryProvider queryProvider) {
        MetricsService.configuration = configuration;
        atlasGraph = graph;
        cacheTTLInSecs = configuration != null ? configuration.getInt(METRIC_QUERY_CACHE_TTL, DEFAULT_CACHE_TTL_IN_SECS)
                                 : DEFAULT_CACHE_TTL_IN_SECS;
        gremlinBatchSize = configuration != null ? configuration.getInt(METRIC_QUERY_GREMLIN_TYPES_BATCH_SIZE, DEFAULT_GREMLIN_BATCH_SIZE)
                                   : DEFAULT_GREMLIN_BATCH_SIZE;
        gremlinQueryProvider = queryProvider;
        this.typeRegistry = typeRegistry;
    }

    @SuppressWarnings("unchecked")
    public AtlasMetrics getMetrics(boolean ignoreCache) {
        if (ignoreCache || !isCacheValid()) {
            AtlasMetrics metrics = new AtlasMetrics();

            int typeCount = 0, unusedTypeCount = 0;

            Collection<String> typeNames = typeRegistry.getAllTypeNames();
            if (CollectionUtils.isNotEmpty(typeNames)) {
                typeCount = typeNames.size();
            }
            metrics.addMetric(GENERAL, METRIC_TYPE_COUNT, typeCount);

            int tagCount = 0;

            Map<String, Number> activeCountMap  = new HashMap<>();
            Map<String, Number> deletedCountMap = new HashMap<>();

            List<String> classificationDefNames = typeRegistry.getAllClassificationDefNames()
                                                              .stream()
                                                              .map(x -> "'" + x + "'")
                                                              .collect(Collectors.toList());

            if (CollectionUtils.isNotEmpty(classificationDefNames)) {
                tagCount = classificationDefNames.size();
            }
            metrics.addMetric(GENERAL, METRIC_TAG_COUNT, tagCount);

            IntStream
                    .range(0, (classificationDefNames.size() + gremlinBatchSize - 1) / gremlinBatchSize)
                    .mapToObj(i -> classificationDefNames.subList(i * gremlinBatchSize, Math.min(classificationDefNames.size(), (i + 1) * gremlinBatchSize)))
                    .forEach(batch -> captureCounts(batch, activeCountMap, deletedCountMap));


            List<String> entityDefNames = typeRegistry.getAllEntityDefNames()
                                                      .stream()
                                                      .map(x -> "'" + x + "'")
                                                      .collect(Collectors.toList());
            IntStream
                    .range(0, (entityDefNames.size() + gremlinBatchSize - 1) / gremlinBatchSize)
                    .mapToObj(i -> entityDefNames.subList(i * gremlinBatchSize, Math.min(entityDefNames.size(), (i + 1) * gremlinBatchSize)))
                    .forEach(batch -> captureCounts(batch, activeCountMap, deletedCountMap));

            int totalEntities = 0;

            Map<String, Number> activeEntityCount  = new HashMap<>();
            Map<String, Number> deletedEntityCount = new HashMap<>();

            for (String entityDefName : typeRegistry.getAllEntityDefNames()) {
                Number activeCount  = activeCountMap.getOrDefault(entityDefName, null);
                Number deletedCount = deletedCountMap.getOrDefault(entityDefName, null);

                if (activeCount != null) {
                    activeEntityCount.put(entityDefName, activeCount);
                    totalEntities += activeCount.intValue();
                }
                if (deletedCount != null) {
                    deletedEntityCount.put(entityDefName, deletedCount);
                    totalEntities += deletedCount.intValue();
                }
                if (activeCount == null && deletedCount == null) {
                    unusedTypeCount++;
                }
            }

            metrics.addMetric(GENERAL, METRIC_TYPE_UNUSED_COUNT, unusedTypeCount);
            metrics.addMetric(GENERAL, METRIC_ENTITY_COUNT, totalEntities);
            metrics.addMetric(ENTITY, METRIC_ENTITY_ACTIVE, activeEntityCount);
            metrics.addMetric(ENTITY, METRIC_ENTITY_DELETED, deletedEntityCount);

            Map<String, Number> taggedEntityCount = new HashMap<>();
            for (String classificationName : typeRegistry.getAllClassificationDefNames()) {
                Object count = activeCountMap.getOrDefault(classificationName, null);
                if (count != null) {
                    taggedEntityCount.put(classificationName, (Number) count);
                }
            }
            metrics.addMetric(TAG, METRIC_ENTITIES_PER_TAG, taggedEntityCount);


            // Miscellaneous metrics
            long collectionTime = System.currentTimeMillis();

            metrics.addMetric(GENERAL, METRIC_COLLECTION_TIME, collectionTime);

            this.cachedMetrics = metrics;
            this.cacheExpirationTime = (collectionTime + cacheTTLInSecs * 1000);
        }

        return cachedMetrics;
    }

    private void captureCounts(List<String> typeNames, Map<String, Number> activeCountMap, Map<String, Number> deletedCountMap) {
        String typeNamesAsStr = String.join(",", typeNames);
        String query          = String.format(gremlinQueryProvider.getQuery(AtlasGremlinQuery.ENTITY_ACTIVE_METRIC), typeNamesAsStr);
        activeCountMap.putAll(extractCounts(query));

        query = String.format(gremlinQueryProvider.getQuery(AtlasGremlinQuery.ENTITY_DELETED_METRIC), typeNamesAsStr);
        deletedCountMap.putAll(extractCounts(query));

    }

    private Map<String, Number> extractCounts(final String query) {
        Map<String, Number> ret = new HashMap<>();
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Executing query: {}", query);
            }

            Object result = executeQuery(query);
            if (result instanceof List) {
                for (Object entry : (List) result) {
                    if (entry instanceof Map) {
                        ret.putAll((Map<String, Number>) entry);
                    }
                }
            } else if (result instanceof Map) {
                ret.putAll((Map<String, Number>) result);
            } else {
                String returnClassName = result != null ? result.getClass().getSimpleName() : "null";
                LOG.warn("Unhandled return type {} for {}. Ignoring", returnClassName, query);
            }
        } catch (AtlasBaseException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Gremlin execution failed for metric {}", query, e);
            } else {
                LOG.warn("Gremlin execution failed for metric {}", query);
            }
        }
        return ret;
    }

    private Object executeQuery(final String query) throws AtlasBaseException {
        return atlasGraph.executeGremlinScript(query, false);
    }

    private boolean isCacheValid() {
        boolean valid = cachedMetrics != null && System.currentTimeMillis() < cacheExpirationTime;

        if (LOG.isDebugEnabled()) {
            LOG.debug("cachedMetrics: {}", cachedMetrics != null);
            LOG.debug("cacheExpirationTime: {}", cacheExpirationTime);
            LOG.debug("valid: {}", valid);
        }

        return valid;
    }

    private static String getQuery(String type, String name, String defaultQuery) {
        String ret = configuration != null ? configuration.getString(METRIC_QUERY_PREFIX + type + "." + name, defaultQuery)
                             : defaultQuery;

        if (LOG.isDebugEnabled()) {
            LOG.debug("query for {}.{}: {}", type, name, ret);
        }

        return ret;
    }
}

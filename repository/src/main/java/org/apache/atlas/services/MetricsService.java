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

import com.google.inject.Singleton;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.metrics.AtlasMetrics;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

@Singleton
public class MetricsService {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

    public static final String METRIC_QUERY_PREFIX = "atlas.metric.query.";

    public static final String TYPE = "type";
    public static final String ENTITY = "entity";
    public static final String TAG = "tag";
    public static final String GENERAL = "general";

    public static final String METRIC_TYPE_COUNT = TYPE + "Count";
    public static final String METRIC_TYPE_UNUSED_COUNT = TYPE + "UnusedCount";
    public static final String METRIC_TYPE_ENTITIES = TYPE + "Entities";

    public static final String METRIC_ENTITY_COUNT = ENTITY + "Count";
    public static final String METRIC_ENTITY_DELETED = ENTITY + "Deleted";
    public static final String METRIC_TAGGED_ENTITIES = ENTITY + "Tagged";
    public static final String METRIC_TAGS_PER_ENTITY = ENTITY + "Tags";

    public static final String METRIC_TAG_COUNT = TAG + "Count";
    public static final String METRIC_ENTITIES_PER_TAG = TAG + "Entities";

    private static AtlasGraph atlasGraph;
    private static Configuration configuration;

    public MetricsService() throws AtlasException {
        atlasGraph = AtlasGraphProvider.getGraphInstance();
        configuration = ApplicationProperties.get();
    }

    @SuppressWarnings("unchecked")
    public AtlasMetrics getMetrics() {
        AtlasMetrics metrics = new AtlasMetrics();
        for (MetricQuery metricQuery : MetricQuery.values()) {
            try {
                Object result = atlasGraph.executeGremlinScript(metricQuery.query, false);
                if (result instanceof Number) {
                    metrics.addData(metricQuery.type, metricQuery.name, ((Number) result).intValue());
                } else if (result instanceof List) {
                    for (Map resultMap : (List<Map>) result) {
                        metrics.addData(metricQuery.type, (String) resultMap.get("key"), ((Number) resultMap.get("value")).intValue());
                    }
                } else {
                    LOG.warn("Unhandled return type {} for {}. Ignoring", result.getClass().getSimpleName(), metricQuery);
                }
            } catch (ScriptException e) {
                LOG.error("Gremlin execution failed for metric {}", metricQuery, e);
            }
        }

        return metrics;
    }

    /**
     * MetricQuery enum has the capability of reading the queries from the externalized config.
     *
     * The default behavior is to read from the properties and override the statically type query if the configured
     * query is not blank/empty.
     */
    enum MetricQuery {
        TYPE_COUNT(GENERAL, METRIC_TYPE_COUNT, "g.V().has('__type', 'typeSystem').filter({it.'__type.category'.name() != 'TRAIT'}).count()"),
        UNUSED_TYPE_COUNT(GENERAL, METRIC_TYPE_UNUSED_COUNT, "g.V('__type', 'typeSystem').filter({ it.'__type.category'.name() != 'TRAIT' && it.inE.count() == 0}).count()"),
        ENTITY_COUNT(GENERAL, METRIC_ENTITY_COUNT, "g.V().has('__superTypeNames', T.in, ['Referenceable']).count()"),
        TAGS_COUNT(GENERAL, METRIC_TAG_COUNT, "g.V().has('__type', 'typeSystem').filter({it.'__type.category'.name() == 'TRAIT'}).count()"),
        DELETED_ENTITY_COUNT(GENERAL, METRIC_ENTITY_DELETED, "g.V().has('__superTypeNames', T.in, ['Referenceable']).has('__status', 'DELETED').count()"),

        ENTITIES_PER_TYPE(ENTITY, METRIC_TYPE_ENTITIES, "g.V().has('__type', 'typeSystem').has('__type.name').filter({it.'__type.category'.name() != 'TRAIT'}).transform{[key: it.'__type.name', value: it.inE.count()]}.dedup().toList()"),
        TAGGED_ENTITIES(ENTITY, METRIC_TAGGED_ENTITIES, "g.V().has('__superTypeNames', T.in, ['Referenceable']).has('__traitNames').count()"),

        TAGS_PER_ENTITY(TAG, METRIC_TAGS_PER_ENTITY, "g.V().has('__superTypeNames', T.in, ['Referenceable']).has('__traitNames').transform{[ key: it.'Referenceable.qualifiedName', value: it.'__traitNames'.size()]}.dedup().toList()"),
        ;
        private String type;
        private String name;
        private String query;

        private static String getQuery(String type, String name) {
            String metricQueryKey = METRIC_QUERY_PREFIX + type + "." + name;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Looking for configured query {}", metricQueryKey);
            }
            return configuration.getString(metricQueryKey, "");
        }

        MetricQuery(String type, String name, String query) {
            this.type = type;
            this.name = name;
            String configuredQuery = getQuery(type, name);
            this.query = StringUtils.isNotEmpty(configuredQuery) ? configuredQuery : query;
        }

        @Override
        public String toString() {
            return "MetricQuery{" + "type='" + type + '\'' +
                    ", name='" + name + '\'' +
                    ", query='" + query + '\'' +
                    '}';
        }
    }
}

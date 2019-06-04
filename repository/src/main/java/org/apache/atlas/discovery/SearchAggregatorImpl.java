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
package org.apache.atlas.discovery;

import org.apache.atlas.AtlasException;
import org.apache.atlas.model.discovery.AtlasAggregationEntry;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SearchAggregatorImpl implements SearchAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(SearchAggregatorImpl.class);

    private final SearchContext searchContext;


    public SearchAggregatorImpl(SearchContext searchContext) {
        this.searchContext = searchContext;
    }

    public Map<String, List<AtlasAggregationEntry>> getAggregatedMetrics() {
        String              queryString       = searchContext.getSearchParameters().getQuery();
        AtlasGraph          atlasGraph        = searchContext.getGraph();
        Set<String>         aggregationFields = new HashSet<>();
        List<PostProcessor> postProcessors    = new ArrayList<>();

        aggregationFields.add(Constants.ENTITY_TYPE_PROPERTY_KEY);
        aggregationFields.add(Constants.ASSET_OWNER_PROPERTY_KEY);

        postProcessors.add(new ServiceTypeAggregator(searchContext.getTypeRegistry()));

        try {
            Map<String, List<AtlasAggregationEntry>> aggregatedMetrics      = atlasGraph.getGraphIndexClient().getAggregatedMetrics(queryString, aggregationFields);
            Set<String>                              aggregationMetricNames = aggregatedMetrics.keySet();

            for(String aggregationMetricName: aggregationMetricNames) {
                for(PostProcessor postProcessor: postProcessors) {
                    if(postProcessor.needsProcessing(aggregationMetricName)) {
                        postProcessor.prepareForMetric(aggregationMetricName);

                        for(AtlasAggregationEntry aggregationEntry: aggregatedMetrics.get(aggregationMetricName)) {
                            postProcessor.process(aggregationEntry);
                        }

                        postProcessor.handleMetricCompletion(aggregationMetricName);
                    }
                }
            }

            for(PostProcessor postProcessor: postProcessors) {
                postProcessor.handleCompletion(aggregatedMetrics);
            }

            // remove entries with 0 counts
            for (List<AtlasAggregationEntry> entries : aggregatedMetrics.values()) {
                for (ListIterator<AtlasAggregationEntry> iter = entries.listIterator(); iter.hasNext(); ) {
                    AtlasAggregationEntry entry = iter.next();

                    if (entry.getCount() <= 0) {
                        iter.remove();
                    }
                }
            }

            return aggregatedMetrics;
        } catch (AtlasException e) {
            LOG.error("Error encountered in post processing stage of aggrgation metrics collection. Empty metrics will be returned.", e);

            return Collections.EMPTY_MAP;
        }
    }

    static interface PostProcessor {
        boolean needsProcessing(String metricName);
        void prepareForMetric(String metricName);
        void process(AtlasAggregationEntry aggregationEntry);
        void handleMetricCompletion(String metricName);
        void handleCompletion(Map<String, List<AtlasAggregationEntry>> aggregatedMetrics);
    }

    static class ServiceTypeAggregator implements  PostProcessor {
        private static final String SERVICE_TYPE = "ServiceType";

        private final AtlasTypeRegistry                  typeRegistry;
        private       List<AtlasAggregationEntry>        entries;
        private       Map<String, AtlasAggregationEntry> entityType2MetricsMap;

        public ServiceTypeAggregator(AtlasTypeRegistry typeRegistry) {
            this.typeRegistry = typeRegistry;
        }

        @Override
        public boolean needsProcessing(String metricName) {
            return Constants.ENTITY_TYPE_PROPERTY_KEY.equals(metricName);
        }

        @Override
        public void prepareForMetric(String metricName) {
            Map<String, AtlasAggregationEntry> serviceName2MetricsMap = new HashMap<>();

            entries = new ArrayList<>();

            //prepare the service map to aggregations
            for(String serviceName: typeRegistry.getAllServiceTypes()) {
                AtlasAggregationEntry serviceMetrics = new AtlasAggregationEntry(serviceName, 0);

                serviceName2MetricsMap.put(serviceName, serviceMetrics);

                entries.add(serviceMetrics);
            }

            //prepare the map from entity type to aggregations
            entityType2MetricsMap = new HashMap<>();

            for(AtlasEntityType entityType: typeRegistry.getAllEntityTypes()) {
                String serviceName = entityType.getServiceType();

                entityType2MetricsMap.put(entityType.getTypeName(), serviceName2MetricsMap.get(serviceName));
            }
        }

        @Override
        public void process(AtlasAggregationEntry aggregationEntryForType) {
            String                entityType                      = aggregationEntryForType.getName();
            AtlasAggregationEntry atlasAggregationEntryForService = entityType2MetricsMap.get(entityType);

            //atlasAggregationEntryForService can be null--classifications for e.g.
            if (atlasAggregationEntryForService != null) {
                atlasAggregationEntryForService.setCount(atlasAggregationEntryForService.getCount() + aggregationEntryForType.getCount());
            }
        }

        @Override
        public void handleMetricCompletion(String metricName) {
            //do nothing
        }

        @Override
        public void handleCompletion(Map<String, List<AtlasAggregationEntry>> aggregatedMetrics) {
            aggregatedMetrics.put(SERVICE_TYPE, entries);
        }
    }
}

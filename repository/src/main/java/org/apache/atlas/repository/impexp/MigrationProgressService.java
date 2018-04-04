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

package org.apache.atlas.repository.impexp;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.atlas.annotation.AtlasService;
import org.apache.atlas.model.impexp.MigrationStatus;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@AtlasService
@Singleton
public class MigrationProgressService {
    private static final Logger LOG = LoggerFactory.getLogger(MigrationProgressService.class);

    private static final String MIGRATION_STATUS_TYPE_NAME = "__MigrationStatus";
    private static final String CURRENT_INDEX_PROPERTY     = "currentIndex";
    private static final String OPERATION_STATUS_PROPERTY  = "operationStatus";
    private static final String START_TIME_PROPERTY        = "startTime";
    private static final String END_TIME_PROPERTY          = "endTime";
    private static final String TOTAL_COUNT_PROPERTY       = "totalCount";
    private static final String MIGRATION_STATUS_KEY       = "1";

    private final AtlasGraph      graph;
    private final MigrationStatus defaultStatus = new MigrationStatus();
    private       LoadingCache<String, MigrationStatus> cache;

    @Inject
    public MigrationProgressService(AtlasGraph graph) {
        this.graph = graph;
    }

    public MigrationStatus getStatus() {
        try {
            if (cache == null) {
                initCache();
                cache.get(MIGRATION_STATUS_KEY);
            }

            if(cache.size() > 0) {
                return cache.get(MIGRATION_STATUS_KEY);
            }

            return defaultStatus;
        } catch (ExecutionException e) {
            return defaultStatus;
        }
    }

    private void initCache() {
        this.cache = CacheBuilder.newBuilder().refreshAfterWrite(30, TimeUnit.SECONDS).
                build(new CacheLoader<String, MigrationStatus>() {
                    @Override
                    public MigrationStatus load(String key) {
                        try {
                            return from(fetchStatusVertex());
                        } catch (Exception e) {
                            LOG.error("Error retrieving status.", e);
                            return defaultStatus;
                        }
                    }

                    private MigrationStatus from(AtlasVertex vertex) {
                        if (vertex == null) {
                            return null;
                        }

                        MigrationStatus ms = new MigrationStatus();

                        ms.setStartTime(GraphHelper.getSingleValuedProperty(vertex, START_TIME_PROPERTY, Date.class));
                        ms.setEndTime(GraphHelper.getSingleValuedProperty(vertex, END_TIME_PROPERTY, Date.class));
                        ms.setCurrentIndex(GraphHelper.getSingleValuedProperty(vertex, CURRENT_INDEX_PROPERTY, Long.class));
                        ms.setOperationStatus(GraphHelper.getSingleValuedProperty(vertex, OPERATION_STATUS_PROPERTY, String.class));
                        ms.setTotalCount(GraphHelper.getSingleValuedProperty(vertex, TOTAL_COUNT_PROPERTY, Long.class));

                        return ms;
                    }

                    private AtlasVertex fetchStatusVertex() {
                        Iterator<AtlasVertex> itr = graph.query().has(Constants.ENTITY_TYPE_PROPERTY_KEY, MIGRATION_STATUS_TYPE_NAME).vertices().iterator();
                        return itr.hasNext() ? itr.next() : null;
                    }
                });
    }
}

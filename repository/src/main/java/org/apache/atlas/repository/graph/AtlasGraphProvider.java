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

package org.apache.atlas.repository.graph;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides access to the AtlasGraph
 *
 */
@Configuration
public class AtlasGraphProvider implements IAtlasGraphProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasGraphProvider.class);

    private static final String  GRAPH_REPOSITORY_MAX_RETRIES     = "atlas.graph.repository.max.retries";
    private static final String  GRAPH_REPOSITORY_RETRY_SLEEPTIME = "atlas.graph.repository.retry.sleeptime.ms";
    private static final Integer MAX_RETRY_COUNT                  = getMaxRetryCount();
    private static final Long    RETRY_SLEEP_TIME_MS              = getRetrySleepTime();

    private static volatile GraphDatabase<?, ?>                            graphDb;
    private static          org.apache.commons.configuration.Configuration applicationProperties;

    public static <V, E> AtlasGraph<V, E> getGraphInstance() {
        GraphDatabase<?, ?> db    = getGraphDatabase();
        AtlasGraph<?, ?>    graph = db.getGraph();

        return (AtlasGraph<V, E>) graph;
    }

    @VisibleForTesting
    public static void cleanup() {
        getGraphDatabase().cleanup();
    }

    @Override
    @Bean(destroyMethod = "")
    public AtlasGraph get() throws RepositoryException {
        try {
            return getGraphInstance();
        } catch (Exception ex) {
            LOG.info("Failed to obtain graph instance, retrying {} times, error: {}", MAX_RETRY_COUNT, ex);

            return retry();
        }
    }

    public AtlasGraph getBulkLoading() {
        try {
            GraphDatabase<?, ?> graphDB;

            synchronized (AtlasGraphProvider.class) {
                Class<?> implClass = AtlasRepositoryConfiguration.getGraphDatabaseImpl();

                graphDB = (GraphDatabase<?, ?>) implClass.newInstance();
            }

            return graphDB.getGraphBulkLoading();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException("Error initializing graph database", e);
        }
    }

    private static <V, E> GraphDatabase<?, ?> getGraphDatabase() {
        try {
            GraphDatabase<?, ?> me = graphDb;

            if (me == null) {
                synchronized (AtlasGraphProvider.class) {
                    me = graphDb;

                    if (me == null) {
                        Class<?> implClass = AtlasRepositoryConfiguration.getGraphDatabaseImpl();

                        me = (GraphDatabase<V, E>) implClass.newInstance();

                        graphDb = me;
                    }
                }
            }

            return me;
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException("Error initializing graph database", e);
        }
    }

    private AtlasGraph retry() throws RepositoryException {
        int retryCounter = 0;

        while (retryCounter < MAX_RETRY_COUNT) {
            try {
                // Retry after 30 sec to get graph instance
                Thread.sleep(RETRY_SLEEP_TIME_MS);

                return getGraphInstance();
            } catch (Exception ex) {
                retryCounter++;

                LOG.warn("Failed to obtain graph instance on attempt {} of {}", retryCounter, MAX_RETRY_COUNT, ex);

                if (retryCounter >= MAX_RETRY_COUNT) {
                    LOG.info("Max retries exceeded.");

                    break;
                }
            }
        }

        throw new RepositoryException("Max retries exceeded. Failed to obtain graph instance after " + MAX_RETRY_COUNT + " retries");
    }

    private static Integer getMaxRetryCount() {
        initApplicationProperties();

        return (applicationProperties == null) ? 3 : applicationProperties.getInt(GRAPH_REPOSITORY_MAX_RETRIES, 3);
    }

    private static Long getRetrySleepTime() {
        initApplicationProperties();

        return (applicationProperties == null) ? 30000 : applicationProperties.getLong(GRAPH_REPOSITORY_RETRY_SLEEPTIME, 30000);
    }

    private static void initApplicationProperties() {
        if (applicationProperties == null) {
            try {
                applicationProperties = ApplicationProperties.get();
            } catch (AtlasException ex) {
                // ignore
            }
        }
    }
}

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

package org.apache.atlas.repository.graph;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides access to the AtlasGraph
 *
 */
@Configuration
public class AtlasGraphProvider implements IAtlasGraphProvider {

    private static volatile GraphDatabase<?,?> graphDb_;    

    public static <V, E> AtlasGraph<V, E> getGraphInstance() {
        GraphDatabase<?,?> db = getGraphDatabase();      
        AtlasGraph<?, ?> graph = db.getGraph();
        return (AtlasGraph<V, E>) graph;

    }

    private static <V, E> GraphDatabase<?,?> getGraphDatabase() {

        try {
            if (graphDb_ == null) {
                synchronized(AtlasGraphProvider.class) {
                    if(graphDb_ == null) {
                        Class implClass = AtlasRepositoryConfiguration.getGraphDatabaseImpl();
                        graphDb_ = (GraphDatabase<V, E>) implClass.newInstance();
                    }
                }
            }         
            return graphDb_;
        }
        catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException("Error initializing graph database", e);
        }
    }

    @VisibleForTesting
    public static void cleanup() {
        getGraphDatabase().cleanup();
    }

    @Override
    @Bean
    public AtlasGraph get() throws RepositoryException {
        return getGraphInstance();
    }
}

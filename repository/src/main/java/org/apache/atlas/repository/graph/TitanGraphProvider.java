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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Provides;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.StandardIndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexInformation;
import com.thinkaurelius.titan.diskstorage.solr.Solr5Index;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation for Graph Provider that doles out Titan Graph.
 */
public class TitanGraphProvider implements GraphProvider<TitanGraph> {

    private static final Logger LOG = LoggerFactory.getLogger(TitanGraphProvider.class);

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    public static final String GRAPH_PREFIX = "atlas.graph";

    public static final String INDEX_BACKEND_CONF = "index.search.backend";

    public static final String INDEX_BACKEND_LUCENE = "lucene";

    public static final String INDEX_BACKEND_ES = "elasticsearch";

    private static volatile TitanGraph graphInstance;

    public static Configuration getConfiguration() throws AtlasException {
        Configuration configProperties = ApplicationProperties.get();
        return ApplicationProperties.getSubsetConfiguration(configProperties, GRAPH_PREFIX);
    }

    static {
        addSolr5Index();
    }

    /**
     * Titan loads index backend name to implementation using StandardIndexProvider.ALL_MANAGER_CLASSES
     * But StandardIndexProvider.ALL_MANAGER_CLASSES is a private static final ImmutableMap
     * Only way to inject Solr5Index is to modify this field. So, using hacky reflection to add Sol5Index
     */
    private static void addSolr5Index() {
        try {
            Field field = StandardIndexProvider.class.getDeclaredField("ALL_MANAGER_CLASSES");
            field.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            Map<String, String> customMap = new HashMap(StandardIndexProvider.getAllProviderClasses());
            customMap.put("solr5", Solr5Index.class.getName());
            ImmutableMap<String, String> immap = ImmutableMap.copyOf(customMap);
            field.set(null, immap);

            LOG.debug("Injected solr5 index - {}", Solr5Index.class.getName());
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TitanGraph getGraphInstance() {
        if (graphInstance == null) {
            synchronized (TitanGraphProvider.class) {
                if (graphInstance == null) {
                    Configuration config;
                    try {
                        config = getConfiguration();
                    } catch (AtlasException e) {
                        throw new RuntimeException(e);
                    }

                    graphInstance = TitanFactory.open(config);
                    validateIndexBackend(config);
                }
            }
        }
        return graphInstance;
    }

    public static void clear() {
        synchronized (TitanGraphProvider.class) {
            graphInstance.shutdown();
            graphInstance = null;
        }
    }

    static void validateIndexBackend(Configuration config) {
        String configuredIndexBackend = config.getString(INDEX_BACKEND_CONF);

        TitanManagement managementSystem = graphInstance.getManagementSystem();
        String currentIndexBackend = managementSystem.get(INDEX_BACKEND_CONF);
        managementSystem.commit();
        
        if(!configuredIndexBackend.equals(currentIndexBackend)) {
            throw new RuntimeException("Configured Index Backend " + configuredIndexBackend + " differs from earlier configured Index Backend " + currentIndexBackend + ". Aborting!");
        }

    }

    @Override
    @Singleton
    @Provides
    public TitanGraph get() {
        return getGraphInstance();
    }
}

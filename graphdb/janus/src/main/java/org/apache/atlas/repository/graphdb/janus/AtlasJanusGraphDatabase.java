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

package org.apache.atlas.repository.graphdb.janus;

import com.google.common.collect.ImmutableMap;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.atlas.repository.graphdb.janus.serializer.BigDecimalSerializer;
import org.apache.atlas.repository.graphdb.janus.serializer.BigIntegerSerializer;
import org.apache.atlas.repository.graphdb.janus.serializer.TypeCategorySerializer;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.StandardIndexProvider;
import org.janusgraph.diskstorage.solr.Solr6Index;
import org.janusgraph.graphdb.database.serialize.attribute.SerializableSerializer;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation for Graph Provider that doles out JanusGraph.
 */
public class AtlasJanusGraphDatabase implements GraphDatabase<AtlasJanusVertex, AtlasJanusEdge> {
    private static final Logger LOG      = LoggerFactory.getLogger(AtlasJanusGraphDatabase.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("AtlasJanusGraphDatabase");

    private static final String OLDER_STORAGE_EXCEPTION = "Storage version is incompatible with current client";

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    public static final String GRAPH_PREFIX         = "atlas.graph";
    public static final String INDEX_BACKEND_CONF   = "index.search.backend";
    public static final String SOLR_ZOOKEEPER_URL   = "atlas.graph.index.search.solr.zookeeper-url";
    public static final String SOLR_ZOOKEEPER_URLS  = "atlas.graph.index.search.solr.zookeeper-urls";
    public static final String INDEX_BACKEND_LUCENE = "lucene";
    public static final String INDEX_BACKEND_ES     = "elasticsearch";

    private static volatile AtlasJanusGraph atlasGraphInstance = null;
    private static volatile JanusGraph graphInstance;

    public AtlasJanusGraphDatabase() {
        //update registry
        GraphSONMapper.build().addRegistry(JanusGraphIoRegistry.getInstance()).create();
    }

    public static Configuration getConfiguration() throws AtlasException {
        startLocalSolr();

        Configuration configProperties = ApplicationProperties.get();

        configProperties.setProperty(SOLR_ZOOKEEPER_URLS, configProperties.getStringArray(SOLR_ZOOKEEPER_URL));

        Configuration janusConfig = ApplicationProperties.getSubsetConfiguration(configProperties, GRAPH_PREFIX);

        //add serializers for non-standard property value types that Atlas uses
        janusConfig.addProperty("attributes.custom.attribute1.attribute-class", TypeCategory.class.getName());
        janusConfig.addProperty("attributes.custom.attribute1.serializer-class", TypeCategorySerializer.class.getName());

        //not ideal, but avoids making large changes to Atlas
        janusConfig.addProperty("attributes.custom.attribute2.attribute-class", ArrayList.class.getName());
        janusConfig.addProperty("attributes.custom.attribute2.serializer-class", SerializableSerializer.class.getName());

        janusConfig.addProperty("attributes.custom.attribute3.attribute-class", BigInteger.class.getName());
        janusConfig.addProperty("attributes.custom.attribute3.serializer-class", BigIntegerSerializer.class.getName());

        janusConfig.addProperty("attributes.custom.attribute4.attribute-class", BigDecimal.class.getName());
        janusConfig.addProperty("attributes.custom.attribute4.serializer-class", BigDecimalSerializer.class.getName());

        return janusConfig;
    }

    static {
        addSolr6Index();
    }

    private static void addSolr6Index() {
        try {
            Field field = StandardIndexProvider.class.getDeclaredField("ALL_MANAGER_CLASSES");
            field.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            Map<String, String> customMap = new HashMap<>(StandardIndexProvider.getAllProviderClasses());
            customMap.put("solr", Solr6Index.class.getName());
            ImmutableMap<String, String> immap = ImmutableMap.copyOf(customMap);
            field.set(null, immap);

            LOG.debug("Injected solr6 index - {}", Solr6Index.class.getName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static JanusGraph getGraphInstance() {
        if (graphInstance == null) {
            synchronized (AtlasJanusGraphDatabase.class) {
                if (graphInstance == null) {
                    Configuration config;
                    try {
                        config = getConfiguration();
                    } catch (AtlasException e) {
                        throw new RuntimeException(e);
                    }

                    try {
                        graphInstance = JanusGraphFactory.open(config);
                    } catch (JanusGraphException e) {
                        LOG.warn("JanusGraphException: {}", e.getMessage());
                        if (e.getMessage().startsWith(OLDER_STORAGE_EXCEPTION)) {
                            LOG.info("Newer client is being used with older janus storage version. Setting allow-upgrade=true and reattempting connection");
                            config.addProperty("graph.allow-upgrade", true);
                            graphInstance = JanusGraphFactory.open(config);
                        }
                    }
                    atlasGraphInstance = new AtlasJanusGraph();
                    validateIndexBackend(config);
                }
            }
        }
        return graphInstance;
    }

    public static JanusGraph getBulkLoadingGraphInstance() {
        try {
            Configuration cfg = getConfiguration();
            cfg.setProperty("storage.batch-loading", true);
            return JanusGraphFactory.open(cfg);
        } catch (IllegalArgumentException ex) {
            LOG.error("getBulkLoadingGraphInstance: Failed!", ex);
        } catch (AtlasException ex) {
            LOG.error("getBulkLoadingGraphInstance: Failed!", ex);
        }

        return null;
    }

    public static void unload() {
        synchronized (AtlasJanusGraphDatabase.class) {

            if (graphInstance == null) {
                return;
            }
            graphInstance.tx().commit();
            graphInstance.close();
            graphInstance = null;
        }
    }

    static void validateIndexBackend(Configuration config) {
        String configuredIndexBackend = config.getString(INDEX_BACKEND_CONF);

        JanusGraphManagement managementSystem = getGraphInstance().openManagement();
        String currentIndexBackend = managementSystem.get(INDEX_BACKEND_CONF);
        managementSystem.commit();

        if (!configuredIndexBackend.equals(currentIndexBackend)) {
            throw new RuntimeException("Configured Index Backend " + configuredIndexBackend
                    + " differs from earlier configured Index Backend " + currentIndexBackend + ". Aborting!");
        }

    }

    @Override
    public boolean isGraphLoaded() {
        return graphInstance != null;
    }

    @Override
    public void initializeTestGraph() {
        //nothing to do

    }

    @Override
    public void cleanup() {
        JanusGraph g = getGraphInstance();
        try {
            if(g != null) {
                g.close();
            }
        } catch (Throwable t) {
            LOG.warn("Could not close test JanusGraph", t);
            t.printStackTrace();
        }

        try {
            if(g != null) {
                JanusGraphFactory.drop(g);
            }
        } catch (Throwable t) {
            LOG.warn("Could not clear test JanusGraph", t);
            t.printStackTrace();
        }

        try {
            LocalSolrRunner.stop();
        } catch (Throwable t) {
            LOG.warn("Could not stop local solr server", t);
        }
    }

    @Override
    public AtlasGraph<AtlasJanusVertex, AtlasJanusEdge> getGraph() {
        getGraphInstance();
        return atlasGraphInstance;
    }

    private static void startLocalSolr() {
        if (isEmbeddedSolr()) {
            try {
                LocalSolrRunner.start();

                Configuration configuration = ApplicationProperties.get();
                configuration.clearProperty(SOLR_ZOOKEEPER_URL);
                configuration.setProperty(SOLR_ZOOKEEPER_URL, LocalSolrRunner.getZookeeperUrls());
            } catch (Exception e) {
                throw new RuntimeException("Failed to start embedded solr cloud server. Aborting!", e);
            }
        }
    }

    public static boolean isEmbeddedSolr() {
        boolean ret = false;

        try {
            Configuration conf     = ApplicationProperties.get();
            Object        property = conf.getProperty("atlas.graph.index.search.solr.embedded");

            if (property != null && property instanceof String) {
                ret = Boolean.valueOf((String) property);
            }
        } catch (AtlasException ignored) { }

        return ret;
    }
}

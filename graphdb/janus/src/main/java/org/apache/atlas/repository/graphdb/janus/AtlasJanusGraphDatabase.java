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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.MigrationStatus;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.atlas.repository.graphdb.janus.migration.AtlasGraphSONReader;
import org.apache.atlas.repository.graphdb.janus.migration.ReaderStatusManager;
import org.apache.atlas.repository.graphdb.janus.serializer.BigDecimalSerializer;
import org.apache.atlas.repository.graphdb.janus.serializer.BigIntegerSerializer;
import org.apache.atlas.repository.graphdb.janus.serializer.TypeCategorySerializer;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.janusgraph.graphdb.database.serialize.attribute.SerializableSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

/**
 * Default implementation for Graph Provider that doles out Titan Graph.
 */
public class AtlasJanusGraphDatabase implements GraphDatabase<AtlasJanusVertex, AtlasJanusEdge> {
    private static final Logger LOG      = LoggerFactory.getLogger(AtlasJanusGraphDatabase.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("AtlasJanusGraphDatabase");


    /**
     * Constant for the configuration property that indicates the prefix.
     */
    public static final String GRAPH_PREFIX         = "atlas.graph";
    public static final String INDEX_BACKEND_CONF   = "index.search.backend";
    public static final String SOLR_ZOOKEEPER_URL   = "atlas.graph.index.search.solr.zookeeper-url";
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

        Configuration janusConfig = ApplicationProperties.getSubsetConfiguration(configProperties, GRAPH_PREFIX);

        //add serializers for non-standard property value types that Atlas uses

        janusConfig.addProperty("attributes.custom.attribute1.attribute-class", TypeCategory.class.getName());
        janusConfig.addProperty("attributes.custom.attribute1.serializer-class",
                TypeCategorySerializer.class.getName());

        //not ideal, but avoids making large changes to Atlas
        janusConfig.addProperty("attributes.custom.attribute2.attribute-class", ArrayList.class.getName());
        janusConfig.addProperty("attributes.custom.attribute2.serializer-class", SerializableSerializer.class.getName());

        janusConfig.addProperty("attributes.custom.attribute3.attribute-class", BigInteger.class.getName());
        janusConfig.addProperty("attributes.custom.attribute3.serializer-class", BigIntegerSerializer.class.getName());

        janusConfig.addProperty("attributes.custom.attribute4.attribute-class", BigDecimal.class.getName());
        janusConfig.addProperty("attributes.custom.attribute4.serializer-class", BigDecimalSerializer.class.getName());

        return janusConfig;
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

                    graphInstance = JanusGraphFactory.open(config);
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

    public static void loadLegacyGraphSON(Map<String, String> relationshipCache, InputStream fs) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            LOG.info("Starting loadLegacyGraphSON...");

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "loadLegacyGraphSON");
            }

            AtlasGraphSONReader legacyGraphSONReader = AtlasGraphSONReader.build().
                    relationshipCache(relationshipCache).
                    schemaDB(getGraphInstance()).
                    bulkLoadingDB(getBulkLoadingGraphInstance()).create();

            legacyGraphSONReader.readGraph(fs);
        } catch (Exception ex) {
            LOG.error("Error loading loadLegacyGraphSON2", ex);

            throw new AtlasBaseException(ex);
        } finally {
            AtlasPerfTracer.log(perf);

            LOG.info("Done! loadLegacyGraphSON.");
        }
    }

    public static MigrationStatus getMigrationStatus() {
        return ReaderStatusManager.get(getGraphInstance());
    }
}

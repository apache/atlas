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

package org.apache.atlas.repository.graphdb.janus;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.atlas.repository.graphdb.janus.serializer.BigDecimalSerializer;
import org.apache.atlas.repository.graphdb.janus.serializer.BigIntegerSerializer;
import org.apache.atlas.repository.graphdb.janus.serializer.TypeCategorySerializer;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.StandardIndexProvider;
import org.janusgraph.diskstorage.StandardStoreManager;
import org.janusgraph.diskstorage.es.ElasticSearch7Index;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.diskstorage.rdbms.RdbmsStoreManager;
import org.janusgraph.diskstorage.solr.Solr6Index;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.serialize.attribute.SerializableSerializer;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.atlas.ApplicationProperties.DEFAULT_INDEX_RECOVERY;
import static org.apache.atlas.ApplicationProperties.INDEX_RECOVERY_CONF;

/**
 * Default implementation for Graph Provider that doles out JanusGraph.
 */
public class AtlasJanusGraphDatabase implements GraphDatabase<AtlasJanusVertex, AtlasJanusEdge> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusGraphDatabase.class);

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    public static final String GRAPH_PREFIX              = "atlas.graph";
    public static final String INDEX_BACKEND_CONF        = "index.search.backend";
    public static final String SOLR_ZOOKEEPER_URL        = "atlas.graph.index.search.solr.zookeeper-url";
    public static final String SOLR_ZOOKEEPER_URLS       = "atlas.graph.index.search.solr.zookeeper-urls";
    public static final String INDEX_BACKEND_LUCENE      = "lucene";
    public static final String INDEX_BACKEND_ES          = "elasticsearch";
    public static final String GRAPH_TX_LOG_CONF         = "tx.log-tx";
    public static final String GRAPH_TX_LOG_VERBOSE_CONF = "tx.recovery.verbose";
    public static final String GRAPH_TX_LOG_TTL_CONF     = "log.tx.ttl";

    private static final String OLDER_STORAGE_EXCEPTION = "Storage version is incompatible with current client";

    /**
     * The first management transaction of the process can open before the stored schema has been
     * read. Committing that view writes a second, empty schema, and every later lookup follows it.
     * A restart that can already see {@code vertex_index} commits immediately. One that cannot
     * waits, and still does not commit, for this long.
     */
    private static final int    SCHEMA_VISIBILITY_ATTEMPTS = 30;
    private static final long   SCHEMA_VISIBILITY_WAIT_MS  = 500L;
    private static final String VERTEX_INDEX_NAME          = "vertex_index";
    /** {@code JanusGraphSchemaCategory.GRAPHINDEX.getSchemaName("vertex_index")}. */
    private static final String VERTEX_INDEX_SCHEMA_NAME   = "gi\u001evertex_index";
    private static final int    ATLAS_DATA_PROBE_LIMIT     = 100;
    private static final String TYPEDEF_BOOTSTRAP_KEY_PREFIX = Constants.TYPEDEF_BOOTSTRAP_FILE_KEY.substring(0, Constants.TYPEDEF_BOOTSTRAP_FILE_KEY.lastIndexOf('.') + 1);

    private static volatile AtlasJanusGraph atlasGraphInstance;
    private static volatile JanusGraph      graphInstance;

    public AtlasJanusGraphDatabase() {
        //update registry
        GraphSONMapper.build().addRegistry(JanusGraphIoRegistry.instance()).create();
    }

    public static Configuration getConfiguration() throws AtlasException {
        Configuration configProperties = ApplicationProperties.get();

        if (isEmbeddedSolr()) { // AtlasJanusGraphIndexClient.performRequestHandlerAction() fails for embedded-solr; disable freetext until this issue is resolved
            startEmbeddedSolr();

            configProperties.setProperty(ApplicationProperties.ENABLE_FREETEXT_SEARCH_CONF, false);
        }

        configProperties.setProperty(SOLR_ZOOKEEPER_URLS, configProperties.getStringArray(SOLR_ZOOKEEPER_URL));

        Configuration janusConfig = ApplicationProperties.getSubsetConfiguration(configProperties, GRAPH_PREFIX);

        //add serializers for non-standard property value types that Atlas uses
        janusConfig.setProperty("attributes.custom.attribute1.attribute-class", TypeCategory.class.getName());
        janusConfig.setProperty("attributes.custom.attribute1.serializer-class", TypeCategorySerializer.class.getName());

        //not ideal, but avoids making large changes to Atlas
        janusConfig.setProperty("attributes.custom.attribute2.attribute-class", ArrayList.class.getName());
        janusConfig.setProperty("attributes.custom.attribute2.serializer-class", SerializableSerializer.class.getName());

        janusConfig.setProperty("attributes.custom.attribute3.attribute-class", BigInteger.class.getName());
        janusConfig.setProperty("attributes.custom.attribute3.serializer-class", BigIntegerSerializer.class.getName());

        janusConfig.setProperty("attributes.custom.attribute4.attribute-class", BigDecimal.class.getName());
        janusConfig.setProperty("attributes.custom.attribute4.serializer-class", BigDecimalSerializer.class.getName());

        return janusConfig;
    }

    public static JanusGraph getGraphInstance() {
        JanusGraph me = graphInstance;

        if (me == null) {
            synchronized (AtlasJanusGraphDatabase.class) {
                me = graphInstance;

                if (me == null) {
                    Configuration config;

                    try {
                        config = getConfiguration();
                    } catch (AtlasException e) {
                        throw new RuntimeException(e);
                    }

                    configureTxLogBasedIndexRecovery();

                    graphInstance      = initJanusGraph(config);
                    atlasGraphInstance = new AtlasJanusGraph();

                    me = graphInstance;

                    validateIndexBackend(config);
                }
            }
        }

        return me;
    }

    public static void configureTxLogBasedIndexRecovery() {
        try {
            boolean  recoveryEnabled = ApplicationProperties.get().getBoolean(INDEX_RECOVERY_CONF, DEFAULT_INDEX_RECOVERY);
            long     ttl             = AtlasConfiguration.SOLR_INDEX_TX_LOG_TTL_CONF.getLong();
            Duration txLogTtlSecs    = Duration.ofSeconds(Duration.ofHours(ttl).getSeconds());

            Map<String, Object> properties = new HashMap<>();

            properties.put(GRAPH_TX_LOG_CONF, recoveryEnabled);
            properties.put(GRAPH_TX_LOG_VERBOSE_CONF, recoveryEnabled);
            properties.put(GRAPH_TX_LOG_TTL_CONF, txLogTtlSecs);

            updateGlobalConfiguration(properties);

            LOG.info("Tx Log-based Index Recovery: {}!", recoveryEnabled ? "Enabled" : "Disabled");
        } catch (Exception e) {
            LOG.error("Error: Failed!", e);
        }
    }

    public static JanusGraph getBulkLoadingGraphInstance() {
        try {
            Configuration cfg = getConfiguration();

            cfg.setProperty("storage.batch-loading", true);

            org.apache.commons.configuration2.Configuration conf2 = createConfiguration2(cfg);

            return JanusGraphFactory.open(conf2);
        } catch (IllegalArgumentException | AtlasException ex) {
            LOG.error("getBulkLoadingGraphInstance: Failed!", ex);
        }

        return null;
    }

    public static void unload() {
        synchronized (AtlasJanusGraphDatabase.class) {
            JanusGraph me = graphInstance;

            if (me != null) {
                me.tx().commit();
                me.close();

                graphInstance = null;
            }
        }
    }

    public static boolean isEmbeddedSolr() {
        boolean ret = false;

        try {
            Configuration conf     = ApplicationProperties.get();
            Object        property = conf.getProperty("atlas.graph.index.search.solr.embedded");

            if (property instanceof String) {
                ret = Boolean.parseBoolean((String) property);
            }
        } catch (AtlasException ignored) {
            // ignore
        }

        return ret;
    }

    @VisibleForTesting
    static JanusGraph initJanusGraph(Configuration config) {
        org.apache.commons.configuration2.Configuration conf2 = createConfiguration2(config);

        try {
            return JanusGraphFactory.open(conf2);
        } catch (JanusGraphException e) {
            LOG.warn("JanusGraphException: {}", e.getMessage());

            if (e.getMessage().startsWith(OLDER_STORAGE_EXCEPTION)) {
                LOG.info("Newer client is being used with older janus storage version. Setting allow-upgrade=true and reattempting connection");

                config.addProperty("graph.allow-upgrade", true);

                return JanusGraphFactory.open(conf2);
            } else {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            LOG.error("Error: Error initializing graph");
            throw e;
        }
    }

    static void validateIndexBackend(Configuration config) {
        JanusGraphManagement managementSystem = null;
        boolean               vertexIndexVisible = false;

        try {
            managementSystem = getGraphInstance().openManagement();
            vertexIndexVisible = managementSystem.getGraphIndex(VERTEX_INDEX_NAME) != null;

            // This read used to commit unconditionally. Committing a management view that
            // does not contain vertex_index replaces the stored schema, so a hidden index
            // is rolled back. The backend comparison runs only once the index is visible.
            if (vertexIndexVisible) {
                String configuredIndexBackend = config.getString(INDEX_BACKEND_CONF);
                String currentIndexBackend    = managementSystem.get(INDEX_BACKEND_CONF);

                if (!configuredIndexBackend.equals(currentIndexBackend)) {
                    throw new RuntimeException("Configured Index Backend " + configuredIndexBackend + " differs from earlier configured Index Backend " + currentIndexBackend + ". Aborting!");
                }
            } else {
                LOG.warn("vertex_index is not visible while checking the index backend. Not committing.");
            }
        } finally {
            if (managementSystem != null && managementSystem.isOpen()) {
                if (vertexIndexVisible) {
                    managementSystem.commit();
                } else {
                    managementSystem.rollback();
                }
            }
        }
    }

    @Override
    public boolean isGraphLoaded() {
        return graphInstance != null;
    }

    @Override
    public AtlasGraph<AtlasJanusVertex, AtlasJanusEdge> getGraph() {
        getGraphInstance();

        return atlasGraphInstance;
    }

    @Override
    public AtlasGraph<AtlasJanusVertex, AtlasJanusEdge> getGraphBulkLoading() {
        return new AtlasJanusGraph(getBulkLoadingGraphInstance());
    }

    @Override
    public void initializeTestGraph() {
        //nothing to do
    }

    @Override
    public void cleanup() {
        JanusGraph g = getGraphInstance();

        try {
            if (g != null) {
                g.close();
            }
        } catch (Throwable t) {
            LOG.warn("Could not close test JanusGraph", t);

            t.printStackTrace();
        }

        try {
            if (g != null) {
                JanusGraphFactory.drop(g);
            }
        } catch (Throwable t) {
            LOG.warn("Could not clear test JanusGraph", t);

            t.printStackTrace();
        }

        if (isEmbeddedSolr()) {
            try {
                stopEmbeddedSolr();
            } catch (Throwable t) {
                LOG.warn("Could not stop local solr server", t);
            }
        }
    }

    private static void addHBase2Support() {
        logArgs();
        try {
            Field field = StandardStoreManager.class.getDeclaredField("ALL_MANAGER_CLASSES");

            field.setAccessible(true);

            Method getDeclaredFields0 = Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class);
            getDeclaredFields0.setAccessible(true);
            Field[] fields = (Field[]) getDeclaredFields0.invoke(Field.class, false);
            Field modifiersField = null;
            for (Field each : fields) {
                if ("modifiers".equals(each.getName())) {
                    modifiersField = each;
                    break;
                }
            }

            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            Map<String, String> customMap = new HashMap<>(StandardStoreManager.getAllManagerClasses());

            customMap.put("hbase2", HBaseStoreManager.class.getName());

            ImmutableMap<String, String> immap = ImmutableMap.copyOf(customMap);

            field.set(null, immap);

            LOG.debug("Injected HBase2 support - {}", HBaseStoreManager.class.getName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void addRdbmsSupport() {
        try {
            Field field = StandardStoreManager.class.getDeclaredField("ALL_MANAGER_CLASSES");

            field.setAccessible(true);

            Method getDeclaredFields0 = Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class);
            getDeclaredFields0.setAccessible(true);
            Field[] fields = (Field[]) getDeclaredFields0.invoke(Field.class, false);
            Field modifiersField = null;
            for (Field each : fields) {
                if ("modifiers".equals(each.getName())) {
                    modifiersField = each;
                    break;
                }
            }

            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            Map<String, String> customMap = new HashMap<>(StandardStoreManager.getAllManagerClasses());

            customMap.put("rdbms", RdbmsStoreManager.class.getName());

            ImmutableMap<String, String> immap = ImmutableMap.copyOf(customMap);

            field.set(null, immap);

            LOG.debug("Injected RDBMS support - {}", RdbmsStoreManager.class.getName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void addSolr6Index() {
        try {
            Field field = StandardIndexProvider.class.getDeclaredField("ALL_MANAGER_CLASSES");

            field.setAccessible(true);

            Method getDeclaredFields0 = Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class);
            getDeclaredFields0.setAccessible(true);
            Field[] fields = (Field[]) getDeclaredFields0.invoke(Field.class, false);
            Field modifiersField = null;
            for (Field each : fields) {
                if ("modifiers".equals(each.getName())) {
                    modifiersField = each;
                    break;
                }
            }

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

    private static void addElasticSearch7Index() {
        try {
            Field field = StandardIndexProvider.class.getDeclaredField("ALL_MANAGER_CLASSES");

            field.setAccessible(true);

            Method getDeclaredFields0 = Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class);
            getDeclaredFields0.setAccessible(true);
            Field[] fields = (Field[]) getDeclaredFields0.invoke(Field.class, false);
            Field modifiersField = null;
            for (Field each : fields) {
                if ("modifiers".equals(each.getName())) {
                    modifiersField = each;
                    break;
                }
            }

            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            Map<String, String> customMap = new HashMap<>(StandardIndexProvider.getAllProviderClasses());

            customMap.put("elasticsearch", ElasticSearch7Index.class.getName());

            ImmutableMap<String, String> immap = ImmutableMap.copyOf(customMap);

            field.set(null, immap);

            LOG.debug("Injected es7 index - {}", ElasticSearch7Index.class.getName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void updateGlobalConfiguration(Map<String, Object> map) {
        for (int attempt = 1; attempt <= SCHEMA_VISIBILITY_ATTEMPTS; attempt++) {
            JanusGraph           graph            = null;
            JanusGraphManagement managementSystem = null;

            try {
                graph = initJanusGraph(getConfiguration());

                // Fill the schema cache from storage before opening management. A management
                // transaction opened against an empty cache, then committed, is what hides the
                // stored indexes.
                boolean storedIndex = storedVertexIndexExists(graph);
                // The probe scans vertices without an index; only a graph with no stored index needs it.
                boolean hasAtlasData = storedIndex || graphHasAtlasData(graph);

                managementSystem = graph.openManagement();

                if (managementSystem.getGraphIndex(VERTEX_INDEX_NAME) != null) {
                    applyGlobalConfiguration(managementSystem, map);
                    managementSystem = null;

                    LOG.info("Global properties updated!: {}", map);

                    return;
                }

                managementSystem.rollback();
                managementSystem = null;

                // Commit an empty schema only after a second read also finds no index and no
                // Atlas data. Claim and typedef-bootstrap records do not count: they exist on a
                // new graph, including one that was just emptied.
                if (!storedIndex && !hasAtlasData && attempt > 1) {
                    managementSystem = graph.openManagement();
                    applyGlobalConfiguration(managementSystem, map);
                    managementSystem = null;

                    LOG.info("Global properties updated on a new graph!: {}", map);

                    return;
                }

                if (!storedIndex && !hasAtlasData) {
                    LOG.info("No stored vertex_index and no Atlas data yet (attempt {}/{}). Confirming this is a new graph before committing.",
                            attempt, SCHEMA_VISIBILITY_ATTEMPTS);
                } else {
                    LOG.warn("vertex_index is not visible yet (attempt {}/{}, storedIndex={}, hasAtlasData={}). Not committing global configuration.",
                            attempt, SCHEMA_VISIBILITY_ATTEMPTS, storedIndex, hasAtlasData);
                }
            } catch (Exception ex) {
                LOG.warn("Could not update global configuration (attempt {}/{})", attempt, SCHEMA_VISIBILITY_ATTEMPTS, ex);
            } finally {
                if (managementSystem != null && managementSystem.isOpen()) {
                    managementSystem.rollback();
                }

                if (graph != null) {
                    graph.close();
                }
            }

            if (attempt == SCHEMA_VISIBILITY_ATTEMPTS) {
                LOG.error("vertex_index stayed invisible. Leaving global configuration unchanged so the stored schema is kept.");

                return;
            }

            try {
                Thread.sleep(SCHEMA_VISIBILITY_WAIT_MS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();

                return;
            }
        }
    }

    private static void applyGlobalConfiguration(JanusGraphManagement managementSystem, Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            managementSystem.set(entry.getKey(), entry.getValue());
        }

        managementSystem.commit();
    }

    /**
     * True when storage already has {@code vertex_index}. A failed read is treated as present so
     * this method cannot be the reason an empty schema is committed.
     */
    private static boolean storedVertexIndexExists(JanusGraph graph) {
        if (!(graph instanceof StandardJanusGraph)) {
            return false;
        }

        try {
            Long schemaId = ((StandardJanusGraph) graph).getSchemaCache().getSchemaId(VERTEX_INDEX_SCHEMA_NAME);

            return schemaId != null;
        } catch (RuntimeException exception) {
            LOG.warn("Could not read the stored vertex index; not committing a management transaction", exception);

            return true;
        }
    }

    /**
     * True when the store holds a vertex other than an Atlas claim or typedef-bootstrap record. Those
     * are written before indexes exist, so a new or just-emptied graph has them. A failed probe, or a
     * full page of coordination records, is reported as data present.
     */
    private static boolean graphHasAtlasData(JanusGraph graph) {
        try {
            int scanned = 0;

            for (Iterator<Vertex> it = graph.traversal().V().limit(ATLAS_DATA_PROBE_LIMIT); it.hasNext(); ) {
                Vertex vertex = it.next();

                scanned++;

                if (!isCoordinationVertex(vertex)) {
                    graph.tx().rollback();

                    return true;
                }
            }

            graph.tx().rollback();

            return scanned >= ATLAS_DATA_PROBE_LIMIT;
        } catch (Exception exception) {
            LOG.warn("Could not tell whether the graph already has Atlas data; treating it as populated", exception);

            try {
                graph.tx().rollback();
            } catch (Exception rollbackFailure) {
                LOG.debug("Rollback after the data probe failed", rollbackFailure);
            }

            return true;
        }
    }

    private static boolean isCoordinationVertex(Vertex vertex) {
        Set<String> keys = vertex.keys();

        if (keys == null || keys.isEmpty()) {
            return false;
        }

        for (String key : keys) {
            if (!key.startsWith(Constants.CLAIM_KEY) && !key.startsWith(TYPEDEF_BOOTSTRAP_KEY_PREFIX)) {
                return false;
            }
        }

        return true;
    }

    private static org.apache.commons.configuration2.Configuration createConfiguration2(Configuration conf) {
        Properties properties = ConfigurationConverter.getProperties(conf);

        return org.apache.commons.configuration2.ConfigurationConverter.getConfiguration(properties);
    }

    private static void startEmbeddedSolr() throws AtlasException {
        LOG.info("==> startEmbeddedSolr()");

        try {
            Class<?> localSolrRunnerClz = Class.forName("org.apache.atlas.runner.LocalSolrRunner");
            Method   startMethod        = localSolrRunnerClz.getMethod("start");

            startMethod.invoke(null);
        } catch (Exception excp) {
            LOG.error("startEmbeddedSolr(): failed", excp);

            throw new AtlasException("startEmbeddedSolr(): failed", excp);
        }

        LOG.info("<== startEmbeddedSolr()");
    }

    private static void stopEmbeddedSolr() throws AtlasException {
        LOG.info("==> stopEmbeddedSolr()");

        try {
            Class<?> localSolrRunnerClz = Class.forName("org.apache.atlas.runner.LocalSolrRunner");
            Method   stopMethod         = localSolrRunnerClz.getMethod("stop");

            stopMethod.invoke(null);
        } catch (Exception excp) {
            LOG.error("stopEmbeddedSolr(): failed", excp);

            throw new AtlasException("stopEmbeddedSolr(): failed", excp);
        }

        LOG.info("<== stopEmbeddedSolr()");
    }

    static {
        addHBase2Support();
        addRdbmsSupport();

        addSolr6Index();

        addElasticSearch7Index();
    }

    public static void logArgs() {
        List<String> jvmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        System.out.println("JVM Args:");
        jvmArgs.forEach(System.out::println);
    }
}

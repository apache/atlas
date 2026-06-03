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
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.atlas.repository.graphdb.janus.serializer.BigDecimalSerializer;
import org.apache.atlas.repository.graphdb.janus.serializer.BigIntegerSerializer;
import org.apache.atlas.repository.graphdb.janus.serializer.TypeCategorySerializer;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.StandardIndexProvider;
import org.janusgraph.diskstorage.StandardStoreManager;
import org.janusgraph.graphdb.database.serialize.attribute.SerializableSerializer;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
    /** Full key in {@code atlas-application.properties}; must be coerced before {@link ApplicationProperties#getSubsetConfiguration} when value is a list/array. */
    private static final String ATLAS_GRAPH_STORAGE_HOSTNAME = GRAPH_PREFIX + ".storage.hostname";
    public static final String INDEX_BACKEND_CONF        = "index.search.backend";
    public static final String SOLR_ZOOKEEPER_URL        = "atlas.graph.index.search.solr.zookeeper-url";
    public static final String SOLR_ZOOKEEPER_URLS       = "atlas.graph.index.search.solr.zookeeper-urls";
    public static final String INDEX_BACKEND_LUCENE      = "lucene";
    public static final String INDEX_BACKEND_ES          = "elasticsearch";
    public static final String BUILD_STORAGE_BACKEND_CONF = "atlas.build.storage.backend";
    public static final String BUILD_INDEX_BACKEND_CONF   = "atlas.build.index.backend";
    public static final String GRAPH_TX_LOG_CONF         = "tx.log-tx";
    public static final String GRAPH_TX_LOG_VERBOSE_CONF = "tx.recovery.verbose";
    public static final String GRAPH_TX_LOG_TTL_CONF     = "log.tx.ttl";

    private static final String OLDER_STORAGE_EXCEPTION = "Storage version is incompatible with current client";

    /** Optional Janus storage / index implementation classes (may be omitted in slim builds). */
    private static final String HBASE2_STORE_MANAGER_CLASS = "org.janusgraph.diskstorage.hbase.HBaseStoreManager";
    private static final String CQL_STORE_MANAGER_CLASS    = "org.janusgraph.diskstorage.cql.CQLStoreManager";
    private static final String RDBMS_STORE_MANAGER_CLASS  = "org.janusgraph.diskstorage.rdbms.RdbmsStoreManager";
    private static final String SOLR6_INDEX_CLASS          = "org.janusgraph.diskstorage.solr.Solr6Index";
    private static final String ELASTICSEARCH7_INDEX_CLASS   = "org.janusgraph.diskstorage.es.ElasticSearch7Index";

    private static volatile AtlasJanusGraph atlasGraphInstance;
    private static volatile JanusGraph      graphInstance;

    public AtlasJanusGraphDatabase() {
        //update registry
        GraphSONMapper.build().addRegistry(JanusGraphIoRegistry.instance()).create();
    }

    public static Configuration getConfiguration() throws AtlasException {
        Configuration configProperties = ApplicationProperties.get();

        // Duplicate keys / Spring list binding can leave String[] on the parent; subset() would still expose storage.hostname as an array.
        coerceSingleJanusString(configProperties, ATLAS_GRAPH_STORAGE_HOSTNAME);

        if (isEmbeddedSolr()) { // AtlasJanusGraphIndexClient.performRequestHandlerAction() fails for embedded-solr; disable freetext until this issue is resolved
            startEmbeddedSolr();

            configProperties.setProperty(ApplicationProperties.ENABLE_FREETEXT_SEARCH_CONF, false);
        }

        configProperties.setProperty(SOLR_ZOOKEEPER_URLS, configProperties.getStringArray(SOLR_ZOOKEEPER_URL));

        Configuration janusConfig = ApplicationProperties.getSubsetConfiguration(configProperties, GRAPH_PREFIX);

        normalizeStringArrayValuesForJanus(janusConfig);
        coerceCriticalJanusStringOptions(janusConfig);

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

                    validateBuildBackendAlignment(config);

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

                conf2 = createConfiguration2(config);

                return JanusGraphFactory.open(conf2);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static void validateBuildBackendAlignment(Configuration janusConfig) {
        try {
            Configuration appConfig = ApplicationProperties.get();
            String        buildStorage = appConfig.getString(BUILD_STORAGE_BACKEND_CONF, null);
            String        buildIndex   = appConfig.getString(BUILD_INDEX_BACKEND_CONF, null);

            if (buildStorage == null && buildIndex == null) {
                return;
            }

            String runtimeStorage = appConfig.getString(ApplicationProperties.STORAGE_BACKEND_CONF, "");
            String runtimeIndex   = janusConfig.getString(INDEX_BACKEND_CONF, "");

            if (buildStorage != null) {
                validateStorageBackendClasspath(buildStorage);

                if (!isStorageBackendCompatible(buildStorage, runtimeStorage)) {
                    throw new RuntimeException(String.format(
                            "Atlas was built for storage backend '%s' (Maven -Pstorage-*) but %s is '%s'. "
                                    + "Rebuild with the matching storage profile or fix atlas-application.properties. "
                                    + "This distribution cannot switch storage backends at runtime.",
                            buildStorage, ApplicationProperties.STORAGE_BACKEND_CONF, runtimeStorage));
                }
            }

            if (buildIndex != null && StringUtils.isNotBlank(runtimeIndex) && !isIndexBackendCompatible(buildIndex, runtimeIndex)) {
                throw new RuntimeException(String.format(
                        "Atlas was built for index backend '%s' (Maven -Pindex-*) but %s is '%s'. "
                                + "Rebuild with the matching index profile or fix atlas-application.properties.",
                        buildIndex, INDEX_BACKEND_CONF, runtimeIndex));
            }
        } catch (AtlasException e) {
            throw new RuntimeException("Failed to validate build-time backend alignment", e);
        }
    }

    private static void validateStorageBackendClasspath(String buildStorage) {
        switch (buildStorage.toLowerCase()) {
            case "hbase":
                if (!isClassLoadable(HBASE2_STORE_MANAGER_CLASS)
                        && !isClasspathResourcePresent("org/janusgraph/diskstorage/hbase/HBaseStoreManager.class")) {
                    throw new RuntimeException(
                            "Atlas was built for HBase storage but janusgraph-hbase is not on the classpath. "
                                    + "Rebuild with -Pdist or -Pstorage-hbase.");
                }
                break;
            case "cassandra":
                if (!isClassLoadable(CQL_STORE_MANAGER_CLASS)) {
                    throw new RuntimeException(
                            "Atlas was built for Cassandra storage but janusgraph-cql is not on the classpath. "
                                    + "Rebuild with -Pstorage-cassandra.");
                }
                break;
            case "rdbms":
                if (!isClassLoadable(RDBMS_STORE_MANAGER_CLASS)) {
                    throw new RuntimeException(
                            "Atlas was built for RDBMS storage but janusgraph-rdbms is not on the classpath. "
                                    + "Rebuild with -Pstorage-rdbms.");
                }
                break;
            case "berkeleyje":
                if (!isClassLoadable("org.janusgraph.diskstorage.berkeleyje.BerkeleyJEStoreManager")) {
                    throw new RuntimeException(
                            "Atlas was built for BerkeleyJE storage but janusgraph-berkeleyje is not on the classpath. "
                                    + "Rebuild with -Pstorage-berkeleyje.");
                }
                break;
            default:
                break;
        }
    }

    @VisibleForTesting
    static boolean isStorageBackendCompatible(String buildBackend, String runtimeJanusName) {
        if (buildBackend == null || runtimeJanusName == null) {
            return false;
        }

        String build = buildBackend.toLowerCase();
        String runtime = runtimeJanusName.toLowerCase();

        switch (build) {
            case "hbase":
                return "hbase2".equals(runtime) || "hbase".equals(runtime);
            case "cassandra":
                return "cql".equals(runtime) || "embeddedcassandra".equals(runtime) || "cassandra".equals(runtime);
            case "rdbms":
                return "rdbms".equals(runtime);
            case "berkeleyje":
                return "berkeleyje".equals(runtime);
            default:
                return build.equals(runtime);
        }
    }

    @VisibleForTesting
    static boolean isIndexBackendCompatible(String buildIndex, String runtimeIndex) {
        return buildIndex != null && runtimeIndex != null && buildIndex.equalsIgnoreCase(runtimeIndex);
    }

    static void validateIndexBackend(Configuration config) {
        JanusGraphManagement managementSystem = null;

        try {
            managementSystem = getGraphInstance().openManagement();

            String configuredIndexBackend = config.getString(INDEX_BACKEND_CONF);
            String currentIndexBackend    = managementSystem.get(INDEX_BACKEND_CONF);

            if (!configuredIndexBackend.equals(currentIndexBackend)) {
                throw new RuntimeException("Configured Index Backend " + configuredIndexBackend + " differs from earlier configured Index Backend " + currentIndexBackend + ". Aborting!");
            }
        } finally {
            if (managementSystem != null) {
                managementSystem.commit();
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

    /**
     * Register shortName -> FQCN in Janus {@code StandardStoreManager} without loading the class first.
     * Pre-loading with {@link Class#forName(String)} caused missing map entries when the class was not yet
     * visible to the caller's loader at static init time, leading Janus to treat {@code hbase2} as a class name.
     * Implementation classes are loaded when the graph opens.
     */
    private static void safeRegisterStoreManager(String shortName, String managerClassName) {
        try {
            injectStoreManager(shortName, managerClassName);
        } catch (Throwable t) {
            LOG.warn("Failed to register Janus storage backend '{}' -> {}", shortName, managerClassName, t);
        }
    }

    private static void injectStoreManager(String shortName, String managerClassName) throws Exception {
        Field field = StandardStoreManager.class.getDeclaredField("ALL_MANAGER_CLASSES");

        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");

        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        Map<String, String> customMap = new HashMap<>(StandardStoreManager.getAllManagerClasses());

        customMap.put(shortName, managerClassName);

        field.set(null, ImmutableMap.copyOf(customMap));

        LOG.debug("Registered Janus storage backend {} -> {}", shortName, managerClassName);
    }

    private static void safeRegisterIndexProvider(String shortName, String providerClassName) {
        try {
            injectIndexProvider(shortName, providerClassName);
        } catch (Throwable t) {
            LOG.warn("Failed to register Janus index provider '{}' -> {}", shortName, providerClassName, t);
        }
    }

    private static void injectIndexProvider(String shortName, String providerClassName) throws Exception {
        Field field = StandardIndexProvider.class.getDeclaredField("ALL_MANAGER_CLASSES");

        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");

        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        Map<String, String> customMap = new HashMap<>(StandardIndexProvider.getAllProviderClasses());

        customMap.put(shortName, providerClassName);

        field.set(null, ImmutableMap.copyOf(customMap));

        LOG.debug("Registered Janus index provider {} -> {}", shortName, providerClassName);
    }

    private static void registerJanusOptionalBackends() {
        registerStoreIfLoadable("hbase2", HBASE2_STORE_MANAGER_CLASS);
        registerStoreIfLoadable("cql", CQL_STORE_MANAGER_CLASS);
        registerStoreIfLoadable("rdbms", RDBMS_STORE_MANAGER_CLASS);
        registerIndexIfLoadable("solr", SOLR6_INDEX_CLASS);
        registerIndexIfLoadable("elasticsearch", ELASTICSEARCH7_INDEX_CLASS);
    }

    private static void registerStoreIfLoadable(String shortName, String managerClassName) {
        if (!isClassLoadable(managerClassName)) {
            LOG.debug("Skipping Janus storage backend {}: {} not on classpath", shortName, managerClassName);

            return;
        }

        safeRegisterStoreManager(shortName, managerClassName);
    }

    private static void registerIndexIfLoadable(String shortName, String providerClassName) {
        if (!isClassLoadable(providerClassName)) {
            LOG.debug("Skipping Janus index provider {}: {} not on classpath", shortName, providerClassName);

            return;
        }

        safeRegisterIndexProvider(shortName, providerClassName);
    }

    private static boolean isClassLoadable(String className) {
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader atlasLoader   = AtlasJanusGraphDatabase.class.getClassLoader();

        if (contextLoader != null && isClassLoadable(className, contextLoader)) {
            return true;
        }

        if (atlasLoader != null && isClassLoadable(className, atlasLoader)) {
            return true;
        }

        return isClassLoadable(className, ClassLoader.getSystemClassLoader());
    }

    private static boolean isClassLoadable(String className, ClassLoader loader) {
        if (loader == null) {
            return false;
        }

        try {
            Class.forName(className, false, loader);

            return true;
        } catch (ClassNotFoundException e) {
            return false;
        } catch (LinkageError e) {
            LOG.debug("Class {} not loadable with loader {}: {}", className, loader, e.toString());

            return false;
        }
    }

    private static boolean isClasspathResourcePresent(String resourcePath) {
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader atlasLoader   = AtlasJanusGraphDatabase.class.getClassLoader();

        return (contextLoader != null && contextLoader.getResource(resourcePath) != null)
                || (atlasLoader != null && atlasLoader.getResource(resourcePath) != null);
    }

    private static void updateGlobalConfiguration(Map<String, Object> map) {
        JanusGraph           graph            = null;
        JanusGraphManagement managementSystem = null;

        try {
            graph            = initJanusGraph(getConfiguration());
            managementSystem = graph.openManagement();

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                managementSystem.set(entry.getKey(), entry.getValue());
            }

            LOG.info("Global properties updated!: {}", map);
        } catch (Exception ex) {
            LOG.error("Error updating global configuration: {}", map, ex);
        } finally {
            if (managementSystem != null) {
                managementSystem.commit();
            }

            if (graph != null) {
                graph.close();
            }
        }
    }

    /**
     * Spring/YAML list properties and {@code setProperty(key, String[])} (e.g. Solr ZK URLs) become {@code String[]}.
     * JanusGraph {@code ConfigOption} values typed as STRING reject arrays and fail with
     * {@code Invalid configuration value for [root.storage.hostname]: [Ljava.lang.String;@...}.
     */
    private static void normalizeStringArrayValuesForJanus(Configuration janusConfig) {
        ArrayList<String> keys = new ArrayList<>();

        for (Iterator<String> it = janusConfig.getKeys(); it.hasNext(); ) {
            keys.add(it.next());
        }

        for (String key : keys) {
            Object val = janusConfig.getProperty(key);

            if (val instanceof String[]) {
                String[] arr = (String[]) val;

                if (arr.length == 0) {
                    janusConfig.clearProperty(key);
                } else {
                    janusConfig.setProperty(key, String.join(",", arr));
                }
            } else if (val instanceof List) {
                List<?> list = (List<?>) val;

                if (list.isEmpty()) {
                    janusConfig.clearProperty(key);
                } else {
                    janusConfig.setProperty(key, joinCommaList(list));
                }
            }
        }
    }

    /**
     * {@link Configuration#getKeys()} on subset configs can omit keys that are only
     * present as multi-valued / list types, so {@link #normalizeStringArrayValuesForJanus} may skip them.
     * Janus {@code storage.hostname} is a STRING for HBase and CQL ({@link org.janusgraph.diskstorage.common.DistributedStoreManager}).
     */
    private static void coerceCriticalJanusStringOptions(Configuration janusConfig) {
        coerceSingleJanusString(janusConfig, "storage.hostname");
    }

    private static void coerceSingleJanusString(Configuration cfg, String key) {
        Object val = cfg.getProperty(key);

        if (val == null || val instanceof String) {
            return;
        }

        if (val instanceof String[]) {
            String[] arr = (String[]) val;

            cfg.setProperty(key, arr.length == 0 ? "" : String.join(",", arr));
        } else if (val instanceof List) {
            cfg.setProperty(key, joinCommaList((List<?>) val));
        } else {
            cfg.setProperty(key, val.toString());
        }
    }

    private static String joinCommaList(List<?> list) {
        StringBuilder sb = new StringBuilder();

        for (Object o : list) {
            if (sb.length() > 0) {
                sb.append(',');
            }

            sb.append(o != null ? o.toString() : "");
        }

        return sb.toString();
    }

    /**
     * {@link ConfigurationConverter#getProperties} can leave {@code String[]} / {@link List} values inside
     * {@link Properties}; {@link org.apache.commons.configuration2.ConfigurationConverter#getConfiguration(Properties)}
     * then passes those through to JanusGraph unchanged.
     */
    private static void sanitizePropertiesForJanus(Properties properties) {
        synchronized (properties) {
            for (Enumeration<?> e = properties.keys(); e.hasMoreElements(); ) {
                String key = String.valueOf(e.nextElement());
                Object val = properties.get(key);

                if (val instanceof String[]) {
                    String[] arr = (String[]) val;

                    properties.put(key, arr.length == 0 ? "" : String.join(",", arr));
                } else if (val instanceof List) {
                    properties.put(key, joinCommaList((List<?>) val));
                }
            }

            // Hashtable-backed Properties may still omit non-String values from keys(); force known Janus STRING keys.
            Object hostnameVal = properties.get("storage.hostname");

            if (hostnameVal instanceof String[]) {
                String[] arr = (String[]) hostnameVal;

                properties.put("storage.hostname", arr.length == 0 ? "" : String.join(",", arr));
            } else if (hostnameVal instanceof List) {
                properties.put("storage.hostname", joinCommaList((List<?>) hostnameVal));
            }
        }
    }

    private static org.apache.commons.configuration2.Configuration createConfiguration2(Configuration conf) {
        normalizeStringArrayValuesForJanus(conf);
        coerceCriticalJanusStringOptions(conf);

        Properties properties = ConfigurationConverter.getProperties(conf);

        sanitizePropertiesForJanus(properties);

        org.apache.commons.configuration2.Configuration cfg2 =
                org.apache.commons.configuration2.ConfigurationConverter.getConfiguration(properties);

        // getConfiguration(Properties) can still surface String[] for keys Janus expects as STRING (e.g. storage.hostname).
        coerceStringArraysInConfiguration2(cfg2);

        return cfg2;
    }

    /**
     * Final pass: Janus {@code storage.hostname} (and similar STRING options) must not be {@code String[]} after
     * {@link org.apache.commons.configuration2.ConfigurationConverter#getConfiguration(Properties)}.
     */
    private static void coerceStringArraysInConfiguration2(org.apache.commons.configuration2.Configuration cfg) {
        if (cfg == null) {
            return;
        }

        ArrayList<String> keys = new ArrayList<>();

        for (Iterator<String> it = cfg.getKeys(); it.hasNext(); ) {
            keys.add(it.next());
        }

        for (String key : keys) {
            Object val = cfg.getProperty(key);

            if (val instanceof String[]) {
                String[] arr = (String[]) val;

                cfg.setProperty(key, arr.length == 0 ? "" : String.join(",", arr));
            } else if (val instanceof List) {
                List<?> list = (List<?>) val;

                cfg.setProperty(key, list.isEmpty() ? "" : joinCommaList(list));
            }
        }
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
        registerJanusOptionalBackends();
    }
}

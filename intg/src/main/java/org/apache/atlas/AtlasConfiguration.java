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

package org.apache.atlas;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

/**
 * Enum that encapsulated each property name and its default value.
 */
public enum AtlasConfiguration {
    //web server configuration
    WEBSERVER_MIN_THREADS("atlas.webserver.minthreads", 10),
    WEBSERVER_MAX_THREADS("atlas.webserver.maxthreads", 100),
    WEBSERVER_RESERVED_THREADS("atlas.webserver.reservedthreads", 40),
    WEBSERVER_KEEPALIVE_SECONDS("atlas.webserver.keepalivetimesecs", 60),
    WEBSERVER_QUEUE_SIZE("atlas.webserver.queuesize", 100),
    WEBSERVER_REQUEST_BUFFER_SIZE("atlas.jetty.request.buffer.size", 16192),

    QUERY_PARAM_MAX_LENGTH("atlas.query.param.max.length", 4*1024),

    REST_API_ENABLE_DELETE_TYPE_OVERRIDE("atlas.rest.enable.delete.type.override", false),
    NOTIFICATION_RELATIONSHIPS_ENABLED("atlas.notification.relationships.enabled", true),

    NOTIFICATION_HOOK_TOPIC_NAME("atlas.notification.hook.topic.name", "ATLAS_HOOK"),
    NOTIFICATION_ENTITIES_TOPIC_NAME("atlas.notification.entities.topic.name", "ATLAS_ENTITIES"),
    NOTIFICATION_RELATIONSHIPS_TOPIC_NAME("atlas.notification.relationships.topic.name", "ATLAS_RELATIONSHIPS"),
    NOTIFICATION_ATLAS_DISTRIBUTED_TASKS_TOPIC_NAME("atlas.notification.distributed.tasks.topic.name", "ATLAS_DISTRIBUTED_TASKS"),

    NOTIFICATION_HOOK_CONSUMER_TOPIC_NAMES("atlas.notification.hook.consumer.topic.names", "ATLAS_HOOK"), //  a comma separated list of topic names
    NOTIFICATION_ENTITIES_CONSUMER_TOPIC_NAMES("atlas.notification.entities.consumer.topic.names", "ATLAS_ENTITIES"), //  a comma separated list of topic names
    NOTIFICATION_RELATIONSHIPS_CONSUMER_TOPIC_NAMES("atlas.notification.relationships.consumer.topic.names", "ATLAS_RELATIONSHIPS"), //  a comma separated list of topic names

    NOTIFICATION_MESSAGE_MAX_LENGTH_BYTES("atlas.notification.message.max.length.bytes", (1000 * 1000)),
    NOTIFICATION_MESSAGE_COMPRESSION_ENABLED("atlas.notification.message.compression.enabled", true),
    NOTIFICATION_SPLIT_MESSAGE_SEGMENTS_WAIT_TIME_SECONDS("atlas.notification.split.message.segments.wait.time.seconds", 15 * 60),
    NOTIFICATION_SPLIT_MESSAGE_BUFFER_PURGE_INTERVAL_SECONDS("atlas.notification.split.message.buffer.purge.interval.seconds", 5 * 60),
    NOTIFICATION_FIXED_BUFFER_ITEMS_INCREMENT_COUNT("atlas.notification.fixed.buffer.items.increment.count", 10),

    NOTIFICATION_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF("atlas.notification.consumer.create.shell.entity.for.non-existing.ref", true),
    REST_API_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF("atlas.rest.create.shell.entity.for.non-existing.ref", false),

    GRAPHSTORE_INDEXED_STRING_SAFE_LENGTH("atlas.graphstore.indexed.string.safe.length", Short.MAX_VALUE),  // based on org.apache.hadoop.hbase.client.Mutation.checkRow()

    RELATIONSHIP_WARN_NO_RELATIONSHIPS("atlas.relationships.warnOnNoRelationships", false),
    ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES("atlas.entity.change.notify.ignore.relationship.attributes", true),

    CLASSIFICATION_PROPAGATION_DEFAULT("atlas.classification.propagation.default", true),

    //search configuration
    SEARCH_MAX_LIMIT("atlas.search.maxlimit", 10000),
    SEARCH_DEFAULT_LIMIT("atlas.search.defaultlimit", 100),

    CUSTOM_ATTRIBUTE_KEY_MAX_LENGTH("atlas.custom.attribute.key.max.length", 50),
    CUSTOM_ATTRIBUTE_VALUE_MAX_LENGTH("atlas.custom.attribute.value.max.length", 500),
    CUSTOM_ATTRIBUTE_KEY_SPECIAL_PREFIX("atlas.custom.attribute.special.prefix", ""),

    LABEL_MAX_LENGTH("atlas.entity.label.max.length", 50),
    IMPORT_TEMP_DIRECTORY("atlas.import.temp.directory", ""),
    MIGRATION_IMPORT_START_POSITION("atlas.migration.import.start.position", 0),
    LINEAGE_USING_GREMLIN("atlas.lineage.query.use.gremlin", false),

    HTTP_HEADER_SERVER_VALUE("atlas.http.header.server.value","Apache Atlas"),
    STORAGE_CONSISTENCY_LOCK_ENABLED("atlas.graph.storage.consistency-lock.enabled", true),
    REBUILD_INDEX("atlas.rebuild.index", false),
    PROCESS_NAME_UPDATE_PATCH("atlas.process.name.update.patch", false),
    STORE_DIFFERENTIAL_AUDITS("atlas.entity.audit.differential", false),
    NOTIFY_DIFFERENTIAL_ENTITY_CHANGES("atlas.notification.differential.entity.changes.enabled", false),
    DSL_EXECUTOR_TRAVERSAL("atlas.dsl.executor.traversal", true),
    DSL_CACHED_TRANSLATOR("atlas.dsl.cached.translator", true),
    DEBUG_METRICS_ENABLED("atlas.debug.metrics.enabled", false),
    TASKS_USE_ENABLED("atlas.tasks.enabled", true),
    TASKS_PENDING_TASK_QUERY_SIZE_PAGE_SIZE("atlas.tasks.pending.tasks.query.page.size", 100),
    ATLAS_DISTRIBUTED_TASK_ENABLED("atlas.distributed.task.enabled", false),
    ENABLE_RELATIONSHIP_CLEANUP("atlas.distributed.task.relationship.cleanup", false),
    ENABLE_DISTRIBUTED_HAS_LINEAGE_CALCULATION("atlas.distributed.task.haslineage.calculation", false),
    TASKS_REQUEUE_GRAPH_QUERY("atlas.tasks.requeue.graph.query", false),
    TASKS_IN_PROGRESS_GRAPH_QUERY("atlas.tasks.inprogress.graph.query", false),
    TASKS_REQUEUE_POLL_INTERVAL("atlas.tasks.requeue.poll.interval.millis", 60000),
    TASKS_QUEUE_SIZE("atlas.tasks.queue.size", 10),
    SESSION_TIMEOUT_SECS("atlas.session.timeout.secs", -1),
    UPDATE_COMPOSITE_INDEX_STATUS("atlas.update.composite.index.status", true),
    TASKS_GRAPH_COMMIT_CHUNK_SIZE("atlas.tasks.graph.commit.chunk.size", 100),
    TAG_CASSANDRA_BATCHING_CHUNK_SIZE("atlas.tags.cassandra.batch.size", 200),
    MAX_NUMBER_OF_RETRIES("atlas.tasks.graph.retry.count", 3),
    GRAPH_TRAVERSAL_PARALLELISM("atlas.graph.traverse.bucket.size",10),
    LINEAGE_ON_DEMAND_ENABLED("atlas.lineage.on.demand.enabled", true),
    LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT("atlas.lineage.on.demand.default.node.count", 3),
    LINEAGE_MAX_NODE_COUNT("atlas.lineage.max.node.count", 100),
    USE_OPTIMISED_LINEAGE_CALCULATION("atlas.lineage.optimised.calculation", false),

    SUPPORTED_RELATIONSHIP_EVENTS("atlas.notification.relationships.filter", "asset_readme,asset_links"),
    ATLAS_RELATIONSHIP_CLEANUP_SUPPORTED_ASSET_TYPES("atlas.relationship.cleanup.supported.asset.types", "Process,AirflowTask"),
    ATLAS_RELATIONSHIP_CLEANUP_SUPPORTED_RELATIONSHIP_LABELS("atlas.relationship.cleanup.supported.relationship.labels", "__Process.inputs,__Process.outputs,__AirflowTask.inputs,__AirflowTask.outputs"),

    REST_API_XSS_FILTER_MASK_STRING("atlas.rest.xss.filter.mask.string", "map<[a-zA-Z _,:<>0-9\\x60]*>|struct<[a-zA-Z _,:<>0-9\\x60]*>|array<[a-zA-Z _,:<>0-9\\x60]*>|\\{\\{[a-zA-Z _,-:0-9\\x60\\{\\}]*\\}\\}"),
    REST_API_XSS_FILTER_EXLUDE_SERVER_NAME("atlas.rest.xss.filter.exclude.server.name", "atlas-service-atlas.atlas.svc.cluster.local"),


    INDEX_CLIENT_CONNECTION_TIMEOUT("atlas.index.client.connection.timeout.ms", 900000),
    INDEX_CLIENT_SOCKET_TIMEOUT("atlas.index.client.socket.timeout.ms", 900000),
    ENABLE_SEARCH_LOGGER("atlas.enable.search.logger", true),
    SEARCH_LOGGER_MAX_THREADS("atlas.enable.search.logger.max.threads", 20),

    PERSONA_POLICY_ASSET_MAX_LIMIT("atlas.persona.policy.asset.maxlimit", 1000),
    ENABLE_KEYCLOAK_TOKEN_INTROSPECTION("atlas.canary.keycloak.token-introspection", false),
    KEYCLOAK_TOKEN_INTROSPECT_CACHE_TTL_SECOND("atlas.keycloak.token-introspection.cache.ttl", 60),
    KEYCLOAK_INTROSPECTION_USE_CACHE("atlas.keycloak.introspection.use.cache", false),
    HERACLES_CLIENT_PAGINATION_SIZE("atlas.heracles.admin.resource-pagination-size", 100),
    HERACLES_API_SERVER_URL("atlas.heracles.api.service.url", "http://heracles-service.heracles.svc.cluster.local"),

    INDEXSEARCH_ASYNC_SEARCH_KEEP_ALIVE_TIME_IN_SECONDS("atlas.indexsearch.async.search.keep.alive.time.in.seconds", 300),
    ENABLE_ASYNC_INDEXSEARCH("atlas.indexsearch.async.enable", false),
    ATLAS_INDEXSEARCH_QUERY_SIZE_MAX_LIMIT("atlas.indexsearch.query.size.max.limit", 100000),
    ATLAS_INDEXSEARCH_LIMIT_UTM_TAGS("atlas.indexsearch.limit.ignore.utm.tags", ""),
    ATLAS_INDEXSEARCH_ENABLE_REQUEST_ISOLATION("atlas.indexsearch.request.isolation.enable", false),
    ATLAS_ELASTICSEARCH_UI_SEARCH_CLUSTER_URL("atlas.index.elasticsearch.ui.cluster.url","atlas-elasticsearch2-ui-search.atlas.svc.cluster.local:9200"),
    ATLAS_ELASTICSEARCH_NON_UI_SEARCH_CLUSTER_URL("atlas.index.elasticsearch.nonui.cluster.url","atlas-elasticsearch2-non-ui-search.atlas.svc.cluster.local:9200"),
    ATLAS_INDEXSEARCH_ENABLE_API_LIMIT("atlas.indexsearch.enable.api.limit", false),
    ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION("atlas.indexsearch.enable.janus.optimization", false),
    ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION_FOR_RELATIONS("atlas.indexsearch.enable.janus.optimization.for.relationship", false),
    ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION_FOR_CLASSIFICATIONS("atlas.indexsearch.enable.janus.optimization.for.classifications", false),
    ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION_FOR_LINEAGE("atlas.indexsearch.enable.janus.optimization.for.lineage", false),
    ATLAS_LINEAGE_ENABLE_CONNECTION_LINEAGE("atlas.lineage.enable.connection.lineage", false),
    ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION_EXTENDED("atlas.indexsearch.enable.janus.optimization.extended", false),
    ATLAS_INDEXSEARCH_EDGE_BULK_FETCH_ENABLE("atlas.indexsearch.edge.bulk.fetch.enable", true),
    ATLAS_INDEXSEARCH_EDGE_BULK_FETCH_BATCH_SIZE ("atlas.indexsearch.edge.bulk.fetch.batch.size", 10),
    ATLAS_MAINTENANCE_MODE("atlas.maintenance.mode", false),
    DELTA_BASED_REFRESH_ENABLED("atlas.authorizer.enable.delta_based_refresh", false),

    ATLAS_UD_RELATIONSHIPS_MAX_COUNT("atlas.ud.relationship.max.count", 100),

    // Slow query logging threshold for search endpoints (ms)
    SEARCH_SLOW_QUERY_THRESHOLD_MS("atlas.search.slow.query.threshold.ms", 1000),


    /***
     * OTEL Configuration
     */
    OTEL_RESOURCE_ATTRIBUTES("OTEL_RESOURCE_ATTRIBUTES", "service.name=atlas"),
    OTEL_SERVICE_NAME(" OTEL_SERVICE_NAME", "atlas"),
    OTEL_EXPORTER_OTLP_ENDPOINT("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
    ATLAS_BULK_API_MAX_ENTITIES_ALLOWED("atlas.bulk.api.max.entities.allowed", 10000),
    ATLAS_ENTITIES_ATTRIBUTE_ALLOWED_LARGE_ATTRIBUTES("atlas.entities.attribute.allowed.large.attributes", "rawQueryText,variablesSchemaBase64,visualBuilderSchemaBase64,dataContractSpec,dataContractJson"),

    ENABLE_ASYNC_TYPE_UPDATE("atlas.types.update.async.enable", false),
    MAX_THREADS_TYPE_UPDATE("atlas.types.update.thread.count", 4),
    MAX_EDGES_SUPER_VERTEX("atlas.jg.super.vertex.edge.count", 10000),
    TIMEOUT_SUPER_VERTEX_FETCH("atlas.jg.super.vertex.edge.timeout", 60),
    OPTIMISE_SUPER_VERTEX("atlas.jg.super.vertex.optimise", false),
    MIN_TIMEOUT_SUPER_VERTEX("atlas.jg.super.vertex.min.edge.timeout", 2),

    // Classification propagation thread pool configuration
    TAG_ASYNC_NOTIFIER_CORE_POOL_SIZE("atlas.classification.propagation.core.pool.size", 2),     // Reduced
    TAG_ASYNC_NOTIFIER_MAX_POOL_SIZE("atlas.classification.propagation.max.pool.size", 4),       // Reduced
    TAG_ASYNC_NOTIFIER_QUEUE_CAPACITY("atlas.classification.propagation.queue.capacity", 100),    // Reduced
    TAG_ASYNC_NOTIFIER_KEEP_ALIVE_SECONDS("atlas.classification.propagation.keep.alive.seconds", 60), // Reduced

    // ES and Cassandra batch operation configurations
    ES_BULK_BATCH_SIZE("atlas.es.bulk.batch.size", 500),
    CASSANDRA_BATCH_SIZE("atlas.cassandra.batch.size", 100),
    ES_MAX_RETRIES("atlas.es.max.retries", 5),
    ES_RETRY_DELAY_MS("atlas.es.retry.delay.ms", 1000),


    MIN_EDGES_SUPER_VERTEX("atlas.jg.super.vertex.min.edge.count", 100),

    // Task resource management configuration
    TASK_MEMORY_THRESHOLD_PERCENT("atlas.tasks.memory.threshold.percent", 75),
    TASK_HIGH_MEMORY_PAUSE_MS("atlas.tasks.high.memory.pause.ms", 2000),
    TASK_MAX_RETRY_ATTEMPTS("atlas.tasks.max.retry.attempts", 3);

    private static final Configuration APPLICATION_PROPERTIES;

    static {
        try {
            APPLICATION_PROPERTIES = ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    private final String propertyName;
    private final Object defaultValue;

    AtlasConfiguration(String propertyName, Object defaultValue) {
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public int getInt() {
        return APPLICATION_PROPERTIES.getInt(propertyName, Integer.valueOf(defaultValue.toString()).intValue());
    }

    public long getLong() {
        return APPLICATION_PROPERTIES.getLong(propertyName, Long.valueOf(defaultValue.toString()).longValue());
    }

    public boolean getBoolean() {
        return APPLICATION_PROPERTIES.getBoolean(propertyName, Boolean.valueOf(defaultValue.toString()).booleanValue());
    }

    public String getString() {
        return APPLICATION_PROPERTIES.getString(propertyName, defaultValue.toString());
    }

    public String[] getStringArray() {
        String[] ret = APPLICATION_PROPERTIES.getStringArray(propertyName);

        if (ret == null ||  ret.length == 0 || (ret.length == 1 && StringUtils.isEmpty(ret[0]))) {
            if (defaultValue != null) {
                ret = StringUtils.split(defaultValue.toString(), ',');
            }
        }

        return ret;
    }

    public String[] getStringArray(String... defaultValue) {
        String[] ret = APPLICATION_PROPERTIES.getStringArray(propertyName);

        if (ret == null ||  ret.length == 0 || (ret.length == 1 && StringUtils.isEmpty(ret[0]))) {
            ret = defaultValue;
        }

        return ret;
    }

    public Object get() {
        Object value = APPLICATION_PROPERTIES.getProperty(propertyName);
        return value == null ? defaultValue : value;
    }
}
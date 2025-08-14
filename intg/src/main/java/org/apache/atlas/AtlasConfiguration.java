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
    WEBSERVER_KEEPALIVE_SECONDS("atlas.webserver.keepalivetimesecs", 60),
    WEBSERVER_QUEUE_SIZE("atlas.webserver.queuesize", 100),
    WEBSERVER_REQUEST_BUFFER_SIZE("atlas.jetty.request.buffer.size", 16192),

    QUERY_PARAM_MAX_LENGTH("atlas.query.param.max.length", 4 * 1024),

    REST_API_ENABLE_DELETE_TYPE_OVERRIDE("atlas.rest.enable.delete.type.override", false),
    NOTIFICATION_RELATIONSHIPS_ENABLED("atlas.notification.relationships.enabled", false),

    NOTIFICATION_HOOK_TOPIC_NAME("atlas.notification.hook.topic.name", "ATLAS_HOOK"),
    NOTIFICATION_ENTITIES_TOPIC_NAME("atlas.notification.entities.topic.name", "ATLAS_ENTITIES"),

    NOTIFICATION_HOOK_CONSUMER_BUFFERING_INTERVAL("atlas.notification.consumer.message.buffering.interval.seconds", 15),
    NOTIFICATION_HOOK_CONSUMER_BUFFERING_BATCH_SIZE("atlas.notification.consumer.message.buffering.batch.size", 100),

    NOTIFICATION_HOOK_REST_ENABLED("atlas.hook.rest.notification.enabled", false),
    NOTIFICATION_HOOK_CONSUMER_TOPIC_NAMES("atlas.notification.hook.consumer.topic.names", "ATLAS_HOOK"), //  a comma separated list of topic names
    NOTIFICATION_ENTITIES_CONSUMER_TOPIC_NAMES("atlas.notification.entities.consumer.topic.names", "ATLAS_ENTITIES"), //  a comma separated list of topic names

    NOTIFICATION_REST_BODY_MAX_LENGTH_BYTES("atlas.notification.rest.body.max.length.bytes", (1024 * 1024)),
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
    LINEAGE_ON_DEMAND_ENABLED("atlas.lineage.on.demand.enabled", false),
    LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT("atlas.lineage.on.demand.default.node.count", 3),
    LINEAGE_MAX_NODE_COUNT("atlas.lineage.max.node.count", 9000),

    HTTP_HEADER_SERVER_VALUE("atlas.http.header.server.value", "Apache Atlas"),
    STORAGE_CONSISTENCY_LOCK_ENABLED("atlas.graph.storage.consistency-lock.enabled", true),
    REBUILD_INDEX("atlas.rebuild.index", false),
    PROCESS_NAME_UPDATE_PATCH("atlas.process.name.update.patch", false),
    PROCESS_IMPALA_NAME_UPDATE_PATCH("atlas.process.impala.name.update.patch", false),
    STORE_DIFFERENTIAL_AUDITS("atlas.entity.audit.differential", false),
    DSL_EXECUTOR_TRAVERSAL("atlas.dsl.executor.traversal", true),
    DSL_CACHED_TRANSLATOR("atlas.dsl.cached.translator", true),
    DEBUG_METRICS_ENABLED("atlas.debug.metrics.enabled", false),
    TASKS_USE_ENABLED("atlas.tasks.enabled", true),
    SESSION_TIMEOUT_SECS("atlas.session.timeout.secs", -1),
    UPDATE_COMPOSITE_INDEX_STATUS("atlas.update.composite.index.status", true),
    METRICS_TIME_TO_LIVE_HOURS("atlas.metrics.ttl.hours", 336), // 14 days default
    SOLR_INDEX_TX_LOG_TTL_CONF("write.ahead.log.ttl.in.hours", 240), //10 days default

    ATLAS_AUDIT_AGING_ENABLED("atlas.audit.aging.enabled", false),
    ATLAS_AUDIT_DEFAULT_AGEOUT_ENABLED("atlas.audit.default.ageout.enabled", true),
    ATLAS_AUDIT_DEFAULT_AGEOUT_TTL("atlas.audit.default.ageout.ttl.in.days", 90),
    ATLAS_AUDIT_DEFAULT_AGEOUT_COUNT("atlas.audit.default.ageout.count", 0),
    ATLAS_AUDIT_CUSTOM_AGEOUT_TTL("atlas.audit.custom.ageout.ttl.in.days", 0),
    ATLAS_AUDIT_CUSTOM_AGEOUT_COUNT("atlas.audit.custom.ageout.count", 0),
    ATLAS_AUDIT_SWEEP_OUT("atlas.audit.sweep.out.enabled", false),
    ATLAS_AUDIT_CREATE_EVENTS_AGEOUT_ALLOWED("atlas.audit.create.events.ageout.allowed", false),
    ATLAS_AUDIT_AGING_SCHEDULER_FREQUENCY("atlas.audit.aging.scheduler.frequency.in.days", 30),
    ATLAS_AUDIT_AGING_SUBTYPES_INCLUDED("atlas.audit.aging.subtypes.included", true),
    MIN_TTL_TO_MAINTAIN("atlas.audit.min.ttl.to.maintain", 7),
    MIN_AUDIT_COUNT_TO_MAINTAIN("atlas.audit.min.count.to.maintain", 50),
    ATLAS_AUDIT_AGING_SEARCH_MAX_LIMIT("atlas.audit.aging.search.maxlimit", 10000),
    ATLAS_AUDIT_DEFAULT_AGEOUT_IGNORE_TTL("atlas.audit.default.ageout.ignore.ttl", false),
    ATLAS_AUDIT_AGING_TTL_TEST_AUTOMATION("atlas.audit.aging.ttl.test.automation", false), //Only for test automation
    RELATIONSHIP_SEARCH_ENABLED("atlas.relationship.search.enabled", false),
    UI_TASKS_TAB_USE_ENABLED("atlas.tasks.ui.tab.enabled", false),
    ATLAS_ASYNC_IMPORT_MIN_DURATION_OVERRIDE_TEST_AUTOMATION("atlas.async.import.min.duration.override.test.automation", false),
    ASYNC_IMPORT_TOPIC_PREFIX("atlas.async.import.topic.prefix", "ATLAS_IMPORT_"),
    ASYNC_IMPORT_REQUEST_ID_PREFIX("atlas.async.import.request_id.prefix", "async_import_"),
    REPLACE_HUGE_SPARK_PROCESS_ATTRIBUTES_PATCH("atlas.process.spark.attributes.update.patch", false);
    private static final Configuration APPLICATION_PROPERTIES;

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
        return APPLICATION_PROPERTIES.getInt(propertyName, Integer.parseInt(defaultValue.toString()));
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

        if (ret == null || ret.length == 0 || (ret.length == 1 && StringUtils.isEmpty(ret[0]))) {
            if (defaultValue != null) {
                ret = StringUtils.split(defaultValue.toString(), ',');
            }
        }

        return ret;
    }

    public String[] getStringArray(String... defaultValue) {
        String[] ret = APPLICATION_PROPERTIES.getStringArray(propertyName);

        if (ret == null || ret.length == 0 || (ret.length == 1 && StringUtils.isEmpty(ret[0]))) {
            ret = defaultValue;
        }

        return ret;
    }

    public Object get() {
        Object value = APPLICATION_PROPERTIES.getProperty(propertyName);

        return value == null ? defaultValue : value;
    }

    static {
        try {
            APPLICATION_PROPERTIES = ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }
}

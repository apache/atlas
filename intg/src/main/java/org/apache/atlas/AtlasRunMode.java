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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controls which subsystems Atlas starts based on the {@code RUN_MODE}
 * environment variable or system property.
 *
 * <table>
 *   <tr><th>RUN_MODE</th><th>What runs</th></tr>
 *   <tr><td>(not set)</td>
 *       <td>MONOLITHIC — every subsystem runs on this node (default, backward-compatible).</td></tr>
 *   <tr><td>INITIALIZER</td>
 *       <td>Graph index setup, type-def bootstrap, Java patch application — then the JVM exits.
 *           Designed for a Kubernetes init-container or a CDPD pre-start job that prepares the
 *           shared store once before the actual server nodes start.</td></tr>
 *   <tr><td>METADATA_SERVER</td>
 *       <td>REST APIs, search, entity CRUD, type-def management, task workers, import/export,
 *           index recovery, typedef-sync Kafka consumer.
 *           Does NOT run patches or consume from the hook Kafka topic.
 *           Assumes an INITIALIZER run has already prepared the store.</td></tr>
 *   <tr><td>NOTIFICATION_PROCESSOR</td>
 *       <td>Hook Kafka consumer only — reads hook messages and writes entities to the graph.
 *           Does NOT run patches, does NOT serve REST/search APIs.
 *           Typedef-sync consumer runs so the in-memory type registry stays current.</td></tr>
 * </table>
 *
 * <p>The value is resolved once at class-load time and is thereafter immutable.
 *
 * <p>The value is resolved once at JVM startup and is thereafter immutable.
 */
public enum AtlasRunMode {
    /**
     * Every subsystem on this node — initialization, REST server, Kafka consumers.
     * Default when no RUN_MODE is configured.
     */
    MONOLITHIC,

    /**
     * Index setup + type-def bootstrap + patch application, then {@code System.exit(0)}.
     * No REST server, no Kafka consumers.
     */
    INITIALIZER,

    /**
     * REST APIs, search, entity CRUD, task workers, import/export, index recovery,
     * typedef-sync consumer.
     * No hook Kafka consumer, no patch application.
     */
    METADATA_SERVER,

    /**
     * Hook Kafka consumer only.
     * Reads hook messages and writes entities/relationships to the graph.
     * Typedef-sync consumer runs to keep the in-memory type registry current.
     * No REST APIs served, no patches, no index recovery.
     */
    NOTIFICATION_PROCESSOR;

    private static final Logger       LOG     = LoggerFactory.getLogger(AtlasRunMode.class);
    private static final AtlasRunMode CURRENT = resolve();

    /** Returns the mode resolved at JVM startup — immutable for the lifetime of the JVM. */
    public static AtlasRunMode current() {
        return CURRENT;
    }

    // -----------------------------------------------------------------------
    // Predicates — used by each ActiveStateChangeHandler in instanceIsActive()
    // -----------------------------------------------------------------------

    /**
     * Returns {@code true} when this mode should execute one-time cluster initialization:
     * graph index setup, type-def bootstrap, and patch application.
     * <p>True for: {@code MONOLITHIC}, {@code INITIALIZER}.
     */
    public boolean runsInitialization() {
        return this == MONOLITHIC || this == INITIALIZER;
    }

    /**
     * Returns {@code true} when this mode runs any long-lived server process
     * (i.e. the JVM does not exit after initialization).
     * <p>True for: {@code MONOLITHIC}, {@code METADATA_SERVER}, {@code NOTIFICATION_PROCESSOR}.
     */
    public boolean runsServer() {
        return this == MONOLITHIC || this == METADATA_SERVER || this == NOTIFICATION_PROCESSOR;
    }

    /**
     * Returns {@code true} when this mode serves REST APIs, search, entity CRUD,
     * import/export, task workers, and index recovery.
     * <p>True for: {@code MONOLITHIC}, {@code METADATA_SERVER}.
     */
    public boolean runsMetadataServer() {
        return this == MONOLITHIC || this == METADATA_SERVER;
    }

    /**
     * Returns {@code true} when this mode consumes and processes hook Kafka messages.
     * <p>True for: {@code MONOLITHIC}, {@code NOTIFICATION_PROCESSOR}.
     */
    public boolean runsNotificationProcessing() {
        return this == MONOLITHIC || this == NOTIFICATION_PROCESSOR;
    }

    /**
     * Returns {@code true} when this mode should set up JanusGraph indices and
     * run the search-indexer initialization.  Skipped for {@code NOTIFICATION_PROCESSOR}
     * because that mode does not serve search queries.
     * <p>True for: {@code MONOLITHIC}, {@code INITIALIZER}, {@code METADATA_SERVER}.
     */
    public boolean runsIndexSetup() {
        return this != NOTIFICATION_PROCESSOR;
    }

    /**
     * Returns {@code true} when the JVM should call {@code System.exit(0)} after all
     * initialization handlers complete.
     * <p>True for: {@code INITIALIZER} only.
     */
    public boolean exitsAfterInit() {
        return this == INITIALIZER;
    }

    // -----------------------------------------------------------------------

    private static AtlasRunMode resolve() {
        // 1. Check RUN_MODE (new property)
        String value = System.getenv("RUN_MODE");
        if (value == null || value.isEmpty()) {
            value = System.getProperty("RUN_MODE");
        }

        if (value != null && !value.isEmpty()) {
            try {
                AtlasRunMode mode = valueOf(value.toUpperCase().trim());
                LOG.info("AtlasRunMode: RUN_MODE='{}' — running in {} mode", value, mode);
                return mode;
            } catch (IllegalArgumentException e) {
                LOG.warn("AtlasRunMode: unknown RUN_MODE='{}' (valid: MONOLITHIC, INITIALIZER, METADATA_SERVER, NOTIFICATION_PROCESSOR) — defaulting to MONOLITHIC", value);
                return MONOLITHIC;
            }
        }

        // 2. Default
        LOG.info("AtlasRunMode: RUN_MODE not set — running in MONOLITHIC mode");
        return MONOLITHIC;
    }
}

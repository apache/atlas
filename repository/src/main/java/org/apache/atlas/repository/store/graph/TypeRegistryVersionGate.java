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
package org.apache.atlas.repository.store.graph;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * Keeps this node's in-memory type registry current with the shared store for active reads.
 *
 * <p>A typedef created on one node is in the graph immediately, but peers only rebuild their
 * registry when the async typedef-sync (Kafka) signal arrives. This gate reads the indexed
 * registry-version vertex on each call and reloads only when that counter has moved, so a peer
 * can answer with the latest types without waiting for Kafka. {@code TypeDefSyncConsumer}
 * also polls this counter so a missed Kafka publish is still reconciled after the broker
 * returns, without a node restart.
 *
 * <p>The store is taken as a {@link Provider} rather than the store itself, because asking for it
 * during construction closes a cycle: {@link AtlasTypeDefStore} needs its typedef-change listeners,
 * one of which reaches {@code AtlasEntityStoreV2} through the audit service, and that is a bean
 * this one is injected into.
 */
@Component
@Singleton
public class TypeRegistryVersionGate {
    private static final Logger LOG = LoggerFactory.getLogger(TypeRegistryVersionGate.class);

    private final AtlasGraph                  graph;
    private final Provider<AtlasTypeDefStore> typeDefStoreProvider;

    private long lastSeenVersion;

    @Inject
    public TypeRegistryVersionGate(AtlasGraph graph, Provider<AtlasTypeDefStore> typeDefStoreProvider) {
        this.graph                = graph;
        this.typeDefStoreProvider = typeDefStoreProvider;
    }

    /**
     * Reloads this node's type registry when the shared version counter has moved since the last
     * reload on this node. Kafka {@code TypeDefSyncConsumer} and REST both call this method so
     * they share one watermark and one {@code refreshFromStore()} — a delta apply, not a full
     * catalog rebuild. {@code init()} remains the startup / fallback rebuild.
     *
     * @return {@code true} when a reload ran, {@code false} when the registry was already current
     *         or the reload failed (the typedef-sync consumer retries).
     */
    public boolean ensureUpToDate() {
        long storeVersion = AtlasGraphUtilsV2.getTypeDefRegistryVersion(graph);

        if (storeVersion <= lastSeenVersion) {
            return false;
        }

        synchronized (this) {
            if (storeVersion <= lastSeenVersion) {
                return false;
            }

            try {
                LOG.info("TypeRegistryVersionGate: store version {} is ahead of last-seen {}; applying typedef delta",
                        storeVersion, lastSeenVersion);

                typeDefStoreProvider.get().refreshFromStore();
                expireMixedIndexSchema();

                lastSeenVersion = storeVersion;

                return true;
            } catch (AtlasBaseException excp) {
                LOG.warn("TypeRegistryVersionGate: delta refresh failed at store version {}; the typedef-sync path will retry",
                        storeVersion, excp);

                return false;
            }
        }
    }

    /**
     * A typedef delta can add fields to a mixed index on another node. This node's cache of
     * {@code vertex_index} stays stale until Janus delivers the schema-eviction log, which is
     * later than the registry reload. Drop the cached index definitions so the next write reads
     * the fields the peer stored.
     */
    private void expireMixedIndexSchema() {
        graph.expireCachedSchemaElement(Constants.VERTEX_INDEX);
        graph.expireCachedSchemaElement(Constants.EDGE_INDEX);
        graph.expireCachedSchemaElement(Constants.FULLTEXT_INDEX);
    }

    /**
     * Point-loads one classification a peer has already written, when the category scan used by
     * {@link #forceRefresh()} has not published it. The typename index answers this without a scan.
     */
    public boolean installClassification(String name) {
        if (StringUtils.isBlank(name)) {
            return false;
        }

        try {
            return typeDefStoreProvider.get().installClassificationFromGraph(name);
        } catch (AtlasBaseException excp) {
            LOG.warn("TypeRegistryVersionGate: could not load classification '{}' from the store", name, excp);

            return false;
        }
    }

    /**
     * Point-loads one business-metadata definition a peer has already written, when the category
     * scan used by {@link #forceRefresh()} has not published it.
     */
    public boolean installBusinessMetadata(String name) {
        if (StringUtils.isBlank(name)) {
            return false;
        }

        try {
            return typeDefStoreProvider.get().installBusinessMetadataFromGraph(name);
        } catch (AtlasBaseException excp) {
            LOG.warn("TypeRegistryVersionGate: could not load business metadata '{}' from the store", name, excp);

            return false;
        }
    }

    /**
     * Reloads from the store even when the version watermark says this node is current.
     * Use only on the failure path where a type is already known to be missing — the
     * watermark can be stale if {@link #markSeen(long)} ran before the type landed, or
     * if the graph index has not yet published the version bump.
     *
     * @return {@code true} when a reload ran, {@code false} when it failed
     */
    public boolean forceRefresh() {

        synchronized (this) {
            try {
                LOG.info("TypeRegistryVersionGate: forcing typedef delta refresh (last-seen {})", lastSeenVersion);

                typeDefStoreProvider.get().refreshFromStore();

                lastSeenVersion = AtlasGraphUtilsV2.getTypeDefRegistryVersion(graph);

                return true;
            } catch (AtlasBaseException excp) {
                LOG.warn("TypeRegistryVersionGate: forced delta refresh failed; the typedef-sync path will retry", excp);

                return false;
            }
        }
    }

    /**
     * Records that this node's in-memory registry already reflects {@code version} — used after
     * a local typedef write (the writer updated the registry in-place) or after applying this
     * node's own Kafka signal, so the next read does not pay for a full catalog rebuild.
     */
    public void markSeen(long version) {
        synchronized (this) {
            if (version > lastSeenVersion) {
                lastSeenVersion = version;
            }
        }
    }

    /**
     * {@code true} when the shared store counter is not ahead of {@link #lastSeenVersion}.
     * The Kafka consumer uses this after {@link #ensureUpToDate()} so a failed reload is
     * retried instead of being recorded as applied.
     */
    public boolean isCurrent() {
        long storeVersion = AtlasGraphUtilsV2.getTypeDefRegistryVersion(graph);

        synchronized (this) {
            return storeVersion <= lastSeenVersion;
        }
    }
}

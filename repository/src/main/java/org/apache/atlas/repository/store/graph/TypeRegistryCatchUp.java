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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * Brings this node's type registry up to date on demand, for callers that cannot tolerate not
 * finding a type that already exists.
 *
 * <p>A typedef created on one node reaches its peers over the typedef-changes topic, which takes a
 * second or more. Reads survive that window because {@link AtlasTypeDefGraphStore} falls back to
 * the store, but a caller that needs a resolved {@link AtlasClassificationType} cannot: a type is
 * only usable once it has been resolved against every other type, which happens when the registry
 * is rebuilt. So a classification created on one node and applied on another moments later is
 * rejected as unknown, even though it exists.
 *
 * <p>Reloading is still the typedef-sync path's job; this only asks it to happen now rather than
 * when the signal arrives, and only when it would change the answer:
 * <ul>
 *   <li>the store must actually hold the type, so an unknown name costs one indexed lookup and
 *       still fails, and cannot be used to force repeated reloads;</li>
 *   <li>reloads are spaced by {@link AtlasConfiguration#TYPEDEF_CATCHUP_MIN_RELOAD_INTERVAL_MS}, so
 *       a burst of requests naming the same missing type causes one rebuild rather than one each.</li>
 * </ul>
 *
 * <p>The store is taken as a {@link Provider} rather than the store itself, because asking for it
 * during construction closes a cycle: {@link AtlasTypeDefStore} needs its typedef-change listeners,
 * one of which reaches {@code AtlasEntityStoreV2} through the audit service, and that is the bean
 * this one is injected into. Resolving the store on first use instead keeps this off the startup
 * dependency graph entirely.
 */
@Component
@Singleton
public class TypeRegistryCatchUp {
    private static final Logger LOG = LoggerFactory.getLogger(TypeRegistryCatchUp.class);

    private final AtlasTypeRegistry           typeRegistry;
    private final Provider<AtlasTypeDefStore> typeDefStoreProvider;
    private final long                        minReloadIntervalMs;

    private long lastReloadAtMs;

    @Inject
    public TypeRegistryCatchUp(AtlasTypeRegistry typeRegistry, Provider<AtlasTypeDefStore> typeDefStoreProvider) {
        this.typeRegistry         = typeRegistry;
        this.typeDefStoreProvider = typeDefStoreProvider;
        this.minReloadIntervalMs  = AtlasConfiguration.TYPEDEF_CATCHUP_MIN_RELOAD_INTERVAL_MS.getLong();
    }

    /**
     * Looks up a classification type, reloading the registry first if this node has fallen behind a
     * peer that created it.
     *
     * @return the resolved type, or null if no type by this name exists anywhere.
     */
    public AtlasClassificationType classificationType(String typeName) {
        AtlasClassificationType ret = typeRegistry.getClassificationTypeByName(typeName);

        if (ret != null || !storeHasClassification(typeName)) {
            return ret;
        }

        synchronized (this) {
            // Another thread may have reloaded while this one waited, which is the common case
            // when a batch of entities is tagged with a classification a peer just created.
            ret = typeRegistry.getClassificationTypeByName(typeName);

            if (ret != null) {
                return ret;
            }

            long sinceLastReload = System.currentTimeMillis() - lastReloadAtMs;

            if (sinceLastReload < minReloadIntervalMs) {
                LOG.debug("classificationType({}): reloaded {}ms ago and still absent; not reloading again", typeName, sinceLastReload);

                return null;
            }

            try {
                LOG.info("classificationType({}): in the store but not in this node's registry; reloading to catch up with the peer that created it", typeName);

                typeDefStoreProvider.get().init();
            } catch (AtlasBaseException excp) {
                LOG.warn("classificationType({}): reload failed; the typedef-sync path will retry", typeName, excp);

                return null;
            } finally {
                lastReloadAtMs = System.currentTimeMillis();
            }
        }

        return typeRegistry.getClassificationTypeByName(typeName);
    }

    /**
     * @return whether the store holds a classification by this name, whatever this node's registry
     * believes. Reads go through {@link AtlasTypeDefGraphStore}, which falls back to the store when
     * the registry has not caught up.
     */
    private boolean storeHasClassification(String typeName) {
        try {
            return typeDefStoreProvider.get().getClassificationDefByName(typeName) != null;
        } catch (AtlasBaseException excp) {
            LOG.debug("storeHasClassification({}): no such classification", typeName, excp);

            return false;
        }
    }
}

/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.atlas.connector.entities;

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Base class for all couchbase atlas models
 * The class uses "Self-Builder" pattern:
 * 1. First, create the "builder" instance of the class
 * 2. Populate the identifying fields of the class (check the `qualifiedName` method of the entity for the list)
 * (all setters return the instance just as a Builder would)
 * 3. Call `get()` method to resolve the instance and replace it with previously loaded from Atlas data (if present)
 * <p>
 * Example:
 * ```java
 * clusterEntity = new CouchbaseCluster()
 * .name(CBConfig.address())
 * .url(CBConfig.address())
 * .get();
 * ```
 *
 * @param <E> extending class
 */
public abstract class CouchbaseAtlasEntity<E extends CouchbaseAtlasEntity<?>> {
    private static final Map<Class, Map<String, AtlasEntity>>          ENTITY_BY_TYPE_AND_ID = Collections.synchronizedMap(new HashMap<>());
    private static final Map<Class, Map<String, CouchbaseAtlasEntity>> MODEL_BY_TYPE_AND_ID  = Collections.synchronizedMap(new HashMap<>());
    private              String                                        name;

    public static void dropCache() {
        ENTITY_BY_TYPE_AND_ID.clear();
        MODEL_BY_TYPE_AND_ID.clear();
    }

    public String name() {
        return name;
    }

    public E name(String name) {
        this.name = name;
        return (E) this;
    }

    /**
     * Loads or creates corresponding Atlas Entity and marks this model as existing in Atlas
     *
     * @param atlas
     * @return
     */
    public AtlasEntity atlasEntity(AtlasClientV2 atlas) {
        AtlasEntity atlasEntity = atlasEntity()
                .filter(entity -> entity.getGuid().charAt(0) != '-')
                .orElseGet(() ->
                        cache(load(atlas)
                                .orElseGet(() ->
                                        atlasEntity()
                                                .orElseGet(() -> new AtlasEntity(atlasTypeName())))));

        atlasEntity.setAttribute("name", name);
        atlasEntity.setAttribute("qualifiedName", qualifiedName());

        updateAtlasEntity(atlasEntity);

        return atlasEntity;
    }

    /**
     * Looks up precreated atlas entity in the entity cache
     *
     * @return Optional of the cached entity
     */
    public Optional<AtlasEntity> atlasEntity() {
        return cachedEntity().map(atlasEntity -> {
            updateAtlasEntity(atlasEntity);
            return atlasEntity;
        });
    }

    public abstract String atlasTypeName();

    public abstract UUID id();

    /**
     * First checks if the entity has been loaded and cached and, if not, then tries to load it from Atlas
     *
     * @param atlas Atlas client to use
     * @return true if the entity found either in cache or in Atlas
     */
    public boolean exists(AtlasClientV2 atlas) {
        if (!exists()) {
            return load(atlas).isPresent();
        }
        return true;
    }

    /**
     * Returns pre-cached model with provided identifiers or caches this model and returns it
     *
     * @return the model
     */
    public E get() {
        Class<E> type = (Class<E>) getClass();
        String   id   = id().toString();

        // ensure valid cache structure
        if (!MODEL_BY_TYPE_AND_ID.containsKey(type)) {
            MODEL_BY_TYPE_AND_ID.put(type, Collections.synchronizedMap(new HashMap<>()));
        }

        // put the model into the cache, if not already present
        Map<String, CouchbaseAtlasEntity> modelsById = MODEL_BY_TYPE_AND_ID.get(type);
        if (!modelsById.containsKey(id)) {
            try {
                modelsById.put(id, this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return (E) modelsById.get(id);
    }

    protected abstract String qualifiedName();

    /**
     * Checks whether the model has the Atlas Entity created for it
     * by looking it up in the entity cache.
     * NOTE: this method does not check if the entity has been saved in Atlas so,
     * it will return true when the entity is already created and cached but is yet to be sent to Atlas
     * <p>
     * This method is _mostly_ used in related objects when setting relationship field to ensure that related
     * model has an AtlasEntity that can be referenced when storing relationship information.
     *
     * @return true if the entity found
     */
    protected boolean exists() {
        return cachedEntity().isPresent();
    }

    /**
     * Invoked when the entity needs to be updated with values from the model
     *
     * @param entity the entity to write the values into
     */
    protected void updateAtlasEntity(AtlasEntity entity) {
        // override me
    }

    /**
     * Invoked when the model needs to be updated with values from the entity
     *
     * @param entity the entity to read the values from
     */
    protected void updateJavaModel(AtlasEntity entity) {
        // override me
    }

    /**
     * Loads the entity for this model from Atlas and stores it in the entity cache
     *
     * @param client Atlas client to use
     * @return loaded entity
     */
    private Optional<AtlasEntity> load(AtlasClientV2 client) {
        try {
            Map<String, String> query = new HashMap<>();
            query.put("qualifiedName", qualifiedName());
            AtlasEntity atlasEntity = client.getEntityByAttribute(atlasTypeName(), query).getEntity();

            if (atlasEntity != null) {
                cache(atlasEntity);
                if (atlasEntity.hasAttribute("name")) {
                    this.name = (String) atlasEntity.getAttribute("name");
                }
                updateJavaModel(atlasEntity);
                return Optional.of(atlasEntity);
            }
        } catch (AtlasServiceException e) {
            if (e.getStatus().getStatusCode() != 404) {
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
    }

    /**
     * Puts an entity into the entity cache
     *
     * @param atlasEntity the entity to cache
     * @return the same entity
     */
    private AtlasEntity cache(AtlasEntity atlasEntity) {
        if (!ENTITY_BY_TYPE_AND_ID.containsKey(getClass())) {
            ENTITY_BY_TYPE_AND_ID.put(getClass(), new HashMap<>());
        }

        ENTITY_BY_TYPE_AND_ID.get(getClass()).put(id().toString(), atlasEntity);
        return atlasEntity;
    }

    /**
     * Looks up the entity in the cache
     *
     * @return Optional of cached entity
     */
    private Optional<AtlasEntity> cachedEntity() {
        return Optional.ofNullable(ENTITY_BY_TYPE_AND_ID.getOrDefault(getClass(), (Map<String, AtlasEntity>) Collections.EMPTY_MAP).getOrDefault(id().toString(), null));
    }
}

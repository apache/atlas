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
import org.apache.atlas.model.instance.AtlasEntity;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CouchbaseBucket extends CouchbaseAtlasEntity<CouchbaseBucket> {
    public static final String                      TYPE_NAME  = "couchbase_bucket";

    private final transient Map<String, CouchbaseScope> scopes = Collections.synchronizedMap(new HashMap<>());

    private                 CouchbaseCluster            cluster;

    public CouchbaseBucket() {
    }

    @Override
    public AtlasEntity atlasEntity(AtlasClientV2 atlas) {
        AtlasEntity entity = super.atlasEntity(atlas);
        entity.setRelationshipAttribute("cluster", cluster.atlasEntity(atlas));
        return entity;
    }

    @Override
    protected String qualifiedName() {
        return String.format("%s/%s", cluster.qualifiedName(), name());
    }

    @Override
    public String atlasTypeName() {
        return TYPE_NAME;
    }

    @Override
    public UUID id() {
        return UUID.nameUUIDFromBytes(String.format("%s:%s:%s", atlasTypeName(), cluster().id(), name()).getBytes(Charset.defaultCharset()));
    }

    public CouchbaseCluster cluster() {
        return cluster;
    }

    public CouchbaseBucket cluster(CouchbaseCluster cluster) {
        this.cluster = cluster;
        return this;
    }

    public CouchbaseScope scope(String name) {
        if (!scopes.containsKey(name)) {
            scopes.put(name, new CouchbaseScope().bucket(this).name(name).get());
        }

        return scopes.get(name);
    }
}

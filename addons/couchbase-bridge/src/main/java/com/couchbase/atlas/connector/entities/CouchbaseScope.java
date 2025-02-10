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

public class CouchbaseScope extends CouchbaseAtlasEntity<CouchbaseScope> {
    public static final String          TYPE_NAME = "couchbase_scope";

    private final transient Map<String, CouchbaseCollection> collections = Collections.synchronizedMap(new HashMap<>());

    private             CouchbaseBucket bucket;

    public CouchbaseBucket bucket() {
        return bucket;
    }

    public CouchbaseScope bucket(CouchbaseBucket bucket) {
        this.bucket = bucket;
        return this;
    }

    @Override
    public AtlasEntity atlasEntity(AtlasClientV2 atlas) {
        AtlasEntity entity = super.atlasEntity(atlas);
        entity.setRelationshipAttribute("bucket", bucket.atlasEntity(atlas));
        return entity;
    }

    @Override
    public String qualifiedName() {
        return String.format("%s/%s", bucket.qualifiedName(), name());
    }

    @Override
    public String atlasTypeName() {
        return TYPE_NAME;
    }

    @Override
    public UUID id() {
        return UUID.nameUUIDFromBytes(
                String.format("%s:%s:%s",
                        atlasTypeName(),
                        bucket().id().toString(),
                        name()
                ).getBytes(Charset.defaultCharset()));
    }

    public CouchbaseCollection collection(String name) {
        if (!collections.containsKey(name)) {
            collections.put(name, new CouchbaseCollection().name(name).scope(this).get());
        }

        return collections.get(name);
    }
}

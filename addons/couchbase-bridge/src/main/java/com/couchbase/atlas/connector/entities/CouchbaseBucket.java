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
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.type.AtlasTypeUtil;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class CouchbaseBucket extends CouchbaseAtlasEntity<CouchbaseBucket> {
    public static final String TYPE_NAME = "couchbase_bucket";
    private CouchbaseCluster cluster;
    private transient Map<String, CouchbaseScope> scopes = Collections.synchronizedMap(new HashMap<>());

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

    public CouchbaseBucket() {

    }

    public CouchbaseCluster cluster() {
        return cluster;
    }

    public CouchbaseBucket cluster(CouchbaseCluster cluster) {
        this.cluster = cluster;
        return this;
    }

    public static AtlasEntityDef atlasEntityDef() {
        AtlasEntityDef definition = AtlasTypeUtil.createClassTypeDef(
                "couchbase_bucket",
                new HashSet<>()
        );

        definition.getSuperTypes().add("Asset");
        definition.setServiceType("couchbase");
        definition.setTypeVersion("0.1");

        List<AtlasStructDef.AtlasAttributeDef> attributes = definition.getAttributeDefs();

        return definition;
    }

    public static Collection<? extends AtlasRelationshipDef> atlasRelationshipDefs() {
        return Arrays.asList(
                new AtlasRelationshipDef(
                        "couchbase_bucket_scopes",
                        "",
                        "0.1",
                        "couchbase",
                        AtlasRelationshipDef.RelationshipCategory.AGGREGATION,
                        AtlasRelationshipDef.PropagateTags.ONE_TO_TWO,
                        new AtlasRelationshipEndDef(
                                "couchbase_bucket",
                                "scopes",
                                AtlasStructDef.AtlasAttributeDef.Cardinality.SET,
                                true
                        ),
                        new AtlasRelationshipEndDef(
                                "couchbase_scope",
                                "bucket",
                                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE,
                                false
                        )
                )
        );
    }

    @Override
    public String atlasTypeName() {
        return TYPE_NAME;
    }

    @Override
    public UUID id() {
        return UUID.nameUUIDFromBytes(String.format("%s:%s:%s", atlasTypeName(), cluster().id(), name()).getBytes(Charset.defaultCharset()));
    }

    public CouchbaseScope scope(String name) {
        if (!scopes.containsKey(name)) {
            scopes.put(name, new CouchbaseScope()
                    .bucket(this)
                    .name(name)
                    .get()
            );
        }

        return scopes.get(name);
    }
}

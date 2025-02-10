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
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

public class CouchbaseCluster extends CouchbaseAtlasEntity<CouchbaseCluster> {
    public static final String TYPE_NAME = "couchbase_cluster";
    private             String url;

    public static AtlasEntityDef atlasEntityDef() {
        AtlasEntityDef definition = AtlasTypeUtil.createClassTypeDef(
                "couchbase_cluster",
                new HashSet<>());

        definition.getSuperTypes().add("Asset");
        definition.setServiceType("couchbase");
        definition.setTypeVersion("0.1");

        List<AtlasStructDef.AtlasAttributeDef> attributes = definition.getAttributeDefs();

        attributes.add(new AtlasStructDef.AtlasAttributeDef(
                "url",
                "string",
                false,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE,
                1,
                1,
                true,
                true,
                true,
                Collections.EMPTY_LIST));

        return definition;
    }

    public static Collection<? extends AtlasRelationshipDef> atlasRelationshipDefs() {
        return Arrays.asList(
                new AtlasRelationshipDef(
                        "couchbase_cluster_buckets",
                        "",
                        "0.1",
                        "couchbase",
                        AtlasRelationshipDef.RelationshipCategory.AGGREGATION,
                        AtlasRelationshipDef.PropagateTags.ONE_TO_TWO,
                        new AtlasRelationshipEndDef(
                                "couchbase_cluster",
                                "buckets",
                                AtlasStructDef.AtlasAttributeDef.Cardinality.SET,
                                true),
                        new AtlasRelationshipEndDef(
                                "couchbase_bucket",
                                "cluster",
                                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE,
                                false)));
    }

    public String url() {
        return url;
    }

    public CouchbaseCluster url(String url) {
        this.url = url;
        return this;
    }

    @Override
    public AtlasEntity atlasEntity(AtlasClientV2 atlas) {
        AtlasEntity entity = super.atlasEntity(atlas);
        entity.setAttribute("url", url());
        return entity;
    }

    @Override
    protected String qualifiedName() {
        return url();
    }

    @Override
    public String atlasTypeName() {
        return TYPE_NAME;
    }

    @Override
    public UUID id() {
        return UUID.nameUUIDFromBytes(String.format("%s:%s", atlasTypeName(), url()).getBytes(Charset.defaultCharset()));
    }

    @Override
    protected void updateJavaModel(AtlasEntity entity) {
        if (entity.hasAttribute("url")) {
            this.url = (String) entity.getAttribute("url");
        }
    }
}

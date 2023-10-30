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

public class CouchbaseCollection extends CouchbaseAtlasEntity<CouchbaseCollection> {

    private CouchbaseScope scope;

    private long documentsAnalyzed;

    public static AtlasEntityDef atlasEntityDef() {
        AtlasEntityDef definition = AtlasTypeUtil.createClassTypeDef("couchbase_collection", new HashSet<>());

        definition.getSuperTypes().add("DataSet");
        definition.setServiceType("couchbase");
        definition.setTypeVersion("0.1");
        definition.setOption("schemaElementsAttribute", "fields");

        List<AtlasStructDef.AtlasAttributeDef> attributes = definition.getAttributeDefs();

        attributes.add(new AtlasStructDef.AtlasAttributeDef(
                "documentsAnalyzed",
                "long",
                false,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE,
                1,
                1,
                false,
                false,
                false,
                Collections.EMPTY_LIST
        ));

        return definition;
    }

    public static Collection<? extends AtlasRelationshipDef> atlasRelationshipDefs() {
        return Arrays.asList(new AtlasRelationshipDef("couchbase_collection_fields", "", "0.1", "couchbase", AtlasRelationshipDef.RelationshipCategory.AGGREGATION, AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, new AtlasRelationshipEndDef("couchbase_collection", "fields", AtlasStructDef.AtlasAttributeDef.Cardinality.SET, true), new AtlasRelationshipEndDef("couchbase_field", "collection", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, false)));
    }

    public CouchbaseCollection scope(CouchbaseScope scope) {
        this.scope = scope;
        return this;
    }

    @Override
    public AtlasEntity atlasEntity(AtlasClientV2 atlas) {
        AtlasEntity entity = super.atlasEntity(atlas);
        entity.setRelationshipAttribute("scope", scope.atlasEntity(atlas));
        return entity;
    }

    @Override
    protected void updateAtlasEntity(AtlasEntity entity) {
        entity.setAttribute("documentsAnalyzed", documentsAnalyzed);
    }

    @Override
    protected void updateJavaModel(AtlasEntity entity) {
        documentsAnalyzed = (Integer) entity.getAttribute("documentsAnalyzed");
    }

    public long documentsAnalyzed() {
        return documentsAnalyzed;
    }

    public CouchbaseCollection incrementAnalyzedDocuments() {
        this.documentsAnalyzed++;
        return this;
    }

    @Override
    protected String qualifiedName() {
        return String.format("%s/%s", scope.qualifiedName(), name());
    }

    @Override
    public String atlasTypeName() {
        return "couchbase_collection";
    }

    @Override
    public UUID id() {
        return UUID.nameUUIDFromBytes(String.format("%s:%s:%s", atlasTypeName(), scope().id().toString(), name()).getBytes(Charset.defaultCharset()));
    }

    public CouchbaseScope scope() {
        return this.scope;
    }
}

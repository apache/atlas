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

public class CouchbaseField extends CouchbaseAtlasEntity<CouchbaseField> {
    public static final String TYPE_NAME = "couchbase_field";
    private CouchbaseFieldType fieldType;
    private String fieldPath;
    private long documentCount = 0;

    private CouchbaseField parentField;

    private CouchbaseCollection collection;

    public CouchbaseField() {

    }

    public static Collection<? extends AtlasRelationshipDef> atlasRelationshipDefs() {
        return Arrays.asList(
                new AtlasRelationshipDef(
                        "couchbase_field_fields",
                        "",
                        "0.1",
                        "couchbase",
                        AtlasRelationshipDef.RelationshipCategory.AGGREGATION,
                        AtlasRelationshipDef.PropagateTags.ONE_TO_TWO,
                        new AtlasRelationshipEndDef(
                                "couchbase_field",
                                "objectFields",
                                AtlasStructDef.AtlasAttributeDef.Cardinality.SET,
                                true
                        ),
                        new AtlasRelationshipEndDef(
                                "couchbase_field",
                                "parentField",
                                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE,
                                false
                        )
                )
        );
    }

    public CouchbaseFieldType fieldType() {
        return fieldType;
    }

    public CouchbaseField fieldType(CouchbaseFieldType fieldType) {
        this.fieldType = fieldType;
        return this;
    }

    public String fieldPath() {
        return fieldPath;
    }

    public CouchbaseField fieldPath(String fieldPath) {
        this.fieldPath = fieldPath;
        return this;
    }

    public long documentCount() {
        return documentCount;
    }

    public CouchbaseField documentCount(long documentCount) {
        this.documentCount = documentCount;
        return this;
    }

    public void incrementDocumentCount() {
        this.documentCount++;
    }

    public CouchbaseCollection collection() {
        return collection;
    }

    public CouchbaseField collection(CouchbaseCollection collection) {
        this.collection = collection;
        return this;
    }

    public static AtlasEntityDef atlasEntityDef() {
        AtlasEntityDef definition = AtlasTypeUtil.createClassTypeDef(
                TYPE_NAME,
                new HashSet<>()
        );

        definition.setTypeVersion("0.1");
        definition.getSuperTypes().add("DataSet");
        definition.setServiceType("couchbase");
        List<AtlasStructDef.AtlasAttributeDef> attributes = definition.getAttributeDefs();

        attributes.add(new AtlasStructDef.AtlasAttributeDef(
                "fieldType",
                "couchbase_field_type",
                false,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE,
                1,
                1,
                false,
                true,
                false,
                Collections.emptyList()
        ));

        attributes.add(new AtlasStructDef.AtlasAttributeDef(
                "fieldPath",
                "string",
                false,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE,
                1,
                1,
                false,
                true,
                false,
                Collections.emptyList()
        ));

        attributes.add((new AtlasStructDef.AtlasAttributeDef(
                "documentCount",
                "long",
                false,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE,
                1,
                1,
                false,
                false,
                false,
                Collections.emptyList()
        )));

        return definition;
    }

    @Override
    public AtlasEntity atlasEntity(AtlasClientV2 atlas) {
        AtlasEntity entity = super.atlasEntity(atlas);
        entity.setRelationshipAttribute("collection", collection.atlasEntity(atlas));
        if (parentField != null) {
            entity.setRelationshipAttribute("parentField", parentField.atlasEntity(atlas));
        }
        return entity;
    }

    @Override
    protected void updateAtlasEntity(AtlasEntity entity) {
        entity.setAttribute("fieldType", fieldType.toString());
        entity.setAttribute("fieldPath", fieldPath);
        entity.setAttribute("documentCount", documentCount);
    }

    @Override
    protected String qualifiedName() {
        return String.format("%s/%s:%s", collection.qualifiedName(), fieldPath(), fieldType());
    }

    @Override
    public String atlasTypeName() {
        return TYPE_NAME;
    }

    @Override
    public UUID id() {
        return UUID.nameUUIDFromBytes(qualifiedName().getBytes(Charset.defaultCharset()));
    }

    public CouchbaseField parentField() {
        return parentField;
    }

    public CouchbaseField parentField(CouchbaseField parent) {
        this.parentField = parent;
        return this;
    }
}
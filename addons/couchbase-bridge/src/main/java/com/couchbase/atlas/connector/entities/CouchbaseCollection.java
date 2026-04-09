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
import java.util.UUID;

public class CouchbaseCollection extends CouchbaseAtlasEntity<CouchbaseCollection> {
    private CouchbaseScope scope;

    private long documentsAnalyzed;

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

    public CouchbaseScope scope() {
        return this.scope;
    }
}

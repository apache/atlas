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

package com.couchbase.atlas.connector;

import com.couchbase.atlas.connector.entities.CouchbaseBucket;
import com.couchbase.atlas.connector.entities.CouchbaseCluster;
import com.couchbase.atlas.connector.entities.CouchbaseCollection;
import com.couchbase.atlas.connector.entities.CouchbaseField;
import com.couchbase.atlas.connector.entities.CouchbaseFieldType;
import com.couchbase.atlas.connector.entities.CouchbaseScope;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class TypeGenerator {

    private static AtlasClientV2 client;

    public static void main(String[] args) {
        client = AtlasConfig.client();

        AtlasTypesDef types = new AtlasTypesDef();

        types.getEnumDefs().add(CouchbaseFieldType.atlasEnumDef());
        types.getEntityDefs().addAll(Arrays.asList(
                CouchbaseField.atlasEntityDef(),
                CouchbaseCollection.atlasEntityDef(),
                CouchbaseScope.atlasEntityDef(),
                CouchbaseBucket.atlasEntityDef(),
                CouchbaseCluster.atlasEntityDef()
        ));

        types.getRelationshipDefs().addAll(CouchbaseField.atlasRelationshipDefs());
        types.getRelationshipDefs().addAll(CouchbaseCollection.atlasRelationshipDefs());
        types.getRelationshipDefs().addAll(CouchbaseScope.atlasRelationshipDefs());
        types.getRelationshipDefs().addAll(CouchbaseBucket.atlasRelationshipDefs());
        types.getRelationshipDefs().addAll(CouchbaseCluster.atlasRelationshipDefs());
        recreate(types);
    }

    private static void recreate(AtlasTypesDef atd) {
        try {
            client.createAtlasTypeDefs(atd);
        } catch (AtlasServiceException e) {
            try {
                client.updateAtlasTypeDefs(atd);
            } catch (AtlasServiceException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private static boolean exists(AtlasRelationshipDef atlasRelationshipDef) {
        try {
            return client.getRelationshipDefByName(atlasRelationshipDef.getName()) != null;
        } catch (AtlasServiceException e) {
            if (e.getMessage().contains("404")) {
                return false;
            }
            throw new RuntimeException("Failed to check if relationship definition " + atlasRelationshipDef.getName() + " exists", e);
        }
    }

    private static boolean exists(AtlasEntityDef atlasEntityDef) {
        try {
            return client.getEntityDefByName(atlasEntityDef.getName()) != null;
        } catch (AtlasServiceException e) {
            if (e.getMessage().contains("404")) {
                return false;
            }
            throw new RuntimeException("Failed to check if entity type definition " + atlasEntityDef.getName() + " exists", e);
        }
    }

    private static boolean exists(AtlasEnumDef atlasEnumDef) {
        try {
            return client.getEnumDefByName(atlasEnumDef.getName()) != null;
        } catch (AtlasServiceException e) {
            if (e.getMessage().contains("404")) {
                return false;
            }
            throw new RuntimeException("Failed to check if enum definition " + atlasEnumDef.getName() + " exists", e);
        }
    }

    private static void deleteExisting(AtlasTypesDef atd) {
        AtlasTypesDef existing = new AtlasTypesDef();
        atd.getRelationshipDefs().stream().filter(TypeGenerator::exists).map(AtlasBaseTypeDef::getName).forEach(typeName -> {
            try {
                client.deleteRelationshipByGuid(client.getRelationshipDefByName(typeName).getGuid());
            } catch (AtlasServiceException e) {
                throw new RuntimeException(e);
            }
        });

        existing.setRelationshipDefs(Collections.EMPTY_LIST);
        existing.setEnumDefs(atd.getEnumDefs().stream().filter(TypeGenerator::exists).collect(Collectors.toList()));
        existing.setEntityDefs(atd.getEntityDefs().stream().filter(TypeGenerator::exists).collect(Collectors.toList()));

        try {
            client.deleteAtlasTypeDefs(existing);
        } catch (AtlasServiceException e) {
            if (!e.getMessage().contains("404")) {
                throw new RuntimeException("Failed to delete " + atd.toString(), e);
            }
        }
    }
}

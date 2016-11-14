/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.adapters;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Singleton
public class AtlasInstanceRestAdapters {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasInstanceRestAdapters.class);

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasFormatConverters instanceFormatters;

    @Inject
    private MetadataService metadataService;

    public ITypedReferenceableInstance[] getITypedReferenceables(List<AtlasEntity> entities) throws AtlasBaseException {
        ITypedReferenceableInstance[] entitiesInOldFormat = new ITypedReferenceableInstance[entities.size()];

        for (int i = 0; i < entities.size(); i++) {
            ITypedReferenceableInstance typedInstance = getITypedReferenceable(entities.get(i));
            entitiesInOldFormat[i] = typedInstance;
        }

        return entitiesInOldFormat;
    }

    public ITypedReferenceableInstance getITypedReferenceable(AtlasEntity entity) throws AtlasBaseException {
        AtlasFormatAdapter entityFormatter = instanceFormatters.getConverter(AtlasFormatConverters.VERSION_V2, AtlasFormatConverters.VERSION_V1, TypeCategory.ENTITY);
        AtlasType entityType = typeRegistry.getType(entity.getTypeName());

        Referenceable ref = (Referenceable) entityFormatter.convert(AtlasFormatConverters.VERSION_V2, AtlasFormatConverters.VERSION_V1, entityType, entity);
        try {
            return metadataService.getTypedReferenceableInstance(ref);
        } catch (AtlasException e) {
            LOG.error("Exception while getting a typed reference for the entity ", e);
            throw toAtlasBaseException(e);
        }
    }

    public Referenceable getReferenceable(AtlasEntity entity) throws AtlasBaseException {
        AtlasFormatAdapter entityFormatter = instanceFormatters.getConverter(AtlasFormatConverters.VERSION_V2, AtlasFormatConverters.VERSION_V1, TypeCategory.ENTITY);
        AtlasType entityType = typeRegistry.getType(entity.getTypeName());

        return (Referenceable) entityFormatter.convert(AtlasFormatConverters.VERSION_V2, AtlasFormatConverters.VERSION_V1, entityType, entity);
    }

    public ITypedStruct getTrait(AtlasClassification classification) throws AtlasBaseException {
        AtlasFormatAdapter formatter = instanceFormatters.getConverter(AtlasFormatConverters.VERSION_V2, AtlasFormatConverters.VERSION_V1, TypeCategory.CLASSIFICATION);
        AtlasType clsType = typeRegistry.getType(classification.getTypeName());

        Struct ref = (Struct) formatter.convert(AtlasFormatConverters.VERSION_V2, AtlasFormatConverters.VERSION_V1, clsType, classification);
        try {
            return metadataService.createTraitInstance(ref);
        } catch (AtlasException e) {
            LOG.error("Exception while getting a typed reference for the entity ", e);
            throw toAtlasBaseException(e);
        }
    }

    public AtlasClassification getClassification(IStruct classification) throws AtlasBaseException {
        AtlasFormatAdapter formatter = instanceFormatters.getConverter(AtlasFormatConverters.VERSION_V1, AtlasFormatConverters.VERSION_V2, TypeCategory.CLASSIFICATION);
        AtlasType clsType = typeRegistry.getType(classification.getTypeName());

        return (AtlasClassification) formatter.convert(AtlasFormatConverters.VERSION_V1, AtlasFormatConverters.VERSION_V2, clsType, classification);
    }

    public AtlasEntityWithAssociations getAtlasEntity(IReferenceableInstance referenceable) throws AtlasBaseException {
        AtlasFormatAdapter entityFormatter = instanceFormatters.getConverter(AtlasFormatConverters.VERSION_V1, AtlasFormatConverters.VERSION_V2, TypeCategory.ENTITY);
        AtlasType entityType = typeRegistry.getType(referenceable.getTypeName());

        return (AtlasEntityWithAssociations) entityFormatter.convert(AtlasFormatConverters.VERSION_V1, AtlasFormatConverters.VERSION_V2, entityType, referenceable);
    }


    public static EntityMutationResponse toEntityMutationResponse(AtlasClient.EntityResult result) {
        EntityMutationResponse response = new EntityMutationResponse();
        for (String guid : result.getCreatedEntities()) {
            AtlasEntityHeader header = new AtlasEntityHeader();
            header.setGuid(guid);
            response.addEntity(EntityMutations.EntityOperation.CREATE_OR_UPDATE, header);
        }

        for (String guid : result.getUpdateEntities()) {
            AtlasEntityHeader header = new AtlasEntityHeader();
            header.setGuid(guid);
            response.addEntity(EntityMutations.EntityOperation.CREATE_OR_UPDATE, header);
        }

        for (String guid : result.getDeletedEntities()) {
            AtlasEntityHeader header = new AtlasEntityHeader();
            header.setGuid(guid);
            response.addEntity(EntityMutations.EntityOperation.DELETE, header);
        }
        return response;
    }

    public static AtlasBaseException toAtlasBaseException(AtlasException e) {
        if ( e instanceof EntityNotFoundException) {
            return new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, e);
        }

        if ( e instanceof TypeNotFoundException) {
            return new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, e);
        }

        return new AtlasBaseException(e);
    }

}

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
package org.apache.atlas.repository.converters;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasClient.EntityResult;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.CreateUpdateEntitiesResult;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.instance.GuidMapping;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TraitNotFoundException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.repository.converters.AtlasFormatConverter.ConverterContext;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Singleton
public class AtlasInstanceConverter {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasInstanceConverter.class);

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasFormatConverters instanceFormatters;

    @Inject
    private MetadataService metadataService;

    public ITypedReferenceableInstance[] getITypedReferenceables(Collection<AtlasEntity> entities) throws AtlasBaseException {
        ITypedReferenceableInstance[] entitiesInOldFormat = new ITypedReferenceableInstance[entities.size()];

        AtlasFormatConverter.ConverterContext ctx = new AtlasFormatConverter.ConverterContext();
        for(Iterator<AtlasEntity> i = entities.iterator(); i.hasNext(); ) {
            ctx.addEntity(i.next());
        }

        Iterator<AtlasEntity> entityIterator = entities.iterator();
        for (int i = 0; i < entities.size(); i++) {
            ITypedReferenceableInstance typedInstance = getITypedReferenceable(entityIterator.next());
            entitiesInOldFormat[i] = typedInstance;
        }
        return entitiesInOldFormat;
    }

    public ITypedReferenceableInstance getITypedReferenceable(AtlasEntity entity) throws AtlasBaseException {
        try {
            return metadataService.getEntityDefinition(entity.getGuid());
        } catch (AtlasException e) {
            LOG.error("Exception while getting a typed reference for the entity ", e);
            throw toAtlasBaseException(e);
        }
    }

    public ITypedReferenceableInstance getITypedReferenceable(String guid) throws AtlasBaseException {
        try {
            return metadataService.getEntityDefinition(guid);
        } catch (AtlasException e) {
            LOG.error("Exception while getting a typed reference for the entity ", e);
            throw toAtlasBaseException(e);
        }
    }

    public Referenceable getReferenceable(AtlasEntity entity, final ConverterContext ctx) throws AtlasBaseException {
        AtlasFormatConverter converter  = instanceFormatters.getConverter(TypeCategory.ENTITY);
        AtlasType            entityType = typeRegistry.getType(entity.getTypeName());
        Referenceable        ref        = (Referenceable) converter.fromV2ToV1(entity, entityType, ctx);

        return ref;
    }

    public ITypedStruct getTrait(AtlasClassification classification) throws AtlasBaseException {
        AtlasFormatConverter converter          = instanceFormatters.getConverter(TypeCategory.CLASSIFICATION);
        AtlasType            classificationType = typeRegistry.getType(classification.getTypeName());
        Struct               trait               = (Struct)converter.fromV2ToV1(classification, classificationType, new ConverterContext());

        try {
            return metadataService.createTraitInstance(trait);
        } catch (AtlasException e) {
            LOG.error("Exception while getting a typed reference for the entity ", e);
            throw toAtlasBaseException(e);
        }
    }

    public AtlasClassification getClassification(IStruct classification) throws AtlasBaseException {
        AtlasFormatConverter converter          = instanceFormatters.getConverter(TypeCategory.CLASSIFICATION);
        AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(classification.getTypeName());
        if (classificationType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.CLASSIFICATION.name(), classification.getTypeName());
        }
        AtlasClassification  ret                = (AtlasClassification)converter.fromV1ToV2(classification, classificationType, new AtlasFormatConverter.ConverterContext());

        return ret;
    }

    public AtlasEntitiesWithExtInfo toAtlasEntity(IReferenceableInstance referenceable) throws AtlasBaseException {
        AtlasEntityFormatConverter converter  = (AtlasEntityFormatConverter) instanceFormatters.getConverter(TypeCategory.ENTITY);
        AtlasEntityType            entityType = typeRegistry.getEntityTypeByName(referenceable.getTypeName());

        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), referenceable.getTypeName());
        }

        // validate
        try {
            metadataService.validateAndConvertToTypedInstance(referenceable, entityType.getTypeName());
        } catch (AtlasException excp) {
            throw toAtlasBaseException(excp);
        }

        ConverterContext ctx    = new ConverterContext();
        AtlasEntity      entity = converter.fromV1ToV2(referenceable, entityType, ctx);

        ctx.addEntity(entity);

        return ctx.getEntities();
    }

    public static EntityMutationResponse toEntityMutationResponse(AtlasClient.EntityResult entityResult) {

        CreateUpdateEntitiesResult result = new CreateUpdateEntitiesResult();
        result.setEntityResult(entityResult);
        return toEntityMutationResponse(result);
    }

    public static EntityMutationResponse toEntityMutationResponse(CreateUpdateEntitiesResult result) {
        EntityMutationResponse response = new EntityMutationResponse();
        for (String guid : result.getCreatedEntities()) {
            AtlasEntityHeader header = new AtlasEntityHeader();
            header.setGuid(guid);
            response.addEntity(EntityMutations.EntityOperation.CREATE, header);
        }

        for (String guid : result.getUpdatedEntities()) {
            AtlasEntityHeader header = new AtlasEntityHeader();
            header.setGuid(guid);
            response.addEntity(EntityMutations.EntityOperation.UPDATE, header);
        }

        for (String guid : result.getDeletedEntities()) {
            AtlasEntityHeader header = new AtlasEntityHeader();
            header.setGuid(guid);
            response.addEntity(EntityMutations.EntityOperation.DELETE, header);
        }
        GuidMapping guidMapping = result.getGuidMapping();
        if(guidMapping != null) {
            response.setGuidAssignments(guidMapping.getGuidAssignments());
        }
        return response;
    }

    public static AtlasBaseException toAtlasBaseException(AtlasException e) {
        if ( e instanceof EntityNotFoundException || e instanceof TraitNotFoundException) {
            return new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, e);
        }

        if ( e instanceof TypeNotFoundException) {
            return new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, e);
        }

        if (e instanceof ValueConversionException) {
            return new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, e, e.getMessage());
        }

        return new AtlasBaseException(e);
    }


    public AtlasEntity.AtlasEntitiesWithExtInfo toAtlasEntities(List<Referenceable> referenceables) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> toAtlasEntities");
        }

        AtlasFormatConverter.ConverterContext context = new AtlasFormatConverter.ConverterContext();
        for (Referenceable referenceable : referenceables) {
            AtlasEntity entity = fromV1toV2Entity(referenceable, context);

            context.addEntity(entity);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== toAtlasEntities");
        }

        return context.getEntities();
    }

    public AtlasEntitiesWithExtInfo toAtlasEntities(String entitiesJson) throws AtlasBaseException, AtlasException {
        ITypedReferenceableInstance[] referenceables = metadataService.deserializeClassInstances(entitiesJson);
        AtlasEntityFormatConverter    converter      = (AtlasEntityFormatConverter) instanceFormatters.getConverter(TypeCategory.ENTITY);
        ConverterContext              context        = new ConverterContext();
        AtlasEntitiesWithExtInfo      ret            = null;

        if (referenceables != null) {
            for (IReferenceableInstance referenceable : referenceables) {
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(referenceable.getTypeName());

                if (entityType == null) {
                    throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), referenceable.getTypeName());
                }

                AtlasEntity entity = converter.fromV1ToV2(referenceable, entityType, context);

                context.addEntity(entity);
            }

            ret = context.getEntities();
        }

        return ret;
    }

    private AtlasEntity fromV1toV2Entity(Referenceable referenceable, AtlasFormatConverter.ConverterContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> fromV1toV2Entity");
        }

        AtlasEntityFormatConverter converter = (AtlasEntityFormatConverter) instanceFormatters.getConverter(TypeCategory.ENTITY);

        AtlasEntity entity = converter.fromV1ToV2(referenceable, typeRegistry.getType(referenceable.getTypeName()), context);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== fromV1toV2Entity");
        }
        return entity;
    }

    public CreateUpdateEntitiesResult toCreateUpdateEntitiesResult(EntityMutationResponse reponse) {
        CreateUpdateEntitiesResult ret = null;

        if (reponse != null) {
            Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = reponse.getMutatedEntities();
            Map<String, String>                           guidAssignments = reponse.getGuidAssignments();

            ret = new CreateUpdateEntitiesResult();

            if (MapUtils.isNotEmpty(guidAssignments)) {
                ret.setGuidMapping(new GuidMapping(guidAssignments));
            }

            if (MapUtils.isNotEmpty(mutatedEntities)) {
                EntityResult entityResult = new EntityResult();

                for (Map.Entry<EntityOperation, List<AtlasEntityHeader>> e : mutatedEntities.entrySet()) {
                    switch (e.getKey()) {
                        case CREATE:
                            List<AtlasEntityHeader> createdEntities = mutatedEntities.get(EntityOperation.CREATE);
                            if (CollectionUtils.isNotEmpty(createdEntities)) {
                                Collections.reverse(createdEntities);
                                entityResult.set(EntityResult.OP_CREATED, getGuids(createdEntities));
                            }
                            break;
                        case UPDATE:
                            List<AtlasEntityHeader> updatedEntities = mutatedEntities.get(EntityOperation.UPDATE);
                            if (CollectionUtils.isNotEmpty(updatedEntities)) {
                                Collections.reverse(updatedEntities);
                                entityResult.set(EntityResult.OP_UPDATED, getGuids(updatedEntities));
                            }
                            break;
                        case PARTIAL_UPDATE:
                            List<AtlasEntityHeader> partialUpdatedEntities = mutatedEntities.get(EntityOperation.PARTIAL_UPDATE);
                            if (CollectionUtils.isNotEmpty(partialUpdatedEntities)) {
                                Collections.reverse(partialUpdatedEntities);
                                entityResult.set(EntityResult.OP_UPDATED, getGuids(partialUpdatedEntities));
                            }
                            break;
                        case DELETE:
                            List<AtlasEntityHeader> deletedEntities = mutatedEntities.get(EntityOperation.DELETE);
                            if (CollectionUtils.isNotEmpty(deletedEntities)) {
                                Collections.reverse(deletedEntities);
                                entityResult.set(EntityResult.OP_DELETED, getGuids(deletedEntities));
                            }
                            break;
                    }

                }

                ret.setEntityResult(entityResult);
            }
        }

        return ret;
    }

    public List<String> getGuids(List<AtlasEntityHeader> entities) {
        List<String> ret = null;

        if (CollectionUtils.isNotEmpty(entities)) {
            ret = new ArrayList<>();
            for (AtlasEntityHeader entity : entities) {
                ret.add(entity.getGuid());
            }
        }

        return ret;
    }
}

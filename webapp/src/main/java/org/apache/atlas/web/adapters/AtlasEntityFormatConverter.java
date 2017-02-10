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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.Id.EntityState;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AtlasEntityFormatConverter extends AtlasStructFormatConverter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityFormatConverter.class);

    public AtlasEntityFormatConverter(AtlasFormatConverters registry, AtlasTypeRegistry typeRegistry) {
        super(registry, typeRegistry, TypeCategory.ENTITY);
    }

    @Override
    public Object fromV1ToV2(Object v1Obj, AtlasType type, ConverterContext context) throws AtlasBaseException {
        AtlasObjectId ret = null;

        if (v1Obj != null) {
            AtlasEntityType entityType = (AtlasEntityType) type;

            if (v1Obj instanceof Id) {
                Id id = (Id) v1Obj;

                ret = new AtlasObjectId(id._getId(), id.getTypeName());
            } else if (v1Obj instanceof IReferenceableInstance) {
                IReferenceableInstance entRef = (IReferenceableInstance) v1Obj;

                ret = new AtlasObjectId(entRef.getId()._getId(), entRef.getTypeName());

                if (!context.entityExists(ret.getGuid())) {
                    Map<String, Object> v1Attribs = null;

                    try {
                        v1Attribs = entRef.getValuesMap();
                    } catch (AtlasException excp) {
                        LOG.error("IReferenceableInstance.getValuesMap() failed", excp);
                    }

                    AtlasEntity entity = new AtlasEntity(entRef.getTypeName(),
                                                         super.fromV1ToV2(entityType, v1Attribs, context));
                    entity.setGuid(entRef.getId()._getId());
                    entity.setStatus(convertState(entRef.getId().getState()));
                    entity.setCreatedBy(entRef.getSystemAttributes().createdBy);
                    entity.setCreateTime(entRef.getSystemAttributes().createdTime);
                    entity.setUpdatedBy(entRef.getSystemAttributes().modifiedBy);
                    entity.setUpdateTime(entRef.getSystemAttributes().modifiedTime);
                    entity.setVersion(new Long(entRef.getId().version));

                    if (CollectionUtils.isNotEmpty(entRef.getTraits())) {
                        List<AtlasClassification> classifications = new ArrayList<>();
                        AtlasFormatConverter traitConverter = converterRegistry.getConverter(TypeCategory.CLASSIFICATION);

                        for (String traitName : entRef.getTraits()) {
                            IStruct trait = entRef.getTrait(traitName);
                            AtlasType classifiType = typeRegistry.getType(traitName);
                            AtlasClassification classification = (AtlasClassification) traitConverter.fromV1ToV2(trait, classifiType, context);

                            classifications.add(classification);
                        }

                        entity.setClassifications(classifications);
                    }

                    context.addEntity(entity);
                }
            } else {
                throw new AtlasBaseException(AtlasErrorCode.UNEXPECTED_TYPE, "IReferenceableInstance",
                                             v1Obj.getClass().getCanonicalName());
            }
        }
        return ret;
    }

    private AtlasEntity.Status convertState(EntityState state){
        Status status = Status.ACTIVE;
        if(state != null && state.equals(EntityState.DELETED)){
            status = Status.DELETED;
        }
        LOG.debug("Setting state to {}", state);
        return status;
    }

    @Override
    public Object fromV2ToV1(Object v2Obj, AtlasType type, ConverterContext context) throws AtlasBaseException {
        Object ret = null;

        if (v2Obj != null) {
            AtlasEntityType entityType = (AtlasEntityType) type;

            if (v2Obj instanceof Map) {
                Map    v2Map    = (Map) v2Obj;
                String idStr    = (String)v2Map.get(AtlasObjectId.KEY_GUID);
                String typeName = type.getTypeName();

                if (StringUtils.isEmpty(idStr)) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND);
                }

                final Map v2Attribs = (Map) v2Map.get(ATTRIBUTES_PROPERTY_KEY);

                if (MapUtils.isEmpty(v2Attribs)) {
                    ret = new Id(idStr, 0, typeName);
                } else {
                    ret = new Referenceable(idStr, typeName, super.fromV2ToV1(entityType, v2Attribs, context));
                }
            } else if (v2Obj instanceof AtlasEntity) {
                AtlasEntity entity = (AtlasEntity) v2Obj;

                ret = new Referenceable(entity.getGuid(), entity.getTypeName(),
                                        fromV2ToV1(entityType, entity.getAttributes(), context));

            } else if (v2Obj instanceof AtlasObjectId) { // transient-id
                AtlasEntity entity = context.getById(((AtlasObjectId) v2Obj).getGuid());
                if ( entity == null) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Could not find entity ",
                        v2Obj.toString());
                }
                ret = this.fromV2ToV1(entity, typeRegistry.getType(((AtlasObjectId) v2Obj).getTypeName()), context);
            } else {
                throw new AtlasBaseException(AtlasErrorCode.UNEXPECTED_TYPE, "Map or AtlasEntity or String",
                                             v2Obj.getClass().getCanonicalName());
            }
        }

        return ret;
    }
}

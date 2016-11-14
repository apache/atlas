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
package org.apache.atlas.web.adapters.v1;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.web.adapters.AtlasFormatAdapter;
import org.apache.atlas.web.adapters.AtlasFormatConverters;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class ReferenceableToAtlasEntityConverter implements AtlasFormatAdapter {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    @Inject
    public ReferenceableToAtlasEntityConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this, AtlasFormatConverters.VERSION_V1, AtlasFormatConverters.VERSION_V2);
    }

    @Override
    public Object convert(final String sourceVersion, final String targetVersion, final AtlasType type, final Object source) throws AtlasBaseException {
        AtlasEntityWithAssociations result = null;
        if ( source != null) {
            if ( isId(source)) {
                Id idObj = (Id) source;
                result = new AtlasEntityWithAssociations(idObj.getTypeName());
                setId(idObj, result);
            } else if (isEntityType(source) ) {

                IReferenceableInstance entity = (IReferenceableInstance) source;

                //Resolve attributes
                StructToAtlasStructConverter converter = (StructToAtlasStructConverter) registry.getConverter(sourceVersion, targetVersion, TypeCategory.STRUCT);
                result =  new AtlasEntityWithAssociations(entity.getTypeName(), converter.convertAttributes((AtlasEntityType) type, entity));

                //Id
                setId(entity, result);

                //Resolve traits
                List<AtlasClassification> classifications = new ArrayList<>();
                for (String traitName : entity.getTraits()) {
                    IStruct trait = entity.getTrait(traitName);
                    AtlasClassificationType traitType = (AtlasClassificationType) typeRegistry.getType(traitName);
                    AtlasClassification clsInstance =  new AtlasClassification(traitType.getTypeName(), converter.convertAttributes(traitType, trait));
                    classifications.add(clsInstance);
                }
                result.setClassifications(classifications);
            }
        }
        return result;
    }

    private boolean isEntityType(Object o) {
        if ( o != null && o instanceof IReferenceableInstance) {
            return true;
        }
        return false;
    }

    private boolean isId(Object o) {
        if ( o != null && o instanceof Id) {
            return true;
        }
        return false;
    }

    @Override
    public TypeCategory getTypeCategory() {
        return TypeCategory.ENTITY;
    }


    private void setId(IReferenceableInstance entity, AtlasEntity result) {
        result.setGuid(entity.getId()._getId());
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.ogm;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.AtlasBaseModelObject;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang3.StringUtils;


public abstract class AbstractDataTransferObject<T extends AtlasBaseModelObject> implements DataTransferObject<T> {
    private final AtlasTypeRegistry typeRegistry;
    private final Class<T>          objectType;
    private final String            entityTypeName;

    protected AbstractDataTransferObject(AtlasTypeRegistry typeRegistry, Class<T> tClass) {
        this.typeRegistry   = typeRegistry;
        this.objectType     = tClass;
        this.entityTypeName = Constants.INTERNAL_PROPERTY_KEY_PREFIX + objectType.getSimpleName();
    }

    @Override
    public Class getObjectType() {
        return objectType;
    }

    @Override
    public AtlasEntityType getEntityType() {
        return typeRegistry.getEntityTypeByName(entityTypeName);
    }


    protected AtlasEntity getDefaultAtlasEntity(T obj) throws AtlasBaseException {
        AtlasEntity ret = getEntityType().createDefaultValue();

        if (obj != null) {
            if (StringUtils.isNotEmpty(obj.getGuid())) {
                ret.setGuid(obj.getGuid());
            }
        }

        return ret;
    }

    public void setGuid(T o, AtlasEntity entity) {
        o.setGuid(entity.getGuid());
    }
}

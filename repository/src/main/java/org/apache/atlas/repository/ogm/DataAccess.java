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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.AtlasBaseModelObject;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStream;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import javax.inject.Inject;


@Component
public class DataAccess {
    private final AtlasEntityStore entityStore;
    private final DTORegistry      dtoRegistry;

    @Inject
    public DataAccess(AtlasEntityStore entityStore, DTORegistry dtoRegistry) {
        this.entityStore = entityStore;
        this.dtoRegistry = dtoRegistry;
    }

    public <T extends AtlasBaseModelObject> T save(T obj) throws AtlasBaseException {
        DataTransferObject<T> dto = (DataTransferObject<T>)dtoRegistry.get(obj.getClass());

        AtlasEntityWithExtInfo entityWithExtInfo      = dto.toEntityWithExtInfo(obj);
        EntityMutationResponse entityMutationResponse = entityStore.createOrUpdate(new AtlasEntityStream(entityWithExtInfo), false);

        if (hasError(entityMutationResponse)) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_ACCESS_SAVE_FAILED, obj.toString());
        }

        return obj;
    }

    public <T extends AtlasBaseModelObject> T load(T obj) throws AtlasBaseException {
        DataTransferObject<T>  dto = (DataTransferObject<T>)dtoRegistry.get(obj.getClass());

        AtlasEntityWithExtInfo entityWithExtInfo;

        if (StringUtils.isNotEmpty(obj.getGuid())) {
            entityWithExtInfo = entityStore.getById(obj.getGuid());
        } else {
            entityWithExtInfo = entityStore.getByUniqueAttributes(dto.getEntityType(), dto.getUniqueAttributes(obj));
        }

        return dto.from(entityWithExtInfo);
    }

    public void deleteUsingGuid(String guid) throws AtlasBaseException {
        entityStore.deleteById(guid);
    }

    public <T extends AtlasBaseModelObject> void delete(T obj) throws AtlasBaseException {
        T object = load(obj);

        if (object != null) {
            deleteUsingGuid(object.getGuid());
        }
    }

    private boolean hasError(EntityMutationResponse er) {
        return (er == null ||
                !((er.getCreatedEntities() != null && er.getCreatedEntities().size() > 0)
                        || (er.getUpdatedEntities() != null && er.getUpdatedEntities().size() > 0)
                )
        );
    }
}

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

import org.apache.atlas.model.AtlasBaseModelObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
public class DTORegistry {
    private static final Logger LOG = LoggerFactory.getLogger(DTORegistry.class);

    private final Map<Class<? extends AtlasBaseModelObject>, DataTransferObject<? extends AtlasBaseModelObject>> typeDTOMap = new HashMap<>();

    @Inject
    public DTORegistry(Set<DataTransferObject> availableDTOs) {
        for (DataTransferObject<? extends AtlasBaseModelObject> availableDTO : availableDTOs) {
            LOG.info("Registering DTO: {}", availableDTO.getClass().getSimpleName());

            registerDTO(availableDTO);
        }
    }

    public <T extends AtlasBaseModelObject> DataTransferObject<T> get(Class<T> t) {
        return (DataTransferObject<T>) typeDTOMap.get(t);
    }

    private <T extends AtlasBaseModelObject> void registerDTO(DataTransferObject<T> dto) {
        typeDTOMap.put(dto.getObjectType(), dto);
    }
}

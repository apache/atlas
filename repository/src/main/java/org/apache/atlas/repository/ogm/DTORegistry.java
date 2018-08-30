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
package org.apache.atlas.repository.ogm;

import org.apache.atlas.type.AtlasTypeRegistry;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

@Component
public class DTORegistry {
    private final Map<Type, DataTransferObject> typeDTOMap = new HashMap<>();


    @Inject
    public DTORegistry(AtlasTypeRegistry typeRegistry) {
        AtlasSavedSearchDTO savedSearchDTO = new AtlasSavedSearchDTO(typeRegistry);
        AtlasUserProfileDTO userProfileDTO = new AtlasUserProfileDTO(typeRegistry, savedSearchDTO);

        registerDTO(savedSearchDTO);
        registerDTO(userProfileDTO);
        registerDTO(new AtlasServerDTO(typeRegistry));
        registerDTO(new ExportImportAuditEntryDTO(typeRegistry));
    }

    public <T extends DataTransferObject> DataTransferObject get(Type t) {
        return typeDTOMap.get(t);
    }

    private void registerDTO(DataTransferObject dto) {
        typeDTOMap.put(dto.getObjectType(), dto);
    }
}

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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.migration.MigrationImportStatus;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Component
public class MigrationImportStatusDTO extends AbstractDataTransferObject<MigrationImportStatus> {
    public static final String PROPERTY_NAME = "name";
    public static final String PROPERTY_SIZE = "size";
    public static final String PROPERTY_POSITION = "position";
    public static final String PROPERTY_START_TIME = "startTime";
    public static final String PROPERTY_END_TIME = "endTime";
    public static final String PROPERTY_ADDITIONAL_INFO = "additionalInfo";

    private static final Set<String> ATTRIBUTE_NAMES = new HashSet<>(Arrays.asList(PROPERTY_NAME,
            PROPERTY_SIZE, PROPERTY_POSITION,
            PROPERTY_START_TIME, PROPERTY_END_TIME,
            PROPERTY_ADDITIONAL_INFO));

    @Inject
    public MigrationImportStatusDTO(AtlasTypeRegistry typeRegistry) {
        super(typeRegistry, MigrationImportStatus.class, Constants.INTERNAL_PROPERTY_KEY_PREFIX + MigrationImportStatus.class.getSimpleName());
    }

    public static Set<String> getAttributes() {
        return ATTRIBUTE_NAMES;
    }

    public static MigrationImportStatus from(String guid, Map<String,Object> attributes) {
        MigrationImportStatus entry = new MigrationImportStatus();

        entry.setGuid(guid);
        entry.setName((String) attributes.get(PROPERTY_NAME));
        entry.setSize((int) attributes.get(PROPERTY_SIZE));
        entry.setPosition((String) attributes.get(PROPERTY_POSITION));
        entry.setStartTime((long) attributes.get(PROPERTY_START_TIME));
        entry.setEndTime((long) attributes.get(PROPERTY_END_TIME));

        return entry;
    }

    @Override
    public MigrationImportStatus from(AtlasEntity entity) {
        return from(entity.getGuid(), entity.getAttributes());
    }

    @Override
    public MigrationImportStatus from(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        return from(entityWithExtInfo.getEntity());
    }

    @Override
    public AtlasEntity toEntity(MigrationImportStatus obj) {
        AtlasEntity entity = getDefaultAtlasEntity(obj);

        entity.setAttribute(PROPERTY_NAME, obj.getName());
        entity.setAttribute(PROPERTY_SIZE, obj.getSize());
        entity.setAttribute(PROPERTY_POSITION, obj.getPosition());
        entity.setAttribute(PROPERTY_START_TIME, obj.getStartTime());
        entity.setAttribute(PROPERTY_END_TIME, obj.getEndTime());

        return entity;
    }

    @Override
    public AtlasEntity.AtlasEntityWithExtInfo toEntityWithExtInfo(MigrationImportStatus obj) throws AtlasBaseException {
        return new AtlasEntity.AtlasEntityWithExtInfo(toEntity(obj));
    }

    @Override
    public Map<String, Object> getUniqueAttributes(final MigrationImportStatus obj) {
        return Collections.singletonMap(PROPERTY_NAME, obj.getName());
    }
}

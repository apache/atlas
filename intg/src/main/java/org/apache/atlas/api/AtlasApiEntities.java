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
package org.apache.atlas.api;

import java.util.List;
import java.util.Map;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntities;
import org.apache.atlas.model.instance.AtlasObjectId;

/**
 * API to work with CRUD of Atlas entities.
 */
public interface AtlasApiEntities {
    AtlasEntity createEntity(AtlasEntity entity) throws AtlasBaseException;

    AtlasEntity getEntity(AtlasObjectId objId) throws AtlasBaseException;

    AtlasEntity updateEntity(AtlasObjectId objId, AtlasEntity entity) throws AtlasBaseException;

    AtlasEntity updateEntityAttributes(AtlasObjectId objId, Map<String, Object> attributes) throws AtlasBaseException;

    void deleteEntity(AtlasObjectId objId) throws AtlasBaseException;

    AtlasEntities searchEntities(SearchFilter filter) throws AtlasBaseException;


    void addEntityClassification(AtlasObjectId entityId, AtlasClassification classification) throws AtlasBaseException;

    void removeEntityClassification(AtlasObjectId entityId, String classificationName) throws AtlasBaseException;

    List<AtlasClassification> getEntityClassifications(AtlasObjectId entityId) throws AtlasBaseException;

    List<String> getEntityClassificationNames(AtlasObjectId entityId) throws AtlasBaseException;
}

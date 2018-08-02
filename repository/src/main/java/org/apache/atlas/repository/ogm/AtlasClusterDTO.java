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
import org.apache.atlas.model.clusterinfo.AtlasCluster;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.type.AtlasTypeRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AtlasClusterDTO extends AbstractDataTransferObject<AtlasCluster> {
    private final String PROPERTY_CLUSTER_NAME = "displayName";
    private final String PROPERTY_QUALIFIED_NAME = "qualifiedName";
    private final String PROPERTY_ADDITIONAL_INFO = "additionalInfo";
    private final String PROPERTY_URLS = "urls";

    public AtlasClusterDTO(AtlasTypeRegistry typeRegistry) {
        super(typeRegistry, AtlasCluster.class, AtlasCluster.class.getSimpleName());
    }

    public AtlasCluster from(AtlasEntity entity) {
        AtlasCluster cluster = new AtlasCluster();

        setGuid(cluster, entity);
        cluster.setName((String) entity.getAttribute(PROPERTY_CLUSTER_NAME));
        cluster.setQualifiedName((String) entity.getAttribute(PROPERTY_QUALIFIED_NAME));
        cluster.setAdditionalInfo((Map<String,String>) entity.getAttribute(PROPERTY_ADDITIONAL_INFO));
        cluster.setUrls((List<String>) entity.getAttribute(PROPERTY_URLS));

        return cluster;
    }

    public AtlasCluster from(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        return from(entityWithExtInfo.getEntity());
    }

    @Override
    public AtlasEntity toEntity(AtlasCluster obj) {
        AtlasEntity entity = getDefaultAtlasEntity(obj);

        entity.setAttribute(PROPERTY_CLUSTER_NAME, obj.getName());
        entity.setAttribute(PROPERTY_QUALIFIED_NAME, obj.getQualifiedName());
        entity.setAttribute(PROPERTY_ADDITIONAL_INFO, obj.getAdditionalInfo());

        return entity;
    }

    @Override
    public AtlasEntity.AtlasEntityWithExtInfo toEntityWithExtInfo(AtlasCluster obj) {
        return new AtlasEntity.AtlasEntityWithExtInfo(toEntity(obj));
    }

    @Override
    public Map<String, Object> getUniqueAttributes(final AtlasCluster obj) {
        return new HashMap<String, Object>() {{
            put(PROPERTY_QUALIFIED_NAME, obj.getQualifiedName());
        }};
    }
}

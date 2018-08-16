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

package org.apache.atlas.repository.impexp;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.annotation.AtlasService;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasCluster;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStreamForImport;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.repository.store.graph.v1.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v1.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.repository.Constants.ATTR_NAME_REFERENCEABLE;

@AtlasService
public class ClusterService {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterService.class);

    private final DataAccess dataAccess;
    private final AtlasEntityStore entityStore;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityGraphRetriever;

    @Inject
    public ClusterService(DataAccess dataAccess, AtlasEntityStore entityStore, AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityGraphRetriever) {
        this.dataAccess = dataAccess;
        this.entityStore = entityStore;
        this.typeRegistry = typeRegistry;
        this.entityGraphRetriever = entityGraphRetriever;
    }

    public AtlasCluster get(AtlasCluster cluster) throws AtlasBaseException {
        try {
            return dataAccess.load(cluster);
        } catch (AtlasBaseException e) {
            LOG.error("dataAccess", e);
            throw e;
        }
    }

    @GraphTransaction
    public AtlasCluster save(AtlasCluster cluster) throws AtlasBaseException {
       return dataAccess.save(cluster);
    }

    @GraphTransaction
    public void updateEntitiesWithCluster(AtlasCluster cluster, List<String> entityGuids, String attributeName) throws AtlasBaseException {
        if (cluster != null && StringUtils.isEmpty(cluster.getGuid())) {
            return;
        }

        AtlasObjectId objectId = getObjectId(cluster);
        for (String guid : entityGuids) {
            AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityStore.getById(guid);
            updateAttribute(entityWithExtInfo, attributeName, objectId);
        }
    }

    private AtlasObjectId getObjectId(AtlasCluster cluster) {
        return new AtlasObjectId(cluster.getGuid(), AtlasCluster.class.getSimpleName());
    }


    /**
     * Attribute passed by name is updated with the value passed.
     * @param entityWithExtInfo Entity to be updated
     * @param propertyName attribute name
     * @param objectId Value to be set for attribute
     */
    private void updateAttribute(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo,
                                 String propertyName,
                                 AtlasObjectId objectId) {
        String value = EntityGraphMapper.getSoftRefFormattedValue(objectId);
        updateAttribute(entityWithExtInfo.getEntity(), propertyName, value);
        for (AtlasEntity e : entityWithExtInfo.getReferredEntities().values()) {
            updateAttribute(e, propertyName, value);
        }
    }

    private void updateAttribute(AtlasEntity entity, String attributeName, Object value) {
        if(entity.hasAttribute(attributeName) == false) return;

        try {
            AtlasVertex vertex = entityGraphRetriever.getEntityVertex(entity.getGuid());
            if(vertex == null) {
                return;
            }

            String qualifiedFieldName = getVertexPropertyName(entity, attributeName);
            List list = vertex.getListProperty(qualifiedFieldName);
            if (list == null) {
                list = new ArrayList();
            }

            if (!list.contains(value)) {
                list.add(value);
                vertex.setListProperty(qualifiedFieldName, list);
            }

        }
        catch (AtlasBaseException ex) {
            LOG.error("error retrieving vertex from guid: {}", entity.getGuid(), ex);
        } catch (AtlasException ex) {
            LOG.error("error setting property to vertex with guid: {}", entity.getGuid(), ex);
        }
    }

    private String getVertexPropertyName(AtlasEntity entity, String attributeName) throws AtlasBaseException {
        AtlasEntityType type = (AtlasEntityType) typeRegistry.getType(entity.getTypeName());
        AtlasStructType.AtlasAttribute attribute = type.getAttribute(attributeName);
        return attribute.getVertexPropertyName();
    }
}

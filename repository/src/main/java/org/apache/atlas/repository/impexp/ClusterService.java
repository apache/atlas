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

import org.apache.atlas.RequestContextV1;
import org.apache.atlas.annotation.AtlasService;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.clusterinfo.AtlasCluster;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.repository.store.graph.v1.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v1.EntityGraphRetriever;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@AtlasService
public class ClusterService {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterService.class);

    private final DataAccess dataAccess;
    private final AtlasEntityStore entityStore;

    @Inject
    public ClusterService(DataAccess dataAccess, AtlasEntityStore entityStore) {
        this.dataAccess = dataAccess;
        this.entityStore = entityStore;
    }

    public AtlasCluster get(AtlasCluster cluster) {
        try {
            return dataAccess.load(cluster);
        } catch (AtlasBaseException e) {
            LOG.error("dataAccess", e);
        }

        return null;
    }

    @GraphTransaction
    public AtlasCluster save(AtlasCluster clusterInfo) throws AtlasBaseException {
       return dataAccess.save(clusterInfo);
    }

    @GraphTransaction
    public void updateEntityWithCluster(AtlasCluster cluster, List<String> guids, String attributeName) throws AtlasBaseException {
        if(cluster != null && StringUtils.isEmpty(cluster.getGuid())) return;

        AtlasObjectId objectId = getObjectId(cluster);
        for (String guid : guids) {
            AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityStore.getById(guid);
            updateAttribute(entityWithExtInfo, attributeName, objectId);
            entityStore.createOrUpdate(new AtlasEntityStream(entityWithExtInfo), true);
        }
    }

    private AtlasObjectId getObjectId(AtlasCluster cluster) {
        return new AtlasObjectId(cluster.getGuid(), AtlasCluster.class.getSimpleName());
    }


    /**
     * Attribute passed by name is updated with the value passed.
     * @param entityWithExtInfo Entity to be updated
     * @param propertyName attribute name
     * @param value Value to be set for attribute
     */
    private void updateAttribute(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo, String propertyName, Object value) {
        updateAttribute(entityWithExtInfo.getEntity(), propertyName, value);
        for (AtlasEntity e : entityWithExtInfo.getReferredEntities().values()) {
            updateAttribute(e, propertyName, value);
        }
    }

    private void updateAttribute(AtlasEntity e, String propertyName, Object value) {
        if(e.hasAttribute(propertyName) == false) return;

        Object oVal = e.getAttribute(propertyName);
        if (oVal != null && !(oVal instanceof List)) return;

        List list;

        if (oVal == null) {
            list = new ArrayList();
        } else {
            list = (List) oVal;
        }

        if (!list.contains(value)) {
            list.add(value);
        }

        e.setAttribute(propertyName, list);
    }
}

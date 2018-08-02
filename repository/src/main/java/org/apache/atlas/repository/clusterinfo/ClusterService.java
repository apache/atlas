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

package org.apache.atlas.repository.clusterinfo;

import org.apache.atlas.annotation.AtlasService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.clusterinfo.AtlasCluster;
import org.apache.atlas.repository.ogm.DataAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

@AtlasService
public class ClusterService {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterService.class);

    private final DataAccess dataAccess;

    @Inject
    public ClusterService(DataAccess dataAccess) {
        this.dataAccess = dataAccess;
    }

    public AtlasCluster get(AtlasCluster cluster) {
        try {
            return dataAccess.load(cluster);
        } catch (AtlasBaseException e) {
            LOG.error("dataAccess", e);
        }

        return null;
    }

    public AtlasCluster save(AtlasCluster clusterInfo) throws AtlasBaseException {
        return dataAccess.save(clusterInfo);
    }
}

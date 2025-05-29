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
package org.apache.atlas.trino.connector;

import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RdbmsEntityConnector extends AtlasEntityConnector {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsEntityConnector.class);

    @Override
    public void connectTrinoCatalog(String instanceName, String catalogName, AtlasEntity entity, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo)  {

    }

    @Override
    public void connectTrinoSchema(String instanceName, String catalogName, String schemaName, AtlasEntity entity, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {

    }

    @Override
    public void connectTrinoTable(String instanceName, String catalogName, String schemaName, String tableName, AtlasEntity entity, List<AtlasEntity> columnEntities, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo)  {

    }
}

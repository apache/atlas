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

package org.apache.atlas.repository.store.graph.v2.bulkimport.pc;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.pc.WorkItemBuilder;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;

import java.util.concurrent.BlockingQueue;

public class EntityConsumerBuilder implements WorkItemBuilder<EntityConsumer, AtlasEntity.AtlasEntityWithExtInfo> {
    private AtlasGraph atlasGraph;
    private AtlasEntityStore entityStore;
    private final EntityGraphRetriever entityGraphRetriever;
    private final AtlasTypeRegistry typeRegistry;
    private int batchSize;

    public EntityConsumerBuilder(AtlasGraph atlasGraph, AtlasEntityStore entityStore,
                                 EntityGraphRetriever entityGraphRetriever, AtlasTypeRegistry typeRegistry, int batchSize) {
        this.atlasGraph = atlasGraph;
        this.entityStore = entityStore;
        this.entityGraphRetriever = entityGraphRetriever;
        this.typeRegistry = typeRegistry;
        this.batchSize = batchSize;
    }

    @Override
    public EntityConsumer build(BlockingQueue<AtlasEntity.AtlasEntityWithExtInfo> queue) {
        return new EntityConsumer(atlasGraph, entityStore, entityGraphRetriever, typeRegistry, queue, this.batchSize);
    }
}

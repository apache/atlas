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

package org.apache.atlas.repository.store.graph.v2.bulkimport;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.converters.AtlasFormatConverters;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasRelationshipStoreV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityImportStream;
import org.apache.atlas.repository.store.graph.v2.IAtlasEntityChangeNotifier;
import org.apache.atlas.repository.store.graph.v2.bulkimport.pc.EntityConsumerBuilder;
import org.apache.atlas.repository.store.graph.v2.bulkimport.pc.EntityCreationManager;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationImport extends ImportStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(MigrationImport.class);

    private final AtlasTypeRegistry typeRegistry;
    private AtlasGraph atlasGraph;
    private EntityGraphRetriever entityGraphRetriever;
    private EntityGraphMapper entityGraphMapper;
    private AtlasEntityStore entityStore;

    public MigrationImport(AtlasGraphProvider atlasGraphProvider, AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
        setupEntityStore(atlasGraphProvider, typeRegistry);
        LOG.info("MigrationImport: Using bulkLoading...");
    }

    public EntityMutationResponse run(EntityImportStream entityStream, AtlasImportResult importResult) throws AtlasBaseException {
        if (entityStream == null || !entityStream.hasNext()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "no entities to create/update.");
        }

        if (importResult.getRequest() == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "importResult should contain request");
        }

        int index = 0;
        int streamSize = entityStream.size();
        EntityMutationResponse ret = new EntityMutationResponse();
        EntityCreationManager creationManager = createEntityCreationManager(atlasGraph, importResult);

        try {
            LOG.info("Migration Import: Size: {}: Starting...", streamSize);
            index = creationManager.read(entityStream);
            creationManager.drain();
            creationManager.extractResults();
        } catch (Exception ex) {
            LOG.error("Migration Import: Error: Current position: {}", index, ex);
        } finally {
            shutdownEntityCreationManager(creationManager);
        }

        LOG.info("Migration Import: Size: {}: Done!", streamSize);
        return ret;
    }

    private EntityCreationManager createEntityCreationManager(AtlasGraph threadedAtlasGraph, AtlasImportResult importResult) {
        int batchSize = importResult.getRequest().getOptionKeyBatchSize();
        int numWorkers = getNumWorkers(importResult.getRequest().getOptionKeyNumWorkers());

        EntityConsumerBuilder consumerBuilder =
                new EntityConsumerBuilder(threadedAtlasGraph, entityStore, entityGraphRetriever, typeRegistry, batchSize);

        return new EntityCreationManager(consumerBuilder, batchSize, numWorkers, importResult);
    }

    private static int getNumWorkers(int numWorkersFromOptions) {
        int ret = (numWorkersFromOptions > 0) ? numWorkersFromOptions : 1;
        LOG.info("Migration Import: Setting numWorkers: {}", ret);
        return ret;
    }

    private void setupEntityStore(AtlasGraphProvider atlasGraphProvider, AtlasTypeRegistry typeRegistry) {
        this.entityGraphRetriever = new EntityGraphRetriever(typeRegistry);
        this.atlasGraph = atlasGraphProvider.getBulkLoading();
        DeleteHandlerDelegate deleteDelegate = new DeleteHandlerDelegate(typeRegistry);

        IAtlasEntityChangeNotifier entityChangeNotifier = new EntityChangeNotifierNop();
        AtlasRelationshipStore relationshipStore = new AtlasRelationshipStoreV2(typeRegistry, deleteDelegate, entityChangeNotifier);
        AtlasFormatConverters formatConverters = new AtlasFormatConverters(typeRegistry);
        AtlasInstanceConverter instanceConverter = new AtlasInstanceConverter(typeRegistry, formatConverters);
        this.entityGraphMapper = new EntityGraphMapper(deleteDelegate, typeRegistry, atlasGraph, relationshipStore, entityChangeNotifier, instanceConverter, new FullTextMapperV2Nop());
        this.entityStore = new AtlasEntityStoreV2(deleteDelegate, typeRegistry, entityChangeNotifier, entityGraphMapper);
    }

    private void shutdownEntityCreationManager(EntityCreationManager creationManager) {
        try {
            creationManager.shutdown();
        } catch (InterruptedException e) {
            LOG.error("Migration Import: Shutdown: Interrupted!", e);
        }
    }
}

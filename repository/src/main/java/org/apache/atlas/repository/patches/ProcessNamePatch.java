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
package org.apache.atlas.repository.patches;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasEntityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;

public class ProcessNamePatch extends AtlasPatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessNamePatch.class);

    private static final String PATCH_ID          = "JAVA_PATCH_0000_009";
    private static final String PATCH_DESCRIPTION = "Set name to qualifiedName.";

    private final PatchContext context;

    public ProcessNamePatch(PatchContext context) {
        super(context.getPatchRegistry(), PATCH_ID, PATCH_DESCRIPTION);

        this.context = context;
    }

    @Override
    public void apply() throws AtlasBaseException {
        if (!AtlasConfiguration.PROCESS_NAME_UPDATE_PATCH.getBoolean()) {
            LOG.info("ProcessNamePatch: Skipped, since not enabled!");

            return;
        }

        ConcurrentPatchProcessor patchProcessor = new ProcessNamePatchProcessor(context);

        patchProcessor.apply();

        setStatus(APPLIED);

        LOG.info("ProcessNamePatch.apply(): patchId={}, status={}", getPatchId(), getStatus());
    }

    public static class ProcessNamePatchProcessor extends ConcurrentPatchProcessor {
        private static final String TYPE_NAME_HIVE_PROCESS        = "hive_process";
        private static final String TYPE_NAME_HIVE_COLUMN_LINEAGE = "hive_column_lineage";
        private static final String ATTR_NAME_QUALIFIED_NAME      = "qualifiedName";
        private static final String ATTR_NAME_NAME                = "name";

        private static final String[] processTypes = {TYPE_NAME_HIVE_PROCESS, TYPE_NAME_HIVE_COLUMN_LINEAGE};

        public ProcessNamePatchProcessor(PatchContext context) {
            super(context);
        }

        @Override
        protected void prepareForExecution() {
        }

        @Override
        public void submitVerticesToUpdate(WorkItemManager manager) {
            AtlasGraph graph = getGraph();

            for (String typeName : processTypes) {
                LOG.info("finding entities of type {}", typeName);

                Iterable<Object> iterable = graph.query().has(Constants.ENTITY_TYPE_PROPERTY_KEY, typeName).vertexIds();
                int              count    = 0;

                for (Object vertexId : iterable) {
                    manager.checkProduce(vertexId);

                    count++;
                }

                LOG.info("found {} entities of type {}", count, typeName);
            }
        }

        @Override
        protected void processVertexItem(Long vertexId, AtlasVertex vertex, String typeName, AtlasEntityType entityType) {
            LOG.debug("processItem(typeName={}, vertexId={})", typeName, vertexId);

            try {
                String qualifiedName = AtlasGraphUtilsV2.getProperty(vertex, entityType.getVertexPropertyName(ATTR_NAME_QUALIFIED_NAME), String.class);

                AtlasGraphUtilsV2.setEncodedProperty(vertex, entityType.getVertexPropertyName(ATTR_NAME_NAME), qualifiedName);
            } catch (AtlasBaseException e) {
                LOG.error("Error updating: {}", vertexId);
            }

            LOG.debug("processItem(typeName={}, vertexId={}): Done!", typeName, vertexId);
        }
    }
}

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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;

public class ClassificationTextPatch extends AtlasPatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ClassificationTextPatch.class);

    private static final String PATCH_ID          = "JAVA_PATCH_0000_002";
    private static final String PATCH_DESCRIPTION = "Populates Classification Text attribute for entities from classifications applied on them.";

    private final PatchContext context;

    public ClassificationTextPatch(PatchContext context) {
        super(context.getPatchRegistry(), PATCH_ID, PATCH_DESCRIPTION);

        this.context = context;
    }

    @Override
    public void apply() throws AtlasBaseException {
        ConcurrentPatchProcessor patchProcessor = new ClassificationTextPatchProcessor(context);

        patchProcessor.apply();

        setStatus(APPLIED);

        LOG.info("ClassificationTextPatch.apply(): patchId={}, status={}", getPatchId(), getStatus());
    }

    public static class ClassificationTextPatchProcessor extends ConcurrentPatchProcessor {

        public ClassificationTextPatchProcessor(PatchContext context) {
            super(context);
        }

        @Override
        protected void processVertexItem(Long vertexId, AtlasVertex vertex, String typeName, AtlasEntityType entityType) throws AtlasBaseException {
            processItem(vertexId, vertex, typeName, entityType);
        }

        @Override
        protected void prepareForExecution() {
            //do nothing
        }

        protected void processItem(Long vertexId, AtlasVertex vertex, String typeName, AtlasEntityType entityType) throws AtlasBaseException {
            if(LOG.isDebugEnabled()) {
                LOG.debug("processItem(typeName={}, vertexId={})", typeName, vertexId);
            }

            getEntityGraphMapper().updateClassificationTextAndNames(vertex);

            if(LOG.isDebugEnabled()) {
                LOG.debug("processItem(typeName={}, vertexId={}): Done!", typeName, vertexId);
            }
        }
    }
}

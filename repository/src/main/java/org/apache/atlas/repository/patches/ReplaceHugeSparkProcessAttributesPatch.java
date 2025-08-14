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

package org.apache.atlas.repository.patches;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;

public class ReplaceHugeSparkProcessAttributesPatch extends AtlasPatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ReplaceHugeSparkProcessAttributesPatch.class);

    private static final String PATCH_ID = "JAVA_PATCH_0000_015";
    private static final String PATCH_DESCRIPTION = "Replace attributes details and sparkPlanDescription to null";

    private final PatchContext context;

    public ReplaceHugeSparkProcessAttributesPatch(PatchContext context) {
        super(context.getPatchRegistry(), PATCH_ID, PATCH_DESCRIPTION);

        this.context = context;
    }

    @Override
    public void apply() throws AtlasBaseException {
        if (AtlasConfiguration.REPLACE_HUGE_SPARK_PROCESS_ATTRIBUTES_PATCH.getBoolean() == false) {
            LOG.info("ReplaceHugeSparkProcessAttributesPatch: Skipped, since not enabled!");
            return;
        }
        ConcurrentPatchProcessor patchProcessor = new ReplaceHugeSparkProcessAttributesPatchProcessor(context);

        patchProcessor.apply();

        setStatus(APPLIED);

        LOG.info("ReplaceHugeSparkProcessAttributesPatch.apply(): patchId={}, status={}", getPatchId(), getStatus());
    }

    public static class ReplaceHugeSparkProcessAttributesPatchProcessor extends ConcurrentPatchProcessor {
        private static final String TYPE_NAME_SPARK_PROCESS = "spark_process";
        private static final String ATTR_NAME_DETAILS = "details";
        private static final String ATTR_NAME_SPARKPLANDESCRIPTION = "sparkPlanDescription";

        public ReplaceHugeSparkProcessAttributesPatchProcessor(PatchContext context) {
            super(context);
        }

        @Override
        protected void prepareForExecution() {
        }

        @Override
        public void submitVerticesToUpdate(WorkItemManager manager) {
            AtlasGraph graph = getGraph();
            Iterable<Object> iterable = graph.query().has(Constants.ENTITY_TYPE_PROPERTY_KEY, TYPE_NAME_SPARK_PROCESS).vertexIds();
            int count = 0;

            for (Iterator<Object> iter = iterable.iterator(); iter.hasNext(); ) {
                Object vertexId = iter.next();

                manager.checkProduce(vertexId);

                count++;
            }

            LOG.info("found {} entities of type {}", count, TYPE_NAME_SPARK_PROCESS);
        }

        @Override
        protected void processVertexItem(Long vertexId, AtlasVertex vertex, String typeName, AtlasEntityType entityType) {
            LOG.debug("processItem(typeName={}, vertexId={})", typeName, vertexId);

            try {
                vertex.removeProperty(entityType.getVertexPropertyName(ATTR_NAME_DETAILS));
                vertex.removeProperty(entityType.getVertexPropertyName(ATTR_NAME_SPARKPLANDESCRIPTION));
            } catch (Exception e) {
                LOG.error("Error updating: {}", vertexId, e);
            }

            LOG.debug("processItem(typeName={}, vertexId={}): Done!", typeName, vertexId);
        }
    }
}

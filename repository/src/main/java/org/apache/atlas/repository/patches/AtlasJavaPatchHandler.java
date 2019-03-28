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

import org.apache.atlas.RequestContext;
import org.apache.atlas.model.patches.AtlasPatch.PatchStatus;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.MapUtils;

import java.util.Map;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.UNKNOWN;
import static org.apache.atlas.repository.Constants.CREATED_BY_KEY;
import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.MODIFIED_BY_KEY;
import static org.apache.atlas.repository.Constants.PATCH_DESCRIPTION_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.PATCH_ID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.PATCH_STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.PATCH_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.findByPatchId;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.setEncodedProperty;
import static org.apache.atlas.repository.store.graph.v2.AtlasTypeDefGraphStoreV2.getCurrentUser;

public abstract class AtlasJavaPatchHandler {
    public final AtlasGraph               graph;
    public final AtlasTypeRegistry        typeRegistry;
    public final Map<String, PatchStatus> patchesRegistry;
    public final EntityGraphRetriever     entityRetriever;
    public final GraphBackedSearchIndexer indexer;
    public final PatchContext             context;
    public final String                   patchId;
    public final String                   patchDescription;

    private PatchStatus patchStatus;

    public static final String JAVA_PATCH_TYPE = "JAVA_PATCH";

    public AtlasJavaPatchHandler(PatchContext context, String patchId, String patchDescription) {
        this.context          = context;
        this.graph            = context.getGraph();
        this.typeRegistry     = context.getTypeRegistry();
        this.indexer          = context.getIndexer();
        this.patchesRegistry  = context.getPatchesRegistry();
        this.patchId          = patchId;
        this.patchDescription = patchDescription;
        this.patchStatus      = getPatchStatus(patchesRegistry);
        this.entityRetriever  = new EntityGraphRetriever(typeRegistry);

        init();
    }

    private void init() {
        PatchStatus patchStatus = getPatchStatus();

        if (patchStatus == UNKNOWN) {
            AtlasVertex patchVertex = graph.addVertex();

            setEncodedProperty(patchVertex, PATCH_ID_PROPERTY_KEY, patchId);
            setEncodedProperty(patchVertex, PATCH_DESCRIPTION_PROPERTY_KEY, patchDescription);
            setEncodedProperty(patchVertex, PATCH_TYPE_PROPERTY_KEY, JAVA_PATCH_TYPE);
            setEncodedProperty(patchVertex, PATCH_STATE_PROPERTY_KEY, getPatchStatus().toString());
            setEncodedProperty(patchVertex, TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
            setEncodedProperty(patchVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
            setEncodedProperty(patchVertex, CREATED_BY_KEY, getCurrentUser());
            setEncodedProperty(patchVertex, MODIFIED_BY_KEY, getCurrentUser());

            addToPatchesRegistry(patchId, getPatchStatus());
        }

        graph.commit();
    }

    private PatchStatus getPatchStatus(Map<String, PatchStatus> patchesRegistry) {
        PatchStatus ret = UNKNOWN;

        if (MapUtils.isNotEmpty(patchesRegistry) && patchesRegistry.containsKey(patchId)) {
            ret = patchesRegistry.get(patchId);
        }

        return ret;
    }

    public void updatePatchVertex(PatchStatus patchStatus) {
        AtlasVertex patchVertex = findByPatchId(patchId);

        if (patchVertex != null) {
            setEncodedProperty(patchVertex, PATCH_STATE_PROPERTY_KEY, patchStatus.toString());
            setEncodedProperty(patchVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
            setEncodedProperty(patchVertex, MODIFIED_BY_KEY, getCurrentUser());

            addToPatchesRegistry(getPatchId(), getPatchStatus());
        }

        graph.commit();
    }

    public PatchStatus getPatchStatus() {
        return patchStatus;
    }

    public void addToPatchesRegistry(String patchId, PatchStatus status) {
        getPatchesRegistry().put(patchId, status);
    }

    public void setPatchStatus(PatchStatus patchStatus) {
        this.patchStatus = patchStatus;
    }

    public String getPatchId() {
        return patchId;
    }

    public Map<String, PatchStatus> getPatchesRegistry() {
        return patchesRegistry;
    }

    public abstract void applyPatch();
}
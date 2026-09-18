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
package org.apache.atlas.tools;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.semantic.OpenSearchSemanticStore;
import org.apache.atlas.semantic.SemanticSearchException;
import org.apache.atlas.semantic.SemanticTextBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;

public class SemanticEntityIndexer {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticEntityIndexer.class);

    private final SemanticTextBuilder      textBuilder;
    private final OpenSearchSemanticStore semanticStore;

    public SemanticEntityIndexer(SemanticTextBuilder textBuilder, OpenSearchSemanticStore semanticStore) {
        this.textBuilder   = textBuilder;
        this.semanticStore = semanticStore;
    }

    public int indexGuids(Set<String> guids) {
        if (guids == null || guids.isEmpty()) {
            return 0;
        }

        int indexed = 0;
        for (String guid : guids) {
            if (indexGuid(guid)) {
                indexed++;
            }
        }

        if (indexed > 0) {
            LOG.debug("Indexed {} of {} guid(s)", indexed, guids.size());
        }

        return indexed;
    }

    private boolean indexGuid(String guid) {
        String text = buildText(guid);
        if (StringUtils.isBlank(text)) {
            LOG.debug("Skipping semantic update for guid={}: no embeddable text", guid);
            return false;
        }

        try {
            String documentId = JanusGraphSemanticDocumentResolver.resolveVertexIndexDocumentId(guid);
            if (StringUtils.isBlank(documentId)) {
                LOG.warn("Skipping semantic update for guid={}: OpenSearch document id not found", guid);
                return false;
            }

            semanticStore.updateEmbedding(documentId, text);
            LOG.debug("Updated semantic embedding for guid={} documentId={}", guid, documentId);
            return true;
        } catch (SemanticSearchException | org.apache.atlas.exception.AtlasBaseException e) {
            LOG.error("Semantic indexing failed for guid={}", guid, e);
            return false;
        }
    }

    private String buildText(String guid) {
        if (StringUtils.isBlank(guid)) {
            return null;
        }

        try {
            AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(guid);
            if (vertex == null) {
                return null;
            }

            String typeName = AtlasGraphUtilsV2.getEncodedProperty(vertex, TYPE_NAME_PROPERTY_KEY, String.class);
            if (GraphHelper.isInternalType(typeName)) {
                return null;
            }

            if (AtlasGraphUtilsV2.getState(vertex) != org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE) {
                return null;
            }

            return textBuilder.buildTextForEntity(guid, vertex);
        } catch (AtlasBaseException e) {
            LOG.error("Semantic indexing failed while building text for guid={}", guid, e);
            return null;
        }
    }
}

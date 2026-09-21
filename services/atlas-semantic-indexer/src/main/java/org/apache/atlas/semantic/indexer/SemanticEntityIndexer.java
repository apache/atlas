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
package org.apache.atlas.semantic.indexer;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.semantic.OpenSearchSemanticStore;
import org.apache.atlas.semantic.SemanticNotificationGuidExpander;
import org.apache.atlas.semantic.SemanticSearchException;
import org.apache.atlas.semantic.SemanticTextBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;

public class SemanticEntityIndexer {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticEntityIndexer.class);

    private final SemanticTextBuilder     textBuilder;
    private final OpenSearchSemanticStore semanticStore;

    public SemanticEntityIndexer(SemanticTextBuilder textBuilder, OpenSearchSemanticStore semanticStore) {
        this.textBuilder   = textBuilder;
        this.semanticStore = semanticStore;
    }

    public static final class IndexStats {
        public static final IndexStats EMPTY = new IndexStats(0, 0, 0);

        private final int indexed;
        private final int skipped;
        private final int failed;

        public IndexStats(int indexed, int skipped, int failed) {
            this.indexed = indexed;
            this.skipped = skipped;
            this.failed  = failed;
        }

        public int getIndexed() {
            return indexed;
        }

        public int getSkipped() {
            return skipped;
        }

        public int getFailed() {
            return failed;
        }

        public boolean hasFailures() {
            return failed > 0;
        }
    }

    public IndexStats indexGuids(Set<String> guids) {
        if (guids == null || guids.isEmpty()) {
            return new IndexStats(0, 0, 0);
        }

        int indexed = 0;
        int skipped = 0;
        int failed  = 0;

        for (String guid : guids) {
            switch (indexGuid(guid)) {
                case INDEXED:
                    indexed++;
                    break;
                case SKIPPED:
                    skipped++;
                    break;
                case FAILED:
                    failed++;
                    break;
                default:
                    break;
            }
        }

        if (indexed > 0) {
            LOG.info("Indexed {} of {} guid(s) (skipped={}, failed={})", indexed, guids.size(), skipped, failed);
        }

        return new IndexStats(indexed, skipped, failed);
    }

    public IndexStats indexGuidsInBatches(Set<String> guids, int batchSize) {
        if (guids == null || guids.isEmpty()) {
            return new IndexStats(0, 0, 0);
        }

        int indexed = 0;
        int skipped = 0;
        int failed  = 0;
        Set<String> batch = new LinkedHashSet<>();

        for (String guid : guids) {
            batch.add(guid);
            if (batch.size() >= batchSize) {
                IndexStats stats = indexGuids(batch);
                indexed += stats.getIndexed();
                skipped += stats.getSkipped();
                failed  += stats.getFailed();
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            IndexStats stats = indexGuids(batch);
            indexed += stats.getIndexed();
            skipped += stats.getSkipped();
            failed  += stats.getFailed();
        }

        return new IndexStats(indexed, skipped, failed);
    }

    private enum IndexOutcome {
        INDEXED, SKIPPED, FAILED
    }

    private IndexOutcome indexGuid(String guid) {
        AtlasVertex vertex = loadIndexableVertex(guid);
        if (vertex == null) {
            LOG.debug("Skipping semantic update for guid={}: not indexable", guid);
            return IndexOutcome.SKIPPED;
        }

        String text = textBuilder.buildText(vertex);
        if (StringUtils.isBlank(text)) {
            LOG.debug("Skipping semantic update for guid={}: no embeddable text", guid);
            return IndexOutcome.SKIPPED;
        }

        try {
            String documentId = JanusGraphSemanticDocumentResolver.resolveVertexIndexDocumentId(vertex);
            if (StringUtils.isBlank(documentId)) {
                LOG.warn("Skipping semantic update for guid={}: OpenSearch document id not found", guid);
                return IndexOutcome.SKIPPED;
            }

            semanticStore.updateEmbedding(documentId, text);
            LOG.debug("Updated semantic embedding for guid={} documentId={}", guid, documentId);
            return IndexOutcome.INDEXED;
        } catch (SemanticSearchException | org.apache.atlas.exception.AtlasBaseException e) {
            LOG.error("Semantic indexing failed for guid={}", guid, e);
            return IndexOutcome.FAILED;
        }
    }

    private AtlasVertex loadIndexableVertex(String guid) {
        if (StringUtils.isBlank(guid)) {
            return null;
        }

        try {
            AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(guid);
            if (vertex == null) {
                return null;
            }

            String typeName = AtlasGraphUtilsV2.getEncodedProperty(vertex, TYPE_NAME_PROPERTY_KEY, String.class);
            if (!SemanticNotificationGuidExpander.isEmbeddableEntityType(typeName)) {
                return null;
            }

            if (AtlasGraphUtilsV2.getState(vertex) != AtlasEntity.Status.ACTIVE) {
                return null;
            }

            return vertex;
        } catch (RuntimeException e) {
            LOG.error("Semantic indexing failed while loading vertex for guid={}", guid, e);
            return null;
        }
    }
}

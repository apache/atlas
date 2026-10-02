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
package org.apache.atlas.semantic;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.glossary.GlossaryUtils;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;

/**
 * Embeds entities by guid: reads each entity from the graph, builds its text and stores the embedding. Shared by
 * the Semantic Indexer and the repair tool. Callers own the thread-bound graph transaction and roll it back after
 * each batch.
 */
public class SemanticEntityEmbedder {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticEntityEmbedder.class);

    // Glossary objects are entity vertices too: their supertype is __internal, but their names lack the "__" prefix
    // that isInternalType() checks, so Atlas notifies them and the repair scan would match them like any entity.
    // Names, not the supertype, because __superTypeNames is a SET property and is not in the OpenSearch vertex index.
    private static final Set<String> NON_EMBEDDABLE_ENTITY_TYPES = Set.of(
            GlossaryUtils.ATLAS_GLOSSARY_TERM_TYPENAME,
            GlossaryUtils.ATLAS_GLOSSARY_TYPENAME,
            GlossaryUtils.ATLAS_GLOSSARY_CATEGORY_TYPENAME);

    private final SemanticTextBuilder textBuilder;
    private final SemanticVectorStore semanticStore;

    public SemanticEntityEmbedder(SemanticTextBuilder textBuilder, SemanticVectorStore semanticStore) {
        this.textBuilder   = textBuilder;
        this.semanticStore = semanticStore;
    }

    public static Set<String> getNonEmbeddableEntityTypes() {
        return NON_EMBEDDABLE_ENTITY_TYPES;
    }

    public static boolean isEmbeddableEntityType(String typeName) {
        return StringUtils.isNotBlank(typeName)
                && !GraphHelper.isInternalType(typeName)
                && !NON_EMBEDDABLE_ENTITY_TYPES.contains(typeName);
    }

    public static final class IndexStats {
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

        public IndexStats add(IndexStats other) {
            return new IndexStats(indexed + other.indexed, skipped + other.skipped, failed + other.failed);
        }
    }

    public IndexStats embed(Collection<String> guids) {
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

        if (indexed > 0 || failed > 0) {
            LOG.info("Indexed {} of {} guid(s) (skipped={}, failed={})", indexed, guids.size(), skipped, failed);
        }

        return new IndexStats(indexed, skipped, failed);
    }

    private enum IndexOutcome {
        INDEXED, SKIPPED, FAILED
    }

    private IndexOutcome indexGuid(String guid) {
        AtlasVertex vertex;

        try {
            vertex = loadIndexableVertex(guid);
        } catch (RuntimeException e) {
            LOG.error("Semantic indexing failed while loading vertex for guid={}", guid, e);
            return IndexOutcome.FAILED;
        }

        if (vertex == null) {
            LOG.debug("Skipping semantic update for guid={}: not indexable or not found", guid);
            return IndexOutcome.SKIPPED;
        }

        String text;

        try {
            text = textBuilder.buildText(vertex);
        } catch (RuntimeException e) {
            LOG.error("Failed to build semantic text for guid={}", guid, e);
            return IndexOutcome.FAILED;
        }

        if (StringUtils.isBlank(text)) {
            LOG.debug("Skipping semantic update for guid={}: no embeddable text", guid);
            return IndexOutcome.SKIPPED;
        }

        try {
            semanticStore.updateEmbedding(guid, text);
            LOG.debug("Updated semantic embedding for guid={}", guid);
            return IndexOutcome.INDEXED;
        } catch (SemanticSearchException e) {
            LOG.error("Abandoning semantic indexing for guid={} (max attempts={}): {}",
                    guid, AtlasConfiguration.SEMANTIC_RETRY_MAX_ATTEMPTS.getInt(), e.getMessage());
            return IndexOutcome.FAILED;
        } catch (RuntimeException e) {
            LOG.error("Semantic indexing failed for guid={}", guid, e);
            return IndexOutcome.FAILED;
        }
    }

    private AtlasVertex loadIndexableVertex(String guid) {
        if (StringUtils.isBlank(guid)) {
            return null;
        }

        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(guid);
        if (vertex == null) {
            return null;
        }

        String typeName = AtlasGraphUtilsV2.getEncodedProperty(vertex, TYPE_NAME_PROPERTY_KEY, String.class);
        if (!isEmbeddableEntityType(typeName)) {
            return null;
        }

        if (AtlasGraphUtilsV2.getState(vertex) != AtlasEntity.Status.ACTIVE) {
            return null;
        }

        return vertex;
    }
}

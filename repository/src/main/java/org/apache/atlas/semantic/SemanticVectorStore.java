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

import java.util.List;
import java.util.function.Consumer;

/**
 * Computes, stores and searches entity embeddings, keyed by Atlas guid.
 */
public interface SemanticVectorStore {
    /**
     * Checks the backend and creates what the store needs (model pipeline, vector field). Run by the Semantic
     * Indexer and the repair tool at startup, after the graph is opened.
     */
    void initialize() throws SemanticSearchException;

    /**
     * Embeds the text and stores the vector on the entity.
     */
    void updateEmbedding(String guid, String text) throws SemanticSearchException;

    /**
     * Embeds the query text and returns the nearest entities.
     */
    List<VectorSearchHit> searchByText(String text, int topK, VectorSearchFilter filter) throws SemanticSearchException;

    /**
     * Returns the entities nearest to a stored vector, without query-time inference.
     */
    List<VectorSearchHit> searchByVector(List<?> vector, int topK, VectorSearchFilter filter) throws SemanticSearchException;

    /**
     * The stored vector of the entity, or null when it has none.
     */
    List<?> getStoredEmbedding(String guid) throws SemanticSearchException;

    /**
     * Pages through the guids of active, embeddable entities in guid order, passing each page to {@code onPage}.
     * With {@code missingEmbeddingOnly}, only entities without a stored embedding. The store can lag behind Atlas:
     * callers must still check that each entity exists, is active and is embeddable.
     */
    void scanEntities(boolean missingEmbeddingOnly, int pageSize, Consumer<List<String>> onPage) throws SemanticSearchException;
}

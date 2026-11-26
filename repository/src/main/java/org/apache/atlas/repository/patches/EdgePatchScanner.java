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

import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.repository.Constants.RELATIONSHIP_GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.RELATIONSHIP_TYPE_PROPERTY_KEY;

/**
 * Finds relationship edges that are missing the relationship typeName property, using the edge index (Solr)
 * instead of iterating the graph. Iterating the graph reads every vertex row in HBase, and fails with
 * RowTooBigException when a vertex has a very large number of edges.
 */
public final class EdgePatchScanner {
    private static final Logger LOG = LoggerFactory.getLogger(EdgePatchScanner.class);

    static final String EDGES_WITHOUT_RELATIONSHIP_TYPE_QUERY = indexField(RELATIONSHIP_GUID_PROPERTY_KEY) + ":* AND -" + indexField(RELATIONSHIP_TYPE_PROPERTY_KEY) + ":*";

    static final int  MAX_PASSES         = 10;
    static final int  MAX_STALE_ATTEMPTS = 30;
    static final long STALE_WAIT_MS      = 1000;

    private EdgePatchScanner() {
    }

    /**
     * Patched edges no longer match the index query, so each round reads from the front of the result,
     * skipping only edges that were left unpatched in earlier rounds; offset paging would skip edges as the
     * result shrinks. Passes are repeated until a pass updates nothing, so edges whose position changed are
     * picked up as well.
     *
     * @return number of edges that still failed to update in the last pass
     */
    public static long submitEdgesWithoutRelationshipType(AtlasGraph graph, EdgePatchProcessor processor, WorkItemManager manager, int pageSize, int roundSize) {
        return submitEdgesWithoutRelationshipType(graph, processor, manager, pageSize, roundSize, STALE_WAIT_MS);
    }

    static long submitEdgesWithoutRelationshipType(AtlasGraph graph, EdgePatchProcessor processor, WorkItemManager manager, int pageSize, int roundSize, long staleWaitMs) {
        long failedInLastPass = 0;

        for (int pass = 1; pass <= MAX_PASSES; pass++) {
            long updatedAtStart = processor.getUpdatedCount();
            long failedAtStart  = processor.getFailedCount();

            runPass(graph, processor, manager, pageSize, roundSize, staleWaitMs);

            long updated = processor.getUpdatedCount() - updatedAtStart;

            failedInLastPass = processor.getFailedCount() - failedAtStart;

            LOG.info("EdgePatchScanner: pass {}: updated {} edges, failed {} edges", pass, updated, failedInLastPass);

            if (updated == 0) {
                return failedInLastPass;
            }
        }

        throw new IllegalStateException("EdgePatchScanner: edges were still being updated after " + MAX_PASSES + " passes");
    }

    private static void runPass(AtlasGraph graph, EdgePatchProcessor processor, WorkItemManager manager, int pageSize, int roundSize, long staleWaitMs) {
        Set<String> previousRound = Collections.emptySet();
        int         skip          = 0;
        int         staleAttempts = 0;

        while (true) {
            Page         page    = fetchEdgeIds(graph, skip, pageSize, roundSize);
            List<String> edgeIds = page.edgeIds;

            if (page.fetched == 0) {
                return;
            }

            if (!edgeIds.isEmpty() && previousRound.containsAll(edgeIds)) {
                // updates from the previous round are not yet visible in the index
                if (++staleAttempts > MAX_STALE_ATTEMPTS) {
                    throw new IllegalStateException("EdgePatchScanner: updated edges are still returned by the edge index after " + MAX_STALE_ATTEMPTS + " attempts");
                }

                sleep(staleWaitMs);

                continue;
            }

            staleAttempts = 0;

            long updatedAtStart = processor.getUpdatedCount();

            for (String edgeId : edgeIds) {
                manager.checkProduce(edgeId);
            }

            manager.drain();
            manager.start();

            long updated = processor.getUpdatedCount() - updatedAtStart;

            skip         += (int) Math.max(0, page.fetched - updated);
            previousRound = new HashSet<>(edgeIds);

            LOG.info("EdgePatchScanner: submitted {} edges, updated {}, left unpatched so far {}", edgeIds.size(), updated, skip);
        }
    }

    static Page fetchEdgeIds(AtlasGraph graph, int offset, int pageSize, int maxCount) {
        List<String> edgeIds = new ArrayList<>();
        int          fetched = 0;

        while (fetched < maxCount) {
            int                              limit   = Math.min(pageSize, maxCount - fetched);
            AtlasIndexQuery                  query   = graph.indexQuery(Constants.EDGE_INDEX, EDGES_WITHOUT_RELATIONSHIP_TYPE_QUERY);
            Iterator<AtlasIndexQuery.Result> results = query.edges(offset + fetched, limit);
            int                              count   = 0;

            while (results != null && results.hasNext()) {
                AtlasEdge edge = results.next().getEdge();

                count++;

                if (edge != null) {
                    edgeIds.add(edge.getId().toString());
                }
            }

            fetched += count;

            if (count < limit) {
                break;
            }
        }

        return new Page(edgeIds, fetched);
    }

    private static String indexField(String propertyName) {
        return AtlasGraphUtilsV2.getIndexSearchPrefix() + "\"" + propertyName + "\"";
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException("EdgePatchScanner: interrupted", e);
        }
    }

    static final class Page {
        final List<String> edgeIds;
        final int          fetched;

        Page(List<String> edgeIds, int fetched) {
            this.edgeIds = edgeIds;
            this.fetched = fetched;
        }
    }
}

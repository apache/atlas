// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.opensearch;

import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.indexing.RawQuery.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author David Clement (david.clement90@laposte.net)
 *
 * <p>Iterates the results of a scrolled OpenSearch search. The server-side scroll context is a finite resource
 * and must be released. The scroll context is deleted:
 * <ul>
 *     <li>on normal exhaustion (last batch smaller than the batch size),</li>
 *     <li>on early termination (e.g. when the consuming {@code Stream} is limited and closed), via {@link #close()},</li>
 *     <li>on exception while fetching the next batch.</li>
 * </ul>
 * Deletion is best-effort: a failure to delete the scroll context is logged but never masks the original result
 * or exception.</p>
 */
public class OpenSearchScroll implements Iterator<RawQuery.Result<String>>, Closeable {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchScroll.class);

    private final BlockingQueue<RawQuery.Result<String>> queue;
    private final OpenSearchClient client;
    private final int batchSize;

    private boolean isFinished;
    private boolean scrollDeleted;
    private String scrollId;

    public OpenSearchScroll(OpenSearchClient client, OpenSearchResponse initialResponse, int nbDocByQuery) {
        queue = new LinkedBlockingQueue<>();
        this.client = client;
        this.batchSize = nbDocByQuery;
        update(initialResponse);
    }

    private void update(OpenSearchResponse response) {
        response.getResults().forEach(queue::add);
        this.scrollId = response.getScrollId();
        this.isFinished = response.numResults() < this.batchSize;
        if (isFinished) {
            deleteScrollQuietly();
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (!queue.isEmpty()) {
                return true;
            }
            if (isFinished) {
                return false;
            }
            final OpenSearchResponse res = client.search(scrollId);
            update(res);
            return res.numResults() > 0;
        } catch (final IOException e) {
            // Fetching the next batch failed: release the scroll context best-effort, then surface the original error.
            deleteScrollQuietly();
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    public Result<String> next() {
        if (hasNext()) {
            return queue.remove();
        }
        throw new NoSuchElementException();
    }

    /**
     * Releases the server-side scroll context. Safe to call multiple times and safe to call on a partially or fully
     * consumed iterator. Intended to be wired to the consuming {@code Stream}'s close handler so that early
     * termination (e.g. {@code stream.limit(n)}) does not leak scroll contexts.
     */
    @Override
    public void close() {
        deleteScrollQuietly();
    }

    private void deleteScrollQuietly() {
        if (scrollDeleted || scrollId == null) {
            return;
        }
        // Mark deleted before the call so a failure does not cause repeated delete attempts.
        scrollDeleted = true;
        try {
            client.deleteScroll(scrollId);
        } catch (final IOException e) {
            // Best-effort cleanup: the scroll context will expire server-side on its own; do not mask the caller's flow.
            log.warn("Failed to delete OpenSearch scroll context {} (will expire server-side).", scrollId, e);
        }
    }
}

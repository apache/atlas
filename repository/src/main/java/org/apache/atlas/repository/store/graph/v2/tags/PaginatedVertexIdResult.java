package org.apache.atlas.repository.store.graph.v2.tags;

import java.util.Set;

/**
 * A class to represent paginated results from GUID queries.
 * Includes both the set of GUIDs and the pagination state for subsequent requests.
 */
public class PaginatedVertexIdResult {
    private final Set<Long> vertexIds;
    private final String pagingState;
    private final Boolean done;

    public PaginatedVertexIdResult(Set<Long> vertexIds, String pagingState, Boolean done) {
        this.vertexIds = vertexIds;
        this.pagingState = pagingState;
        this.done = done;
    }

    public Set<Long> getVertexIds() {
        return vertexIds;
    }

    public String getPagingState() {
        return pagingState;
    }

    public Boolean isDone() {
        return done;
    }

    public boolean hasMorePages() {
        return pagingState != null && !pagingState.isEmpty();
    }
}

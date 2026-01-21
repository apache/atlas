package org.apache.atlas.repository.store.graph.v2.tags;

import java.util.Set;

/**
 * A class to represent paginated results from GUID queries.
 * Includes both the set of GUIDs and the pagination state for subsequent requests.
 */
public class PaginatedGuidResult {
    private final Set<String> guids;
    private final String pagingState;
    private final Boolean done;

    public PaginatedGuidResult(Set<String> guids, String pagingState, Boolean done) {
        this.guids = guids;
        this.pagingState = pagingState;
        this.done = done;
    }

    public Set<String> getGuids() {
        return guids;
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

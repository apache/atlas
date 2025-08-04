package org.apache.atlas.repository.store.graph.v2.tags;

import org.apache.atlas.model.Tag;

import java.util.List; /**
 * A class to represent paginated results from tag queries.
 * Includes both the list of tags and the pagination state for subsequent requests.
 */
public class PaginatedTagResult {
    private final List<Tag> tags;
    private final String pagingState;
    private final Boolean done;

    public PaginatedTagResult(List<Tag> tags, String pagingState, Boolean done) {
        this.tags = tags;
        this.pagingState = pagingState;
        this.done = done;
    }

    public List<Tag> getTags() {
        return tags;
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
package org.apache.atlas.model.instance;

/**
 * Enum defining the types of direct Elasticsearch operations supported.
 */
public enum DirectSearchType {
    /**
     * Simple search operation using a standard Elasticsearch query.
     * Requires indexName and query parameters.
     */
    SIMPLE,

    /**
     * Create a Point-in-Time (PIT) snapshot of an index.
     * Requires indexName parameter, optionally accepts keepAlive.
     */
    PIT_CREATE,

    /**
     * Search using a Point-in-Time ID.
     * Requires query parameter with pit section in query.query.
     */
    PIT_SEARCH,

    /**
     * Delete a Point-in-Time snapshot.
     * Requires pitId parameter.
     */
    PIT_DELETE
} 
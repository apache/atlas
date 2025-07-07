package org.apache.atlas.model.instance;

/**
 * Enum defining the types of search operations supported by the Elasticsearch API.
 */
public enum DirectSearchType {
        /** Normal search operation without Point-in-Time context */
        SIMPLE,

        /** Creates a new Point-in-Time context for consistent paginated searches */
        PIT_CREATE,

        /** Performs a search using an existing Point-in-Time context */
        PIT_SEARCH,

        /** Deletes/closes an existing Point-in-Time context */
        PIT_DELETE
}

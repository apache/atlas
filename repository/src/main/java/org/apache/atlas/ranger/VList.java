package org.apache.atlas.ranger;

public abstract class VList implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Start index for the result
     */
    protected int startIndex;
    /**
     * Page size used for the result
     */
    protected int pageSize;
    /**
     * Total records in the database for the given search conditions
     */
    protected long totalCount;
    /**
     * Number of rows returned for the search condition
     */
    protected int resultSize;
    /**
     * Sort type. Either desc or asc
     */
    protected String sortType;
    /**
     * Comma seperated list of the fields for sorting
     */
    protected String sortBy;

    protected long queryTimeMS = System.currentTimeMillis();

    public int getStartIndex() {
        return startIndex;
    }

    public int getPageSize() {
        return pageSize;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public int getResultSize() {
        return resultSize;
    }

    public String getSortType() {
        return sortType;
    }

    public String getSortBy() {
        return sortBy;
    }

    public long getQueryTimeMS() {
        return queryTimeMS;
    }

    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public void setResultSize(int resultSize) {
        this.resultSize = resultSize;
    }

    public void setSortType(String sortType) {
        this.sortType = sortType;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public void setQueryTimeMS(long queryTimeMS) {
        this.queryTimeMS = queryTimeMS;
    }
}
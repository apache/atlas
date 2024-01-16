package org.apache.atlas.repository.graphdb;

import org.elasticsearch.search.aggregations.Aggregation;

import java.util.Iterator;
import java.util.Map;

public class DirectIndexQueryResult<V, E> {
    private Iterator<AtlasIndexQuery.Result<V, E>> iterator;
    private Map<String, Aggregation> aggregationMap;

    private Map<String, Object> highlightMap;
    private Integer approximateCount;

    public Iterator<AtlasIndexQuery.Result<V, E>> getIterator() {
        return iterator;
    }

    public void setIterator(Iterator<AtlasIndexQuery.Result<V, E>> iterator) {
        this.iterator = iterator;
    }

    public Map<String, Aggregation> getAggregationMap() {
        return aggregationMap;
    }

    public void setAggregationMap(Map<String, Aggregation> aggregationMap) {
        this.aggregationMap = aggregationMap;
    }

    public Integer getApproximateCount() {
        return approximateCount;
    }

    public void setApproximateCount(Integer approximateCount) {
        this.approximateCount = approximateCount;
    }

    public Map<String, Object> getHighlightMap() {
        return highlightMap;
    }

    public void setHighlightMap(Map<String, Object> highlightMap) {
        this.highlightMap = highlightMap;
    }
}
package org.apache.atlas.repository.graphdb;

import org.elasticsearch.search.aggregations.Aggregation;

import java.util.Iterator;
import java.util.Map;

public class DirectIndexQueryResult<V, E> {
    private Iterator<AtlasIndexQuery.Result<V, E>> iterator;
    private Map<String, Aggregation> aggregationMap;

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
}
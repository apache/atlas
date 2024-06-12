package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class ElasticsearchMetadata {

    private Map<String, List<String>> highlights;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private ArrayList<Object> sort;

    public Map<String, List<String>> getHighlights() {
        return highlights;
    }

    public void addHighlights(Map<String, List<String>> highlights) {
        if(MapUtils.isNotEmpty(highlights)) {
            if (MapUtils.isEmpty(this.highlights)) {
                this.highlights = new HashMap<>();
            }
            this.highlights.putAll(highlights);
        }
    }

    public Object getSort() { return sort; }

    public void addSort(ArrayList<Object> sort) {

        if(!sort.isEmpty()) {
            if (MapUtils.isEmpty(this.highlights)) {
                this.sort = new ArrayList<>();
            }
            this.sort = sort;
        }
    }

    @Override
    public String toString() {
        return "SearchMetadata{" +
                "highlights=" + highlights +
                '}';
    }
}

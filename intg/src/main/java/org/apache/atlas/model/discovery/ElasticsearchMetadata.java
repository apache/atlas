package org.apache.atlas.model.discovery;

import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchMetadata {

    private Map<String, List<String>> highlights;

    public Map<String, List<String>> getHighlights() {
        return highlights;
    }

    public void setHighlights(Map<String, List<String>> highlights) {
        this.highlights = highlights;
    }

    public void addHighlights(Map<String, List<String>> highlights) {
        if(MapUtils.isNotEmpty(highlights)) {
            if (MapUtils.isEmpty(this.highlights)) {
                this.highlights = new HashMap<>();
            }
            this.highlights.putAll(highlights);
        }
    }


    @Override
    public String toString() {
        return "SearchMetadata{" +
                "highlights=" + highlights +
                '}';
    }
}

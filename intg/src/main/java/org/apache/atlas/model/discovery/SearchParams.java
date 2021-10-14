package org.apache.atlas.model.discovery;

import java.util.Set;

public abstract class SearchParams {

    Set<String> attributes;
    Set<String> relationAttributes;

    public abstract String getQuery();

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public Set<String> getRelationAttributes() {
        return relationAttributes;
    }

    public void setRelationAttributes(Set<String> relationAttributes) {
        this.relationAttributes = relationAttributes;
    }
}

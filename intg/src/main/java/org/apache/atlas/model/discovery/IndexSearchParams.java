package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.type.AtlasType;

import java.util.Map;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexSearchParams extends SearchParams {

    private Map query;

    @Override
    public String getQuery() {
        return AtlasType.toJson(query);
    }

    public void setQuery(Map query) {
        this.query = query;
    }

    private Set<String> relationAttributes;

    public Set<String> getRelationAttributes() {
        return relationAttributes;
    }

    public void setRelationAttributes(Set<String> relationAttributes) {
        this.relationAttributes = relationAttributes;
    }

    @Override
    public String toString() {
        return "IndexSearchParams{" +
                ", query='" + query + '\'' +
                ", attributes=" + attributes +
                ", relationAttributes=" + relationAttributes +
                '}';
    }
}

package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)

public class IndexSearchParams extends SearchParams {
    private static final Logger LOG = LoggerFactory.getLogger(IndexSearchParams.class);

    private static final Pattern pattern = Pattern.compile("(?<=\").+?(?=\")");

    private Map dsl;
    private String queryString;

    /*
    * Indexsearch includes all relations (if requested with param attributes) even if relationshipStatus is DELETED
    * Changing this behaviour to exclude related attributes which has relationshipStatus as DELETED
    *
    * Pass allowDeletedRelations with value as true to get all relations back in response
    * (this will include related attributes which has relationshipStatus as DELETED along with ACTIVE ones)
    * */
    private boolean allowDeletedRelations;

    @Override
    public String getQuery() {
        return queryString;
    }

    public void setDsl(Map dsl) {
        this.dsl = dsl;
        queryString = AtlasType.toJson(dsl);
    }

    public boolean isAllowDeletedRelations() {
        return allowDeletedRelations;
    }

    public void setAllowDeletedRelations(boolean allowDeletedRelations) {
        this.allowDeletedRelations = allowDeletedRelations;
    }

    public void setRelationAttributes(Set<String> relationAttributes) {
        this.relationAttributes = relationAttributes;
    }

    @Override
    public String toString() {
        return "IndexSearchParams{" +
                "dsl='" + dsl + '\'' +
                ", queryString='" + queryString + '\'' +
                ", attributes=" + attributes +
                ", relationAttributes=" + relationAttributes +
                '}';
    }
}

package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class IndexSearchParams extends SearchParams {
    private static final Logger LOG = LoggerFactory.getLogger(IndexSearchParams.class);

    private Map dsl;
    private String purpose;
    private String persona;
    private String queryString;
    private String optimizedQueryString;

    /*
    * Indexsearch includes all relations (if requested with param attributes) even if relationshipStatus is DELETED
    * Changing this behaviour to exclude related attributes which has relationshipStatus as DELETED
    *
    * Pass allowDeletedRelations with value as true to get all relations back in response
    * (this will include related attributes which has relationshipStatus as DELETED along with ACTIVE ones)
    * */
    private boolean allowDeletedRelations;
    private boolean accessControlExclusive;
    private boolean includeRelationshipAttributes;

    @Override
    public String getQuery() {
        return queryString;
    }

    public String getOptimizedQueryString() {
        return optimizedQueryString;
    }

    public void setOptimizedQueryString(String optimizedQueryString) {
        this.optimizedQueryString = optimizedQueryString;
    }

    public Map getDsl() {
        return dsl;
    }

    public void setDsl(Map dsl) {
        this.dsl = dsl;
        queryString = AtlasType.toJson(dsl);
    }

    public long getQuerySize() {
        return dsl.get("size") != null ? ((Number)dsl.get("size")).longValue() : 10;
    }

    public boolean isAllowDeletedRelations() {
        return allowDeletedRelations;
    }

    public boolean isAccessControlExclusive() {
        return accessControlExclusive;
    }

    public void setAccessControlExclusive(boolean accessControlExclusive) {
        this.accessControlExclusive = accessControlExclusive;
    }

    public void setAllowDeletedRelations(boolean allowDeletedRelations) {
        this.allowDeletedRelations = allowDeletedRelations;
    }

    public String getPurpose() {
        return purpose;
    }

    public void setPurpose(String purpose) {
        this.purpose = purpose;
    }

    public String getPersona() {
        return persona;
    }

    public void setPersona(String persona) {
        this.persona = persona;
    }

    public void setRelationAttributes(Set<String> relationAttributes) {
        this.relationAttributes = relationAttributes;
    }

    public boolean isIncludeRelationshipAttributes() {
        return includeRelationshipAttributes;
    }

    public void setIncludeRelationshipAttributes(boolean includeRelationshipAttributes) {
        this.includeRelationshipAttributes = includeRelationshipAttributes;
    }

    @Override
    public String toString() {
        return "IndexSearchParams{" +
                "dsl=" + dsl +
                ", purpose='" + purpose + '\'' +
                ", persona='" + persona + '\'' +
                ", queryString='" + queryString + '\'' +
                ", allowDeletedRelations=" + allowDeletedRelations +
                ", accessControlExclusive=" + accessControlExclusive +
                ", includeRelationshipAttributes=" + includeRelationshipAttributes +
                ", utmTags="+ getUtmTags() +
                ", enableFullRestriction="+ enableFullRestriction +
                '}';
    }

    @Override
    public void setQuery(String query) {
        this.queryString = query;
    }

}

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
@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexSearchParams extends SearchParams {
    private static final Logger LOG = LoggerFactory.getLogger(IndexSearchParams.class);

    private static final String FILTER_VAR_NAME_PREFIX = "Atlan.";
    private static final Pattern pattern = Pattern.compile("(?<=" + FILTER_VAR_NAME_PREFIX + ").+?(?=\")");

    private Map query;
    private String queryString;

    @Override
    public String getQuery() {
        return queryString;
    }

    public void setQuery(Map query) {
        this.query = query;
        queryString = parseQueryAttrNames(AtlasType.toJson(query));
    }

    public void setRelationAttributes(Set<String> relationAttributes) {
        this.relationAttributes = relationAttributes;
    }

    @Override
    public String toString() {
        return "IndexSearchParams{" +
                ", query='" + query + '\'' +
                ", queryString='" + queryString + '\'' +
                ", attributes=" + attributes +
                ", relationAttributes=" + relationAttributes +
                '}';
    }

    private String parseQueryAttrNames(String queryString){
        Set<String> keysToMap = new HashSet<>();
        Matcher matcher = pattern.matcher(queryString);
        while (matcher.find()) {
            keysToMap.add(matcher.group());
        }
        for (String key : keysToMap) {
            if (attributeNamesMapping.containsKey(key)) {
                queryString = queryString.replaceAll(FILTER_VAR_NAME_PREFIX + key, attributeNamesMapping.get(key));
            }
        }

        return queryString;
    }

    private static final Map<String, String> attributeNamesMapping = new HashMap<String, String>() {{
        put("guid", "__guid");
        put("createdBy", "__createdBy");
        put("modifiedBy", "__modifiedBy");
        put("timestamp", "__timestamp");
        put("modificationTimestamp", "__modificationTimestamp");
        put("terms", "__terms");
        put("classificationNames", "__classificationNames");
        put("classificationsText", "__classificationsText");
        put("traitNames", "__traitNames");
        put("propagatedTraitNames", "__propagatedTraitNames");
        put("propagatedClassificationNames", "__propagatedClassificationNames");
        put("state", "__state");
        put("typeName", "__typeName");
        put("qualifiedName", "Referenceable.qualifiedName");
        put("name", "Asset.name");
        put("displayName", "Asset.displayName");
        put("description", "Asset.description");
        put("userDescription", "Asset.description");
        put("certificateStatus", "Asset.certificateStatus");
        put("certificateUpdatedBy", "Asset.certificateUpdatedBy");
        put("certificateUpdatedAt", "Asset.certificateUpdatedAt");
        put("bannerTitle", "Asset.bannerTitle");
        put("bannerDescription", "Asset.bannerDescription");
        put("bannerType", "Asset.bannerType");
        put("bannerUpdatedAt", "Asset.bannerUpdatedAt");
        put("bannerUpdatedBy", "Asset.bannerUpdatedBy");
        put("ownerUsers", "Asset.ownerUsers");
        put("ownerGroups", "Asset.ownerGroups");
        put("connnectorName", "Asset.connnectorName");
        put("connectionName", "Asset.connectionName");
        put("connectionQualifiedName", "Asset.connectionQualifiedName");
        put("subType", "Asset.subType");
        put("isDiscoverable", "Asset.isDiscoverable");
        put("isEditable", "Asset.isEditable");
        put("sourceOwners", "Asset.sourceOwners");
        put("sourceCreatedBy", "Asset.sourceCreatedBy");
        put("sourceCreatedAt", "Asset.sourceCreatedAt");
        put("sourceUpdatedAt", "Asset.sourceUpdatedAt");
        put("sourceUpdatedBy", "Asset.sourceUpdatedBy");
        put("lastSyncWorkflowName", "Asset.lastSyncWorkflowName");
        put("lastSyncRunAt", "Asset.lastSyncRunAt");
        put("lastSyncRun", "Asset.lastSyncRun");
        put("viewScore", "Asset.viewScore");
        put("code", "Process.code");
        put("sql", "Process.sql");
    }};
}

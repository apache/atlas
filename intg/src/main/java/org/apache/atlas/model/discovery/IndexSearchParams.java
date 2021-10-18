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

    private static final Map<String, String> attributeNamesMapping = new HashMap<String, String>(){
        {
            put("__classificationNames", "__traitNames");
            put("__propagatedClassificationNames", "__propagatedTraitNames");
            put("qualifiedName", "Referenceable.qualifiedName");
        }};

    private static final Pattern pattern = Pattern.compile("(?<=\").+?(?=\")");

    private Map dsl;
    private String queryString;

    @Override
    public String getQuery() {
        return queryString;
    }

    public void setDsl(Map dsl) {
        this.dsl = dsl;
        queryString = parseQueryAttrNames(AtlasType.toJson(dsl));
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

    private String parseQueryAttrNames(String queryString){
        Set<String> keysToMap = new HashSet<>();
        Matcher matcher = pattern.matcher(queryString);
        while (matcher.find()) {
            keysToMap.add(matcher.group());
        }
        for (String key : keysToMap) {
            if (attributeNamesMapping.containsKey(key)) {
                queryString = queryString.replaceAll(key, attributeNamesMapping.get(key));
            }
        }

        return queryString;
    }
}

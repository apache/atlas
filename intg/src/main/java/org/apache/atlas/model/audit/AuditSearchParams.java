package org.apache.atlas.model.audit;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.type.AtlasType;

import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)

public class AuditSearchParams {

    private Map dsl;

    public void setDsl(Map dsl) {
        this.dsl = dsl;
    }

    public Map getDsl() {
        return this.dsl;
    }

    public String getQueryString(String guid) {
        String queryWithEntityFilter;
        if (dsl.get("query") == null || ((Map) dsl.get("query")).isEmpty()) {
            String queryTemplate = "{\"bool\":{\"minimum_should_match\":\"100%\",\"should\":[{\"term\":{\"entityid\":\"entity_id\"}}]}}";
            queryWithEntityFilter = queryTemplate.replace("entity_id", guid);
        } else {
            String queryTemplate = "{\"bool\":{\"minimum_should_match\":\"100%\",\"should\":[{\"term\":{\"entityid\":\"entity_id\"}}, query_from_payload]}}";
            queryWithEntityFilter = queryTemplate.replace("entity_id", guid);
            String queryValue = AtlasType.toJson(dsl.get("query"));
            queryWithEntityFilter = queryWithEntityFilter.replace("query_from_payload", queryValue);
        }
        dsl.put("query", AtlasType.fromJson(queryWithEntityFilter, Map.class));
        return AtlasType.toJson(dsl);
    }
}

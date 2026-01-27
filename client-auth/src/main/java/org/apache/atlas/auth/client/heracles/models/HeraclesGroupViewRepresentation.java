package org.apache.atlas.auth.client.heracles.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of a Group from Heracles API.
 * Maps to the response from /groups and /groups/v2 endpoints.
 * 
 * API Reference:
 * - GET /groups (listGroups)
 * - GET /groups/v2 (listGroupsV2)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HeraclesGroupViewRepresentation {
    
    protected String id;
    protected String name;

    public static String sortBy = "name";
    
    public HeraclesGroupViewRepresentation() {
    }
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
}


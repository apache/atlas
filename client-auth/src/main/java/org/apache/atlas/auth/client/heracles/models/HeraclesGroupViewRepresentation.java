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
    protected String path;
    protected String realmId;
    protected List<String> users;
    protected List<String> roles;
    protected Map<String, List<String>> attributes;
    
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
    
    public String getPath() {
        return path;
    }
    
    public void setPath(String path) {
        this.path = path;
    }
    
    public String getRealmId() {
        return realmId;
    }
    
    public void setRealmId(String realmId) {
        this.realmId = realmId;
    }
    
    public List<String> getUsers() {
        return users == null ? new ArrayList<>() : users;
    }
    
    public void setUsers(List<String> users) {
        this.users = users;
    }
    
    public List<String> getRoles() {
        return roles == null ? new ArrayList<>() : roles;
    }
    
    public void setRoles(List<String> roles) {
        this.roles = roles;
    }
    
    public Map<String, List<String>> getAttributes() {
        return attributes == null ? new HashMap<>() : attributes;
    }
    
    public void setAttributes(Map<String, List<String>> attributes) {
        this.attributes = attributes;
    }
}


package org.apache.atlas.auth.client.heracles.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HeraclesRoleViewRepresentation {
    protected String id;
    protected String name;
    protected String realmId;
    protected List<String> roles;
    protected List<String> groups;

    public static String sortBy = "name";

    public HeraclesRoleViewRepresentation() {
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getRealmId() {
        return realmId;
    }

    public List<String> getRoles() {
        return roles == null ? new ArrayList<>() : roles;
    }

    public List<String> getGroups() {
        return groups == null ? new ArrayList<>() : groups;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setRealmId(String realmId) {
        this.realmId = realmId;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }
}

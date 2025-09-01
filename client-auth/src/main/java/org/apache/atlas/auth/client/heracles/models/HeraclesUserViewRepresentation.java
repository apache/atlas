package org.apache.atlas.auth.client.heracles.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.keycloak.representations.idm.UserRepresentation;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HeraclesUserViewRepresentation {
    protected  String id;
    protected String username;
    protected boolean enabled;
    protected List<String> roles;
    protected List<String> groups;

    public static String sortBy = "username";

    public HeraclesUserViewRepresentation() {
    }

    public String getId() {
        return id;
    }

    public String getUsername() {
        return username;
    }

    public boolean isEnabled() {
        return enabled;
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

    public void setUsername(String username) {
        this.username = username;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }

}

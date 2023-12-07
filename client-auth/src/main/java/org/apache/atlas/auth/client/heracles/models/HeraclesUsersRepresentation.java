package org.apache.atlas.auth.client.heracles.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.keycloak.representations.idm.UserRepresentation;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HeraclesUsersRepresentation {
    protected int totalRecord;
    protected int filterRecord;
    protected List<HeraclesUserRepresentation> records;
    public static final String USER_PROJECTIONS = "emailVerified,enabled,id,status,username";
    public static final String USER_SORT = "username";

    public HeraclesUsersRepresentation() {
    }

    public HeraclesUsersRepresentation(int totalRecord, int filterRecord, List<HeraclesUserRepresentation> records) {
        this.totalRecord = totalRecord;
        this.filterRecord = filterRecord;
        this.records = records;
    }

    public int getTotalRecord() {
        return totalRecord;
    }

    public void setTotalRecord(int totalRecord) {
        this.totalRecord = totalRecord;
    }

    public int getFilterRecord() {
        return filterRecord;
    }

    public void setFilterRecord(int filterRecord) {
        this.filterRecord = filterRecord;
    }

    public List<HeraclesUserRepresentation> getRecords() {
        return records;
    }

    public void setRecords(List<HeraclesUserRepresentation> records) {
        this.records = records;
    }

    public List<UserRepresentation> toKeycloakUserRepresentations() {
        List<UserRepresentation> userRepresentations = new ArrayList<>();
        for (HeraclesUserRepresentation heraclesUserRepresentation : records) {
            UserRepresentation userRepresentation = new UserRepresentation();
            userRepresentation.setEmailVerified(heraclesUserRepresentation.emailVerified);
            userRepresentation.setEnabled(heraclesUserRepresentation.enabled);
            userRepresentation.setUsername(heraclesUserRepresentation.username);
            userRepresentation.setId(heraclesUserRepresentation.id);
            userRepresentations.add(userRepresentation);
        }
        return userRepresentations;
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class HeraclesUserRepresentation {
    protected boolean emailVerified;
    protected boolean enabled;
    protected String username;
    protected String id;

    public HeraclesUserRepresentation() {
    }

    public HeraclesUserRepresentation(boolean emailVerified, boolean enabled, String username, String id) {
        this.emailVerified = emailVerified;
        this.enabled = enabled;
        this.username = username;
        this.id = id;
    }

    public boolean isEmailVerified() {
        return emailVerified;
    }

    public void setEmailVerified(boolean emailVerified) {
        this.emailVerified = emailVerified;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}

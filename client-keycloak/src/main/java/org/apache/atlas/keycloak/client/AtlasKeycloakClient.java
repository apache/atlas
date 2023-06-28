package org.apache.atlas.keycloak.client;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.keycloak.client.config.KeycloakConfig;
import org.apache.atlas.keycloak.client.config.KeycloakConfigBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.keycloak.representations.idm.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.atlas.ApplicationProperties.ATLAS_CONFIGURATION_DIRECTORY_PROPERTY;

public final class AtlasKeycloakClient {

    public static final Logger LOG = LoggerFactory.getLogger(AtlasKeycloakClient.class);

    private static final String KEYCLOAK_PROPERTIES = "keycloak.json";
    private static final String DEFAULT_GRANT_TYPE = "client_credentials";
    private static final String KEY_REALM_ID = "realm";
    private static final String KEY_AUTH_SERVER_URL = "auth-server-url";
    private static final String KEY_CLIENT_ID = "resource";
    private static final String KEY_CREDENTIALS = "credentials";
    private static final String KEY_SECRET = "secret";

    private static KeycloakRestClient KEYCLOAK;
    private static AtlasKeycloakClient KEYCLOAK_CLIENT;

    private AtlasKeycloakClient() {
    }

    public List<UserRepresentation> searchUserByUserName(String username) throws AtlasBaseException {
        return KEYCLOAK.searchUserByUserName(username).body();
    }

    public Set<UserRepresentation> getRoleUserMembers(String roleName, int start, int size) throws AtlasBaseException {
        return KEYCLOAK.getRoleUserMembers(roleName, start, size).body();
    }

    public List<UserRepresentation> getRoleUserMembers(String roleName) throws AtlasBaseException {
        return KEYCLOAK.getRoleUserMembers(roleName).body();
    }

    public List<UserRepresentation> getAllUsers() throws AtlasBaseException {
        int start = 0;
        int size = 500;
        boolean found = true;

        List<UserRepresentation> ret = new ArrayList<>(0);

        do {
            List<UserRepresentation> userRepresentations = KEYCLOAK.getAllUsers(start, size).body();
            if (CollectionUtils.isNotEmpty(userRepresentations)) {
                ret.addAll(userRepresentations);
                start += size;
            } else {
                found = false;
            }

        } while (found && ret.size() % size == 0);

        return ret;
    }

    public List<GroupRepresentation> searchGroupByName(String groupName, Integer start, Integer size) throws AtlasBaseException {
        return KEYCLOAK.searchGroupByName(groupName, start, size).body();
    }

    public List<GroupRepresentation> getRoleGroupMembers(String roleName) throws AtlasBaseException {
        return KEYCLOAK.getRoleGroupMembers(roleName).body();
    }

    public Set<GroupRepresentation> getRoleGroupMembers(String roleName, Integer start, Integer size) throws AtlasBaseException {
        return KEYCLOAK.getRoleGroupMembers(roleName, start, size).body();
    }

    public List<GroupRepresentation> getGroupsForUserById(String userId) throws AtlasBaseException {
        return KEYCLOAK.getGroupsForUserById(userId).body();
    }


    public List<GroupRepresentation> getAllGroups() throws AtlasBaseException {
        int start = 0;
        int size = 500;
        boolean found = true;

        List<GroupRepresentation> ret = new ArrayList<>(0);
        do {
            List<GroupRepresentation> groupRepresentations = KEYCLOAK.getAllGroups(start, size).body();
            if (CollectionUtils.isNotEmpty(groupRepresentations)) {
                ret.addAll(groupRepresentations);
                start += size;
            } else {
                found = false;
            }
        } while (found && ret.size() % size == 0);

        return ret;
    }

    public void addRealmLevelRoleMappingsForGroup(String groupId, List<RoleRepresentation> roles) throws AtlasBaseException {
        KEYCLOAK.addRealmLevelRoleMappingsForGroup(groupId, roles);
    }

    public void deleteRealmLevelRoleMappingsForGroup(String groupId, List<RoleRepresentation> roles) throws AtlasBaseException {
        KEYCLOAK.deleteRealmLevelRoleMappingsForGroup(groupId, roles);
    }

    public List<RoleRepresentation> getAllRoles() throws AtlasBaseException {
        int start = 0;
        int size = 500;
        boolean found = true;

        List<RoleRepresentation> ret = new ArrayList<>(0);
        do {
            List<RoleRepresentation> roleRepresentations = KEYCLOAK.getAllRoles(start, size).body();
            if (CollectionUtils.isNotEmpty(roleRepresentations)) {
                ret.addAll(roleRepresentations);
                start += size;
            } else {
                found = false;
            }
        } while (found && ret.size() % size == 0);

        return ret;
    }

    public void deleteRoleById(String roleId) throws AtlasBaseException {
        KEYCLOAK.deleteRoleById(roleId);
    }

    public void deleteRoleByName(String roleName) throws AtlasBaseException {
        KEYCLOAK.deleteRoleByName(roleName);
    }


    public List<RoleRepresentation> addRealmLevelRoleMappingsForUser(String userId, List<RoleRepresentation> roles) throws AtlasBaseException {
        return KEYCLOAK.addRealmLevelRoleMappingsForUser(userId, roles).body();
    }

    public void deleteRealmLevelRoleMappingsForUser(String userId, List<RoleRepresentation> roles) throws AtlasBaseException {
        KEYCLOAK.deleteRealmLevelRoleMappingsForUser(userId, roles);
    }

    public void createRole(RoleRepresentation roleRepresentation) throws AtlasBaseException {
        KEYCLOAK.createRole(roleRepresentation);
    }

    public void updateRole(String roleId, RoleRepresentation roleRepresentation) throws AtlasBaseException {
        KEYCLOAK.updateRole(roleId, roleRepresentation);
    }


    public RoleRepresentation getRoleById(String roleId) throws AtlasBaseException {
        return KEYCLOAK.getRoleById(roleId).body();
    }

    public RoleRepresentation getRoleByName(String roleName) throws AtlasBaseException {
        return KEYCLOAK.getRoleByName(roleName).body();
    }

    public Set<RoleRepresentation> getRoleComposites(String roleName) throws AtlasBaseException {
        return KEYCLOAK.getRoleComposites(roleName).body();
    }

    public static AtlasKeycloakClient getKeycloakClient() throws AtlasBaseException {
        if (Objects.isNull(KEYCLOAK_CLIENT)) {
            LOG.info("Initializing Keycloak client..");
            try {
                init(getConfig());
            } catch (IOException e) {
                LOG.error("Failed to fetch Keycloak conf {}", e.getMessage());
                throw new AtlasBaseException(AtlasErrorCode.KEYCLOAK_INIT_FAILED, e.getMessage());
            } catch (JSONException e) {
                LOG.error("Failed to parse Keycloak conf {}", e.getMessage());
                throw new AtlasBaseException(AtlasErrorCode.KEYCLOAK_INIT_FAILED, e.getMessage());
            } catch (Exception e) {
                LOG.error("Failed to connect to Keycloak {}", e.getMessage());
                throw new AtlasBaseException(AtlasErrorCode.KEYCLOAK_INIT_FAILED, e.getMessage());
            }

            LOG.info("Initialized Keycloak client..");
        }

        return KEYCLOAK_CLIENT;
    }

    public void reInit() {
        KEYCLOAK.close();
        init(KEYCLOAK.getConfig());
    }

    private static void init(KeycloakConfig config) {
        synchronized (AtlasKeycloakClient.class) {
            if (KEYCLOAK_CLIENT == null) {
                KEYCLOAK = new KeycloakRestClient(config);
                KEYCLOAK_CLIENT = new AtlasKeycloakClient();
            }
        }
    }

    private static KeycloakConfig getConfig() throws Exception {
        String confLocation = System.getProperty(ATLAS_CONFIGURATION_DIRECTORY_PROPERTY);
        File confFile;
        if (StringUtils.isNotEmpty(confLocation)) {
            confFile = new File(confLocation, KEYCLOAK_PROPERTIES);

            if (confFile.exists()) {
                String keyConf = new String(Files.readAllBytes(confFile.toPath()), StandardCharsets.UTF_8);
                JSONObject object = new JSONObject(keyConf);

                String REALM_ID = object.getString(KEY_REALM_ID);
                String AUTH_SERVER_URL = object.getString(KEY_AUTH_SERVER_URL);
                String CLIENT_ID = object.getString(KEY_CLIENT_ID);
                String GRANT_TYPE = DEFAULT_GRANT_TYPE;
                String CLIENT_SECRET = object.getJSONObject(KEY_CREDENTIALS).getString(KEY_SECRET);

                LOG.info("Keycloak conf: REALM_ID:{}, AUTH_SERVER_URL:{}", REALM_ID, AUTH_SERVER_URL);
                return KeycloakConfigBuilder.builder().realId(REALM_ID).authServerUrl(AUTH_SERVER_URL).clientId(CLIENT_ID).grantType(GRANT_TYPE).clientSecret(CLIENT_SECRET).build();
            } else {
                throw new AtlasBaseException(AtlasErrorCode.KEYCLOAK_INIT_FAILED, "Keycloak configuration file not found in location " + confLocation);
            }
        } else {
            throw new AtlasBaseException(AtlasErrorCode.KEYCLOAK_INIT_FAILED, "Configuration location not found " + confLocation);
        }
    }

    public void addComposites(String roleName, List<RoleRepresentation> roles) throws AtlasBaseException {
        KEYCLOAK.addComposites(roleName, roles);
    }

    public void deleteComposites(String roleName, List<RoleRepresentation> roles) throws AtlasBaseException {
        KEYCLOAK.deleteComposites(roleName, roles);
    }

    public List<AdminEventRepresentation> getAdminEvents(List<String> operationTypes, String authRealm, String authClient, String authUser, String authIpAddress, String resourcePath, String dateFrom, String dateTo, Integer first, Integer max) throws AtlasBaseException {
        return KEYCLOAK.getAdminEvents(operationTypes, authRealm, authClient, authUser, authIpAddress, resourcePath, dateFrom, dateTo, first, max).body();
    }

    public List<EventRepresentation> getEvents(List<String> type, String client, String user, String dateFrom, String dateTo, String ipAddress, Integer first, Integer max) throws AtlasBaseException {
        return KEYCLOAK.getEvents(type, client, user, dateFrom, dateTo, ipAddress, first, max).body();
    }
}

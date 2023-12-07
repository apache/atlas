package org.apache.atlas.auth.client.config;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;

import static org.apache.atlas.ApplicationProperties.ATLAS_CONFIGURATION_DIRECTORY_PROPERTY;

public class AuthConfig {
    private static final Logger LOG = LoggerFactory.getLogger(AuthConfig.class);

    public String authServerUrl;
    public String realmId;
    public String clientId;
    public String clientSecret;
    public String grantType;
    public String heraclesApiServerUrl;

    private static final String KEYCLOAK_PROPERTIES = "keycloak.json";
    private static final String DEFAULT_GRANT_TYPE = "client_credentials";
    private static final String KEY_REALM_ID = "realm";
    private static final String KEY_AUTH_SERVER_URL = "auth-server-url";
    private static final String KEY_CLIENT_ID = "resource";
    private static final String KEY_CREDENTIALS = "credentials";
    private static final String KEY_SECRET = "secret";

    public String getAuthServerUrl() {
        return authServerUrl;
    }

    public String getRealmId() {
        return realmId;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getGrantType() {
        return grantType;
    }

    public String getHeraclesApiServerUrl() {
        return heraclesApiServerUrl;
    }

    public static AuthConfig getConfig() throws AtlasBaseException {
        String confLocation = System.getProperty(ATLAS_CONFIGURATION_DIRECTORY_PROPERTY);
        Optional<File> confFile = getConfigurationFile(confLocation);

        if (confFile.isPresent()) {
            try {
                JSONObject object = new JSONObject(readFileToString(confFile.get()));
                return buildAuthConfigFromJson(object);
            } catch (Exception e) {
                LOG.error("Error parsing Keycloak configuration: ", e);
                throw new AtlasBaseException(AtlasErrorCode.KEYCLOAK_INIT_FAILED, "Error parsing Keycloak configuration");
            }
        } else {
            throw new AtlasBaseException(AtlasErrorCode.KEYCLOAK_INIT_FAILED, "Keycloak configuration file not found in location " + confLocation);
        }
    }

    private static Optional<File> getConfigurationFile(String confLocation) {
        if (StringUtils.isNotEmpty(confLocation)) {
            File confFile = new File(confLocation, KEYCLOAK_PROPERTIES);
            if (confFile.exists()) {
                return Optional.of(confFile);
            }
        }
        return Optional.empty();
    }

    private static String readFileToString(File file) throws Exception {
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }

    private static AuthConfig buildAuthConfigFromJson(JSONObject object) throws Exception {
        String realmId = object.getString(KEY_REALM_ID);
        String authServerUrl = object.getString(KEY_AUTH_SERVER_URL) + "/";
        String clientId = object.getString(KEY_CLIENT_ID);
        String grantType = DEFAULT_GRANT_TYPE;
        String clientSecret = object.getJSONObject(KEY_CREDENTIALS).getString(KEY_SECRET);

        LOG.info("Keycloak configuration: REALM_ID:{}, AUTH_SERVER_URL:{}", realmId, authServerUrl);
        return AuthConfigBuilder.builder().realId(realmId).authServerUrl(authServerUrl).clientId(clientId).grantType(grantType).clientSecret(clientSecret).build();
    }
}

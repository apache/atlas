package org.apache.atlas.auth.client.config;

import org.apache.atlas.AtlasConfiguration;

public class AuthConfigBuilder {

    private String authServerUrl;
    private String realmId;
    private String clientId;
    private String clientSecret;
    private String grantType = "client_credentials";

    private AuthConfigBuilder() {
    }

    public static AuthConfigBuilder builder() {
        return new AuthConfigBuilder();
    }

    public AuthConfigBuilder authServerUrl(String authServerUrl) {
        this.authServerUrl = authServerUrl;
        return this;
    }

    public AuthConfigBuilder realId(String realId) {
        this.realmId = realId;
        return this;
    }

    public AuthConfigBuilder clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public AuthConfigBuilder clientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
        return this;
    }

    public AuthConfigBuilder grantType(String grantType) {
        this.grantType = grantType;
        return this;
    }

    public AuthConfig build() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.authServerUrl = authServerUrl;
        authConfig.realmId = realmId;
        authConfig.clientId = clientId;
        authConfig.clientSecret = clientSecret;
        authConfig.grantType = grantType;
        authConfig.heraclesApiServerUrl= AtlasConfiguration.HERACLES_API_SERVER_URL.getString()+"/";
        return authConfig;
    }
}

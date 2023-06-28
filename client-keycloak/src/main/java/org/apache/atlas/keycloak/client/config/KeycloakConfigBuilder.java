package org.apache.atlas.keycloak.client.config;

public final class KeycloakConfigBuilder {

    private String authServerUrl;
    private String realId;
    private String clientId;
    private String clientSecret;
    private String grantType = "client_credentials";

    private KeycloakConfigBuilder() {
    }

    public static KeycloakConfigBuilder builder() {
        return new KeycloakConfigBuilder();
    }

    public KeycloakConfigBuilder authServerUrl(String authServerUrl) {
        this.authServerUrl = authServerUrl;
        return this;
    }

    public KeycloakConfigBuilder realId(String realId) {
        this.realId = realId;
        return this;
    }

    public KeycloakConfigBuilder clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public KeycloakConfigBuilder clientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
        return this;
    }

    public KeycloakConfigBuilder grantType(String grantType) {
        this.grantType = grantType;
        return this;
    }

    public KeycloakConfig build() {
        KeycloakConfig keycloakConfig = new KeycloakConfig();
        keycloakConfig.authServerUrl = authServerUrl;
        keycloakConfig.realmId = realId;
        keycloakConfig.clientId = clientId;
        keycloakConfig.clientSecret = clientSecret;
        keycloakConfig.grantType = grantType;
        return keycloakConfig;
    }
}
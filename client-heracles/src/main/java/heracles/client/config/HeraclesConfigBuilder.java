package main.java.heracles.client.config;

public class HeraclesConfigBuilder {

    String authServerUrl;
    String realmId;
    String clientId;
    String clientSecret;
    String grantType;

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
}

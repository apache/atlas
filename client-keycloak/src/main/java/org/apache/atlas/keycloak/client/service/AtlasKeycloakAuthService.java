package org.apache.atlas.keycloak.client.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.apache.atlas.keycloak.client.RetrofitKeycloakClient;
import org.apache.atlas.keycloak.client.config.KeycloakConfig;
import org.jetbrains.annotations.NotNull;
import org.keycloak.representations.AccessTokenResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.time.OffsetDateTime;
import java.util.Objects;

public final class AtlasKeycloakAuthService {

    public final static Logger LOG = LoggerFactory.getLogger(AtlasKeycloakAuthService.class);

    private final static String GRANT_TYPE = "grant_type";
    public static final String CLIENT_ID = "client_id";
    public static final String CLIENT_SECRET = "client_secret";
    private static final long DEFAULT_MIN_VALIDITY = 30;
    public static final int EXPIRY_OFFSET = 300;

    private final RetrofitKeycloakClient retrofit;
    private final KeycloakConfig keycloakConfig;
    private AccessTokenResponse currentAccessToken;
    private long expirationTime = -1;

    public AtlasKeycloakAuthService(KeycloakConfig keycloakConfig) {
        this.keycloakConfig = keycloakConfig;
        this.retrofit = new Retrofit.Builder().client(getOkHttpClient())
                .baseUrl(this.keycloakConfig.getAuthServerUrl())
                .addConverterFactory(JacksonConverterFactory.create(new ObjectMapper())).build()
                .create(RetrofitKeycloakClient.class);
    }

    @NotNull
    private OkHttpClient getOkHttpClient() {
        HttpLoggingInterceptor interceptor = new HttpLoggingInterceptor();
        interceptor.setLevel(HttpLoggingInterceptor.Level.BODY);
        return new OkHttpClient.Builder()
                .addInterceptor(interceptor)
                .addInterceptor(errorHandlingInterceptor).build();
    }

    Interceptor errorHandlingInterceptor = chain -> {
        Request request = chain.request();
        okhttp3.Response response = chain.proceed(request);
        if (!response.isSuccessful()) {
            LOG.error("Request for url {} failed:, {}", request.url(), new String(response.body().bytes()));
        }
        return response;
    };

    public String getAuthToken() {
        if (Objects.nonNull(currentAccessToken)) {
            return currentAccessToken.getToken();
        }
        synchronized (this) {
            if (Objects.isNull(currentAccessToken)) {
                try {
                    currentAccessToken = this.retrofit.grantToken(this.keycloakConfig.getRealmId(), getTokenRequest()).execute().body();
                    expirationTime = currentTime() + currentAccessToken.getExpiresIn() - EXPIRY_OFFSET;
                } catch (Exception e) {
                    LOG.error("Error while fetching access token for keycloak client.", e);
                    throw new RuntimeException(e);
                }
            }
        }
        return currentAccessToken.getToken();
    }

    public boolean isTokenExpired() {
        return (currentTime() + DEFAULT_MIN_VALIDITY) >= expirationTime;
    }

    public long getExpiryTime() {
        if (Objects.isNull(currentAccessToken)) {
            return -1;
        }
        return expirationTime;
    }

    private RequestBody getTokenRequest() {
        return new FormBody.Builder().addEncoded(CLIENT_ID, this.keycloakConfig.getClientId()).addEncoded(CLIENT_SECRET, this.keycloakConfig.getClientSecret()).addEncoded(GRANT_TYPE, this.keycloakConfig.getGrantType()).build();
    }

    private long currentTime() {
        return OffsetDateTime.now().toEpochSecond();
    }

}

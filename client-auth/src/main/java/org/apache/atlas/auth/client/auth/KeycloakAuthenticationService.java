package org.apache.atlas.auth.client.auth;

import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.apache.atlas.auth.client.config.AuthConfig;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.auth.client.keycloak.RetrofitKeycloakClient;
import org.jetbrains.annotations.NotNull;
import org.keycloak.representations.AccessTokenResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;

public final class KeycloakAuthenticationService {

    public final static Logger LOG = LoggerFactory.getLogger(KeycloakAuthenticationService.class);

    private final static String GRANT_TYPE = "grant_type";
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final int EXPIRY_OFFSET_SEC = 600;
    private static final int TIMEOUT_IN_SECS = 60;

    private final RetrofitKeycloakClient retrofit;
    private final AuthConfig authConfig;
    private AccessTokenResponse currentAccessToken;
    private long expirationTime = -1;

    public KeycloakAuthenticationService(AuthConfig authConfig) {
        this.authConfig = authConfig;
        this.retrofit = new Retrofit.Builder().client(getOkHttpClient())
                .baseUrl(this.authConfig.getAuthServerUrl())
                .addConverterFactory(JacksonConverterFactory.create(ObjectMapperUtils.KEYCLOAK_OBJECT_MAPPER)).build()
                .create(RetrofitKeycloakClient.class);
    }

    @NotNull
    private OkHttpClient getOkHttpClient() {
        HttpLoggingInterceptor interceptor = new HttpLoggingInterceptor();
        interceptor.setLevel(HttpLoggingInterceptor.Level.NONE);
        return new OkHttpClient.Builder()
                .addInterceptor(interceptor)
                .addInterceptor(responseLoggingInterceptor)
                .connectTimeout(TIMEOUT_IN_SECS, TimeUnit.SECONDS)
                .callTimeout(TIMEOUT_IN_SECS, TimeUnit.SECONDS)
                .writeTimeout(TIMEOUT_IN_SECS, TimeUnit.SECONDS)
                .readTimeout(TIMEOUT_IN_SECS, TimeUnit.SECONDS)
                .build();
    }

    Interceptor responseLoggingInterceptor = chain -> {
        Request request = chain.request();
        okhttp3.Response response = chain.proceed(request);
        LOG.info("Auth Client: Auth Request for url {} Status: {}", request.url(), response.code());
        return response;
    };

    public String getAuthToken() {
        if (!isTokenExpired()) {
            return currentAccessToken.getToken();
        }
        synchronized (this) {
            if (isTokenExpired()) {
                try {
                    retrofit2.Response<AccessTokenResponse> resp = this.retrofit.grantToken(this.authConfig.getRealmId(), getTokenRequest()).execute();
                    if (resp.isSuccessful()) {
                        currentAccessToken = resp.body();
                        expirationTime = currentTime() + currentAccessToken.getExpiresIn() - EXPIRY_OFFSET_SEC;
                        LOG.info("Auth Client: Auth token fetched with expiry:{} sec", expirationTime);
                    } else {
                        throw new AtlasBaseException(BAD_REQUEST, resp.errorBody().string());
                    }
                } catch (Exception e) {
                    LOG.error("Auth Client: Error while fetching access token for keycloak client.", e);
                    throw new RuntimeException(e);
                }
            }
        }
        return currentAccessToken.getToken();
    }

    public boolean isTokenExpired() {
        synchronized (this) {
            if (Objects.isNull(currentAccessToken)) {
                return true;
            }
            return currentTime() >= expirationTime;
        }
    }

    private RequestBody getTokenRequest() {
        return new FormBody.Builder().addEncoded(CLIENT_ID, this.authConfig.getClientId()).addEncoded(CLIENT_SECRET, this.authConfig.getClientSecret()).addEncoded(GRANT_TYPE, this.authConfig.getGrantType()).build();
    }

    private long currentTime() {
        return OffsetDateTime.now().toEpochSecond();
    }

}

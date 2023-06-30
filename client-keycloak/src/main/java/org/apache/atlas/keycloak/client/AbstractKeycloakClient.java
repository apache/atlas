package org.apache.atlas.keycloak.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.keycloak.client.config.KeycloakConfig;
import org.apache.atlas.keycloak.client.service.AtlasKeycloakAuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.lang.NonNull;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.RESOURCE_NOT_FOUND;

abstract class AbstractKeycloakClient {

    private final static Logger LOG = LoggerFactory.getLogger(AbstractKeycloakClient.class);
    private static final Map<Integer, AtlasErrorCode> ERROR_CODE_MAP = new HashMap<>();

    private static final int DEFAULT_KEYCLOAK_RETRY = 3;
    public static final String AUTHORIZATION = "Authorization";
    public static final String BEARER = "Bearer ";
    public static final String X_METASTORE_REQUEST_ID = "X-Metastore-Request-Id";
    public static final String TRACE_ID = "trace_id";

    protected final KeycloakConfig keycloakConfig;
    protected final RetrofitKeycloakClient retrofit;

    private final AtlasKeycloakAuthService authService;
    private String currentToken;

    static {
        ERROR_CODE_MAP.put(HTTP_NOT_FOUND, RESOURCE_NOT_FOUND);
        ERROR_CODE_MAP.put(HTTP_BAD_REQUEST, BAD_REQUEST);
    }

    public AbstractKeycloakClient(KeycloakConfig keycloakConfig) {
        this.keycloakConfig = keycloakConfig;
        HttpLoggingInterceptor interceptor = new HttpLoggingInterceptor();
        interceptor.setLevel(HttpLoggingInterceptor.Level.BODY);
        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .addInterceptor(accessTokenInterceptor)
                .authenticator(authInterceptor)
                .addInterceptor(errorHandlingInterceptor)
                .connectTimeout(60, TimeUnit.SECONDS)
                .callTimeout(60, TimeUnit.SECONDS)
                .writeTimeout(60, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS)
                .build();
        this.retrofit = new Retrofit.Builder().client(okHttpClient)
                .baseUrl(this.keycloakConfig.getAuthServerUrl())
                .addConverterFactory(JacksonConverterFactory.create(new ObjectMapper())).build()
                .create(RetrofitKeycloakClient.class);
        authService = new AtlasKeycloakAuthService(keycloakConfig);
    }

    /**
     * Basic interceptor for logging.
     */
    Interceptor errorHandlingInterceptor = chain -> {
        Request request = chain.request();
        okhttp3.Response response = chain.proceed(request);
        LOG.info("Keycloak: Request for url {} Status:{}, {}:{}", request.url(), response.code(), X_METASTORE_REQUEST_ID, request.header(X_METASTORE_REQUEST_ID));
        return response;
    };

    /**
     * Called for every request made to keycloak
     */
    Interceptor accessTokenInterceptor = new Interceptor() {
        @NonNull
        @Override
        public Response intercept(@NonNull Chain chain) throws IOException {
            if (Objects.isNull(currentToken) || authService.isTokenExpired()) {
                synchronized (this) {
                    if (Objects.isNull(currentToken)) {
                        currentToken = authService.getAuthToken();
                    }
                }
            }
            Request request = chain.request().newBuilder()
                    .header(AUTHORIZATION, BEARER + currentToken)
                    .header(X_METASTORE_REQUEST_ID, MDC.get(TRACE_ID))
                    .build();
            return chain.proceed(request);
        }
    };

    /**
     * Called only during auth failures.
     */
    Authenticator authInterceptor = new Authenticator() {
        @Override
        public Request authenticate(Route route, @NonNull Response response) {
            if (responseCount(response) > DEFAULT_KEYCLOAK_RETRY) {
                LOG.warn("Keycloak: Falling back, retried {} times", DEFAULT_KEYCLOAK_RETRY);
                return null;
            }
            LOG.info("Keycloak: Current keycloak token status, Expired: {}", authService.isTokenExpired());
            synchronized (this) {
                if (Objects.isNull(currentToken)) {
                    currentToken = authService.getAuthToken();
                    LOG.info("Keycloak: Token refreshed, with validity:{} sec", authService.getExpiryTime());
                }
                return response.request().newBuilder()
                        .addHeader(AUTHORIZATION, BEARER + currentToken)
                        .header(X_METASTORE_REQUEST_ID, MDC.get(TRACE_ID))
                        .build();
            }
        }

        private int responseCount(Response response) {
            int retryCount = 1;
            while ((response = response.priorResponse()) != null) {
                retryCount++;
            }
            return retryCount;
        }

    };

    /**
     * Process incoming request, and error handling.
     */
    protected <T> retrofit2.Response<T> processResponse(retrofit2.Call<T> req) throws AtlasBaseException {
        try {
            retrofit2.Response<T> response = req.execute();
            if (Objects.isNull(response.errorBody())) {
                return response;
            }
            String errMsg = response.errorBody().string();
            LOG.error("Keycloak: Client request processing failed code {} message:{}", response.code(), errMsg);
            throw new AtlasBaseException(ERROR_CODE_MAP.get(response.code()), errMsg);
        } catch (Exception e) {
            LOG.error("Keycloak: request failed", e);
            throw new AtlasBaseException(BAD_REQUEST, "Keycloak request failed");
        }
    }

}

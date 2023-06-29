package org.apache.atlas.keycloak.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.keycloak.client.config.KeycloakConfig;
import org.apache.atlas.keycloak.client.service.AtlasKeycloakAuthService;
import org.keycloak.representations.idm.ErrorRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.RESOURCE_NOT_FOUND;

abstract class AbstractKeycloakClient {

    private final static Logger LOG = LoggerFactory.getLogger(AbstractKeycloakClient.class);
    private static final int DEFAULT_KEYCLOAK_RETRY = 3;
    public static final String AUTHORIZATION = "Authorization";
    public static final String BEARER = "Bearer ";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<Integer, AtlasErrorCode> ERROR_CODE_MAP = new HashMap<>();

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
                .addInterceptor(errorHandlingInterceptor).build();
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
        if (!response.isSuccessful()) {
            LOG.error("Request for url {} failed:, {}", request.url(), new String(response.body().bytes()));
        }
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
            Request request = chain.request().newBuilder().header(AUTHORIZATION, BEARER + currentToken).build();
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
                LOG.warn("Falling back, retried {} times", DEFAULT_KEYCLOAK_RETRY);
                return null;
            }
            LOG.info("Current keycloak token status, Expired: {}", authService.isTokenExpired());
            synchronized (this) {
                if (Objects.isNull(currentToken)) {
                    currentToken = authService.getAuthToken();
                    LOG.info("Keycloak token refreshed, with validity:{} sec", authService.getExpiryTime());
                }
                return newRequestWithAccessToken(response.request(), currentToken);
            }
        }

        private int responseCount(Response response) {
            int retryCount = 1;
            while ((response = response.priorResponse()) != null) {
                retryCount++;
            }
            return retryCount;
        }

        private Request newRequestWithAccessToken(@NonNull Request request, @NonNull String accessToken) {
            return request.newBuilder().header(AUTHORIZATION, BEARER + accessToken).build();
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
            ErrorRepresentation err = MAPPER.readValue(response.errorBody().byteStream(), ErrorRepresentation.class);
            LOG.error("Keycloak client request processing failed code {} message:, {}", response.code(), err.getErrorMessage());
            throw new AtlasBaseException(ERROR_CODE_MAP.get(response.code()), err.getErrorMessage());
        } catch (Exception e) {
            LOG.error("Processing keycloak error messaged failed", e);
            throw new AtlasBaseException(e);
        }
    }

}

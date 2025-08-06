package org.apache.atlas.auth.client.auth;

import io.micrometer.core.instrument.Timer;
import org.apache.atlas.auth.client.config.AuthConfig;
import org.apache.atlas.auth.client.heracles.RetrofitHeraclesClient;
import org.apache.atlas.auth.client.keycloak.RetrofitKeycloakClient;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.metrics.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

public class AbstractAuthClient {

    private final static Logger LOG = LoggerFactory.getLogger(AbstractAuthClient.class);
    private static final Map<Integer, AtlasErrorCode> ERROR_CODE_MAP = new HashMap<>();

    private static final int DEFAULT_RETRY = 3;
    private static final String AUTHORIZATION = "Authorization";
    private static final String BEARER = "Bearer ";
    private static final int TIMEOUT_IN_SEC = 60;
    private static final String INTEGRATION = "integration";
    private static final String KEYCLOAK = "keycloak";

    protected final AuthConfig authConfig;
    protected final RetrofitKeycloakClient retrofitKeycloakClient;
    protected final RetrofitHeraclesClient retrofitHeraclesClient;

    private final KeycloakAuthenticationService authService;
    private MetricUtils metricUtils = null;

    static {
        ERROR_CODE_MAP.put(HTTP_NOT_FOUND, RESOURCE_NOT_FOUND);
        ERROR_CODE_MAP.put(HTTP_BAD_REQUEST, BAD_REQUEST);
    }

    public AbstractAuthClient(AuthConfig authConfig) {
        this.authConfig = authConfig;
        this.metricUtils = new MetricUtils();
        HttpLoggingInterceptor httpInterceptor = new HttpLoggingInterceptor();
        httpInterceptor.setLevel(HttpLoggingInterceptor.Level.NONE);
        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .addInterceptor(accessTokenInterceptor)
                .addInterceptor(httpInterceptor)
                .addInterceptor(responseLoggingInterceptor)
                .authenticator(authInterceptor)
                .connectTimeout(TIMEOUT_IN_SEC, TimeUnit.SECONDS)
                .callTimeout(TIMEOUT_IN_SEC, TimeUnit.SECONDS)
                .writeTimeout(TIMEOUT_IN_SEC, TimeUnit.SECONDS)
                .readTimeout(TIMEOUT_IN_SEC, TimeUnit.SECONDS)
                .build();
        this.retrofitKeycloakClient = new Retrofit.Builder().client(okHttpClient)
                .baseUrl(this.authConfig.getAuthServerUrl())
                .addConverterFactory(JacksonConverterFactory.create(ObjectMapperUtils.KEYCLOAK_OBJECT_MAPPER)).build()
                .create(RetrofitKeycloakClient.class);
        this.retrofitHeraclesClient = new Retrofit.Builder().client(okHttpClient)
                .baseUrl(this.authConfig.getHeraclesApiServerUrl())
                .addConverterFactory(JacksonConverterFactory.create(ObjectMapperUtils.HERACLES_OBJECT_MAPPER)).build()
                .create(RetrofitHeraclesClient.class);
        authService = new KeycloakAuthenticationService(authConfig);
    }

    /**
     * Basic interceptor for logging.
     */
    Interceptor responseLoggingInterceptor = chain -> {
        Request request = chain.request();
        String rawPath = request.url().uri().getRawPath();
        Timer.Sample timerSample = this.metricUtils.start(rawPath);
        okhttp3.Response response = chain.proceed(request);
        this.metricUtils.recordHttpTimer(timerSample, request.method(), rawPath, response.code(),
                INTEGRATION, KEYCLOAK);
        return response;
    };

    /**
     * Called for every request made to keycloak
     */
    Interceptor accessTokenInterceptor = new Interceptor() {
        @NonNull
        @Override
        public Response intercept(@NonNull Chain chain) throws IOException {
            Request request = chain.request().newBuilder()
                    .header(AUTHORIZATION, BEARER + authService.getAuthToken())
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
            if (responseCount(response) > DEFAULT_RETRY) {
                LOG.warn("Auth Client: Falling back, retried {} times", DEFAULT_RETRY);
                return null;
            }
            LOG.info("Auth Client: Current keycloak token status, Expired: {}", authService.isTokenExpired());
            return response.request().newBuilder()
                    .addHeader(AUTHORIZATION, BEARER + authService.getAuthToken())
                    .build();
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
            LOG.error("Auth Client: Client request processing failed code {} message:{}, request: {} {}",
                    response.code(), errMsg, req.request().method(), req.request().url());
            throw new AtlasBaseException(ERROR_CODE_MAP.getOrDefault(response.code(), BAD_REQUEST), errMsg);
        } catch (Exception e) {
            LOG.error("Auth Client: request failed, request: {} {}, Exception: {}", req.request().method(), req.request().url(), e);
            throw new AtlasBaseException(BAD_REQUEST, "Auth request failed");
        }
    }


}

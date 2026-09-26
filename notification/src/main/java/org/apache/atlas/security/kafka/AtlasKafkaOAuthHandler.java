/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.security.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kafka SASL/OAUTHBEARER callback handler for Kubernetes projected service-account
 * tokens. Reads a JWT from a {@code file://} URL configured via
 * {@link #SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL} and supplies it to the
 * Kafka client without OAuth 2.0 scope validation. The broker validates the JWT.
 * <p>
 * Kubernetes rotates projected SA tokens in place (same path, new contents). Kafka's
 * {@code ExpiringCredentialRefreshingLogin} re-invokes this handler on each login /
 * refresh; {@link #handle(Callback[])} always re-reads the token file and builds a
 * new {@link OAuthBearerToken} from the current JWT (including updated {@code exp}).
 * Token bytes are never cached between callbacks.
 */
public class AtlasKafkaOAuthHandler implements AuthenticateCallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasKafkaOAuthHandler.class);

    /** Kafka client config key for the OAuth bearer token endpoint / file URL. */
    static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL = "sasl.oauthbearer.token.endpoint.url";

    /** Retries while kubelet atomically replaces the projected token file. */
    private static final int TOKEN_READ_MAX_ATTEMPTS = 5;
    private static final long TOKEN_READ_RETRY_SLEEP_MS = 50L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Path tokenFilePath;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        Object tokenEndpointUrl = configs.get(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        if (tokenEndpointUrl == null) {
            throw new IllegalArgumentException("Missing required Kafka config: " + SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
        }

        tokenFilePath = resolveTokenFilePath(tokenEndpointUrl.toString());
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        if (tokenFilePath == null) {
            throw new IllegalStateException("AtlasKafkaOAuthHandler is not configured");
        }

        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                handleTokenCallback((OAuthBearerTokenCallback) callback);
            } else if (callback instanceof SaslExtensionsCallback) {
                ((SaslExtensionsCallback) callback).extensions(SaslExtensions.empty());
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    @Override
    public void close() {
        tokenFilePath = null;
    }

    private void handleTokenCallback(OAuthBearerTokenCallback callback) throws IOException {
        String rawToken = readTokenFromProjectedVolume();

        JwtClaims claims = parseJwtClaims(rawToken);
        callback.token(new AtlasKafkaOAuthBearerToken(rawToken, claims));

        LOG.info("AtlasKafkaOAuthHandler: token set OK for principal={}, lifetimeMs={}",
                claims.principalName, claims.lifetimeMs);
    }

    /**
     * Reads the current JWT from the projected volume. Called on every Kafka OAuth login / refresh.
     */
    private String readTokenFromProjectedVolume() throws IOException {
        IOException lastFailure = null;

        for (int attempt = 1; attempt <= TOKEN_READ_MAX_ATTEMPTS; attempt++) {
            try {
                String rawToken = readTokenOnce();

                parseJwtClaims(rawToken);

                return rawToken;
            } catch (IOException e) {
                lastFailure = e;

                if (attempt < TOKEN_READ_MAX_ATTEMPTS) {
                    LOG.debug("K8sOAuth: token read/parse failed (attempt {}/{}), retrying: {}",
                            attempt, TOKEN_READ_MAX_ATTEMPTS, e.getMessage());

                    sleepBeforeTokenReadRetry();
                }
            }
        }

        throw lastFailure != null ? lastFailure : new IOException("Failed to read projected SA token from " + tokenFilePath);
    }

    private String readTokenOnce() throws IOException {
        byte[] tokenBytes = Files.readAllBytes(tokenFilePath);
        String rawToken   = new String(tokenBytes, StandardCharsets.UTF_8).trim();

        if (rawToken.isEmpty()) {
            throw new IOException("Projected SA token file is empty: " + tokenFilePath);
        }

        return rawToken;
    }

    private static void sleepBeforeTokenReadRetry() throws IOException {
        try {
            Thread.sleep(TOKEN_READ_RETRY_SLEEP_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to re-read projected SA token", e);
        }
    }

    static Path resolveTokenFilePath(String tokenEndpointUrl) {
        URI uri = URI.create(tokenEndpointUrl);

        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("AtlasKafkaOAuthHandler supports only file:// token URLs, got: " + tokenEndpointUrl);
        }

        Path path = Paths.get(uri);

        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("Token file does not exist: " + path);
        }

        return path;
    }

    static JwtClaims parseJwtClaims(String jwt) throws IOException {
        String[] parts = jwt.split("\\.");

        if (parts.length < 2) {
            throw new IOException("Malformed JWT: expected at least header and payload sections");
        }

        byte[] payloadBytes = Base64.getUrlDecoder().decode(parts[1]);
        JsonNode claimsNode = OBJECT_MAPPER.readTree(payloadBytes);

        JsonNode subNode = claimsNode.get("sub");
        JsonNode expNode = claimsNode.get("exp");

        if (subNode == null || subNode.asText().trim().isEmpty()) {
            throw new IOException("JWT is missing required 'sub' claim");
        }

        if (expNode == null || !expNode.isNumber()) {
            throw new IOException("JWT is missing required numeric 'exp' claim");
        }

        Long startTimeMs = null;
        JsonNode iatNode = claimsNode.get("iat");

        if (iatNode != null && iatNode.isNumber()) {
            startTimeMs = toEpochMillis(iatNode);
        }

        return new JwtClaims(subNode.asText(), toEpochMillis(expNode), startTimeMs);
    }

    private static long toEpochMillis(JsonNode epochSecondsNode) {
        return Math.round(epochSecondsNode.asDouble() * 1000);
    }

    static final class JwtClaims {
        private final String principalName;
        private final long   lifetimeMs;
        private final Long   startTimeMs;

        JwtClaims(String principalName, long lifetimeMs, Long startTimeMs) {
            this.principalName = principalName;
            this.lifetimeMs    = lifetimeMs;
            this.startTimeMs   = startTimeMs;
        }
    }

    private static final class AtlasKafkaOAuthBearerToken implements OAuthBearerToken {
        private final String value;
        private final String principalName;
        private final long   lifetimeMs;
        private final Long   startTimeMs;

        AtlasKafkaOAuthBearerToken(String rawJwt, JwtClaims claims) {
            this.value          = rawJwt;
            this.principalName  = claims.principalName;
            this.lifetimeMs     = claims.lifetimeMs;
            this.startTimeMs    = claims.startTimeMs;
        }

        @Override
        public String value() {
            return value;
        }

        @Override
        public Set<String> scope() {
            return Collections.emptySet();
        }

        @Override
        public long lifetimeMs() {
            return lifetimeMs;
        }

        @Override
        public String principalName() {
            return principalName;
        }

        @Override
        public Long startTimeMs() {
            return startTimeMs;
        }
    }
}

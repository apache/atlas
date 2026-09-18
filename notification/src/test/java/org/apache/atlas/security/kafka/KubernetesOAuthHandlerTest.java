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

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class KubernetesOAuthHandlerTest {
    @Test
    public void testHandleTokenFromFileWithoutScopeClaim() throws Exception {
        Path tokenFile = Files.createTempFile("k8s-sa-token", ".jwt");
        String jwt     = buildJwt("system:serviceaccount:ns:atlas", 1_700_000_000L, 1_700_003_600L);

        Files.write(tokenFile, jwt.getBytes(StandardCharsets.UTF_8));

        try {
            KubernetesOAuthHandler handler = new KubernetesOAuthHandler();
            Map<String, Object> configs = new HashMap<>();

            configs.put(KubernetesOAuthHandler.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "file://" + tokenFile);

            handler.configure(configs, "OAUTHBEARER", Collections.emptyList());

            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();

            handler.handle(new javax.security.auth.callback.Callback[] {callback});

            OAuthBearerToken token = callback.token();

            assertEquals(token.value(), jwt);
            assertEquals(token.principalName(), "system:serviceaccount:ns:atlas");
            assertEquals(token.lifetimeMs(), 1_700_003_600_000L);
            assertEquals(token.startTimeMs(), Long.valueOf(1_700_000_000_000L));
            assertTrue(token.scope().isEmpty());
        } finally {
            Files.deleteIfExists(tokenFile);
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConfigureRejectsNonFileUrl() {
        KubernetesOAuthHandler handler = new KubernetesOAuthHandler();
        Map<String, Object> configs = new HashMap<>();

        configs.put(KubernetesOAuthHandler.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "https://example.com/token");

        handler.configure(configs, "OAUTHBEARER", Collections.emptyList());
    }

    @Test
    public void testHandleReloadsTokenAfterKubernetesRotation() throws Exception {
        Path tokenFile = Files.createTempFile("k8s-sa-token-rotate", ".jwt");
        String jwtV1   = buildJwt("system:serviceaccount:ns:atlas", 1_700_000_000L, 1_700_003_600L);
        String jwtV2   = buildJwt("system:serviceaccount:ns:atlas", 1_700_003_601L, 1_700_007_201L);

        Files.write(tokenFile, jwtV1.getBytes(StandardCharsets.UTF_8));

        try {
            KubernetesOAuthHandler handler = new KubernetesOAuthHandler();
            Map<String, Object> configs = new HashMap<>();

            configs.put(KubernetesOAuthHandler.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "file://" + tokenFile);

            handler.configure(configs, "OAUTHBEARER", Collections.emptyList());

            OAuthBearerTokenCallback firstCallback = new OAuthBearerTokenCallback();
            handler.handle(new javax.security.auth.callback.Callback[] {firstCallback});
            OAuthBearerToken firstToken = firstCallback.token();

            assertEquals(firstToken.value(), jwtV1);
            assertEquals(firstToken.lifetimeMs(), 1_700_003_600_000L);

            Files.write(tokenFile, jwtV2.getBytes(StandardCharsets.UTF_8));

            OAuthBearerTokenCallback refreshCallback = new OAuthBearerTokenCallback();
            handler.handle(new javax.security.auth.callback.Callback[] {refreshCallback});
            OAuthBearerToken refreshedToken = refreshCallback.token();

            assertNotEquals(refreshedToken.value(), jwtV1);
            assertEquals(refreshedToken.value(), jwtV2);
            assertEquals(refreshedToken.lifetimeMs(), 1_700_007_201_000L);
            assertEquals(refreshedToken.principalName(), firstToken.principalName());
        } finally {
            Files.deleteIfExists(tokenFile);
        }
    }

    @Test
    public void testHandleTokenFromFileWithoutIatClaim() throws Exception {
        Path tokenFile = Files.createTempFile("k8s-sa-token-no-iat", ".jwt");
        String jwt     = buildJwt("system:serviceaccount:ns:atlas", null, 1_700_003_600L);

        Files.write(tokenFile, jwt.getBytes(StandardCharsets.UTF_8));

        try {
            KubernetesOAuthHandler handler = new KubernetesOAuthHandler();
            Map<String, Object> configs = new HashMap<>();

            configs.put(KubernetesOAuthHandler.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "file://" + tokenFile);

            handler.configure(configs, "OAUTHBEARER", Collections.emptyList());

            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();

            handler.handle(new javax.security.auth.callback.Callback[] {callback});

            OAuthBearerToken token = callback.token();

            assertEquals(token.principalName(), "system:serviceaccount:ns:atlas");
            assertEquals(token.lifetimeMs(), 1_700_003_600_000L);
            assertEquals(token.startTimeMs(), null);
        } finally {
            Files.deleteIfExists(tokenFile);
        }
    }

    private static String buildJwt(String subject, Long issuedAtSeconds, long expirationSeconds) {
        String header  = base64Url("{\"alg\":\"RS256\",\"typ\":\"JWT\"}");
        String payload = base64Url(buildPayload(subject, issuedAtSeconds, expirationSeconds));

        return header + "." + payload + ".signature";
    }

    private static String buildPayload(String subject, Long issuedAtSeconds, long expirationSeconds) {
        StringBuilder payload = new StringBuilder();
        payload.append("{\"sub\":\"").append(subject).append("\",\"exp\":").append(expirationSeconds);

        if (issuedAtSeconds != null) {
            payload.append(",\"iat\":").append(issuedAtSeconds);
        }

        payload.append(",\"iss\":\"https://kubernetes.default.svc.cluster.local\"}");

        return payload.toString();
    }

    private static String base64Url(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}

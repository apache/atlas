/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.atlas.authn.handler.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import org.apache.atlas.authn.handler.AtlasAuthHandler;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;

public abstract class AtlasJwtAuthHandler implements AtlasAuthHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasJwtAuthHandler.class);

    private JWSVerifier        verifier;
    private String             jwksProviderUrl;
    public static final String KEY_PROVIDER_URL    = "atlas.jwt.provider.url";
    public static final String KEY_JWT_PUBLIC_KEY  = "atlas.jwt.public-key";
    public static final String KEY_JWT_COOKIE_NAME = "atlas.jwt.cookie-name";
    public static final String KEY_JWT_AUDIENCES   = "atlas.jwt.audiences";
    public static final String JWT_AUTHZ_PREFIX    = "Bearer ";

    protected List<String> audiences;
    protected JWKSource<SecurityContext> keySource;

    protected static String cookieName = "hadoop-jwt";

    @Override
    public void initialize(final Configuration configuration) throws Exception {
        LOG.debug("===>>> AtlasJwtAuthHandler.initialize()");

        jwksProviderUrl = configuration.getString(KEY_PROVIDER_URL);
        if (!StringUtils.isBlank(jwksProviderUrl)) {
            keySource = new RemoteJWKSet<>(new URL(jwksProviderUrl));
        }

        String pemPublicKey = configuration.getString(KEY_JWT_PUBLIC_KEY);

        if (StringUtils.isNotBlank(pemPublicKey)) {
            verifier = new RSASSAVerifier(parseJwtPublicKey(pemPublicKey));
        } else if (StringUtils.isBlank(jwksProviderUrl)) {
            throw new Exception("AtlasJwtAuthHandler: Mandatory configs ('atlas.jwt.provider.url' & 'atlas.jwt.public-key') are missing, must provide at least one.");
        }

        String customCookieName = configuration.getString(KEY_JWT_COOKIE_NAME);
        if (customCookieName != null) {
            cookieName = customCookieName;
        }

        String audiencesStr = configuration.getString(KEY_JWT_AUDIENCES);
        if (StringUtils.isNotBlank(audiencesStr)) {
            audiences = Arrays.asList(audiencesStr.split(","));
        }

        LOG.debug("<<<=== AtlasJwtAuthHandler.initialize()");
    }

    protected String authenticate(final String jwtAuthHeader, final String jwtCookie) {
        LOG.debug("===>>> AtlasJwtAuthHandler.authenticate()");

        if (shouldProceedAuth(jwtAuthHeader, jwtCookie)) {
            String serializedJWT = getJWT(jwtAuthHeader, jwtCookie);

            if (StringUtils.isNotBlank(serializedJWT)) {
                try {
                    final SignedJWT jwtToken = SignedJWT.parse(serializedJWT);
                    boolean         valid    = validateToken(jwtToken);
                    if (valid) {
                        final String userName = jwtToken.getJWTClaimsSet().getSubject();
                        LOG.info("JWT claims validated; issuing principal user={}", userName);
                        return userName;
                    } else {
                        String sub = null;
                        try {
                            sub = jwtToken.getJWTClaimsSet().getSubject();
                        } catch (ParseException ignored) {
                            // ignore
                        }
                        LOG.warn("JWT validation failed (signature, audience, or expiry). subject={}", sub);
                    }
                } catch (ParseException pe) {
                    LOG.warn("Unable to parse the JWT token", pe);
                }
            } else {
                LOG.warn("JWT token not found.");
            }
        }

        LOG.debug("<<<=== AtlasJwtAuthHandler.authenticate()");

        return null;
    }

    protected String getJWT(final String jwtAuthHeader, final String jwtCookie) {
        String serializedJWT = null;

        if (StringUtils.isNotBlank(jwtAuthHeader) && jwtAuthHeader.startsWith(JWT_AUTHZ_PREFIX)) {
            serializedJWT = jwtAuthHeader.substring(JWT_AUTHZ_PREFIX.length());
        }

        if (StringUtils.isBlank(serializedJWT) && StringUtils.isNotBlank(jwtCookie)) {
            String[] cookie = jwtCookie.split("=");
            if (cookieName.equals(cookie[0])) {
                serializedJWT = cookie[1];
            }
        }

        return serializedJWT;
    }

    protected boolean validateToken(final SignedJWT jwtToken) {
        boolean expValid = validateExpiration(jwtToken);
        boolean sigValid = false;
        boolean audValid = false;

        if (expValid) {
            sigValid = validateSignature(jwtToken);

            if (sigValid) {
                audValid = validateAudiences(jwtToken);
            }
        }

        LOG.debug("expValid={}, sigValid={}, audValid={}", expValid, sigValid, audValid);

        return sigValid && audValid && expValid;
    }

    protected boolean validateSignature(final SignedJWT jwtToken) {
        boolean valid = false;

        if (JWSObject.State.SIGNED == jwtToken.getState()) {
            LOG.debug("JWT token is in a SIGNED state");

            if (jwtToken.getSignature() != null) {
                try {
                    if (StringUtils.isNotBlank(jwksProviderUrl)) {
                        JWSKeySelector<SecurityContext> keySelector            = new JWSVerificationKeySelector<>(jwtToken.getHeader().getAlgorithm(), keySource);
                        ConfigurableJWTProcessor<SecurityContext> jwtProcessor = getJwtProcessor(keySelector);

                        jwtProcessor.process(jwtToken, null);
                        valid = true;
                        LOG.debug("JWT token has been successfully verified.");
                    } else if (verifier != null) {
                        if (jwtToken.verify(verifier)) {
                            valid = true;
                            LOG.debug("JWT token has been successfully verified.");
                        } else {
                            LOG.warn("JWT signature verification failed.");
                        }
                    } else {
                        LOG.warn("Cannot authenticate JWT token as neither JWKS provider URL nor public key provided.");
                    }
                } catch (JOSEException | BadJOSEException e) {
                    LOG.error("Error while validating signature.", e);
                }
            }
        }

        if (!valid) {
            LOG.warn("Signature could not be verified.");
        }

        return valid;
    }

    private static RSAPublicKey parseJwtPublicKey(String pem) throws Exception {
        String trimmed = StringUtils.trimToEmpty(pem);

        if (trimmed.contains("BEGIN CERTIFICATE")) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            try (ByteArrayInputStream input = new ByteArrayInputStream(trimmed.getBytes(StandardCharsets.UTF_8))) {
                X509Certificate cert = (X509Certificate) factory.generateCertificate(input);
                PublicKey key        = cert.getPublicKey();

                if (key instanceof RSAPublicKey) {
                    return (RSAPublicKey) key;
                }
            }

            throw new IllegalArgumentException("Certificate does not contain an RSA public key");
        }

        String base64 = trimmed
                .replace("-----BEGIN PUBLIC KEY-----", "")
                .replace("-----END PUBLIC KEY-----", "")
                .replaceAll("\\s", "");

        byte[]              decoded = Base64.getDecoder().decode(base64);
        X509EncodedKeySpec spec     = new X509EncodedKeySpec(decoded);
        KeyFactory          kf      = KeyFactory.getInstance("RSA");
        PublicKey           key     = kf.generatePublic(spec);

        if (key instanceof RSAPublicKey) {
            return (RSAPublicKey) key;
        }

        throw new IllegalArgumentException("Provided key is not an RSA public key");
    }

    public abstract ConfigurableJWTProcessor<SecurityContext> getJwtProcessor(JWSKeySelector<SecurityContext> keySelector);

    protected boolean validateAudiences(final SignedJWT jwtToken) {
        boolean valid = false;
        try {
            List<String> tokenAudienceList = jwtToken.getJWTClaimsSet().getAudience();
            if (audiences == null) {
                valid = true;
            } else {
                for (String aud : tokenAudienceList) {
                    if (audiences.contains(aud)) {
                        LOG.debug("JWT token audience has been successfully validated.");
                        valid = true;
                        break;
                    }
                }
                if (!valid) {
                    LOG.warn("JWT audience validation failed.");
                }
            }
        } catch (ParseException pe) {
            LOG.warn("Unable to parse the JWT token.", pe);
        }
        return valid;
    }

    protected boolean validateExpiration(final SignedJWT jwtToken) {
        boolean valid = false;
        try {
            Date expires = jwtToken.getJWTClaimsSet().getExpirationTime();
            if (expires == null || new Date().before(expires)) {
                valid = true;
                LOG.debug("JWT token expiration date has been successfully validated.");
            } else {
                LOG.warn("JWT token provided is expired.");
            }
        } catch (ParseException pe) {
            LOG.warn("Failed to validate JWT expiry.", pe);
        }

        return valid;
    }

    public static boolean shouldProceedAuth(final String authHeader, final String jwtCookie) {
        return (StringUtils.isNotBlank(authHeader) && authHeader.startsWith(JWT_AUTHZ_PREFIX))
                || (StringUtils.isNotBlank(jwtCookie) && jwtCookie.startsWith(cookieName));
    }
}

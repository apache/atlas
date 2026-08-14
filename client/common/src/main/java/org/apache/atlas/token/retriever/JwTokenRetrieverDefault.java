/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.token.retriever;

import org.apache.atlas.security.SecurityUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Optional;

public class JwTokenRetrieverDefault implements TokenRetriever<String> {
    private static final Logger LOG = LoggerFactory.getLogger(JwTokenRetrieverDefault.class);

    public static final String JWT_SOURCE     = "atlas.jwt.source";
    public static final String JWT_ENV        = "atlas.jwt.env";
    public static final String JWT_FILE       = "atlas.jwt.file";
    public static final String JWT_CRED_FILE  = "atlas.jwt.cred.file";
    public static final String JWT_CRED_ALIAS = "atlas.jwt.cred.alias";

    private static final String SOURCE_ENV  = "env";
    private static final String SOURCE_FILE = "file";
    private static final String SOURCE_CRED = "cred";
    private static final long CRED_CHECK_INTERVAL_MS = 60 * 1000;

    private final String jwtSource;
    private final String jwtEnvVar;
    private final String jwtFilePath;
    private final String jwtCredPathPropertyName;
    private final String jwtCredFilePath;
    private final String jwtCredAlias;
    private final Configuration configuration;
    private long jwtFileLastModified;
    private long jwtCredFileLastCheckedAt;

    private volatile Optional<String> cachedJwt = Optional.empty();

    public JwTokenRetrieverDefault(Configuration config) {
        configuration           = config;
        jwtSource               = StringUtils.trimToEmpty(config.getString(JWT_SOURCE, ""));
        jwtEnvVar               = StringUtils.trimToEmpty(config.getString(JWT_ENV, ""));
        jwtFilePath             = StringUtils.trimToEmpty(config.getString(JWT_FILE, ""));
        jwtCredPathPropertyName = JWT_CRED_FILE;
        jwtCredFilePath         = StringUtils.trimToEmpty(config.getString(jwtCredPathPropertyName, ""));
        jwtCredAlias            = StringUtils.trimToEmpty(config.getString(JWT_CRED_ALIAS, ""));
    }

    @Override
    public synchronized Optional<String> retrieve() {
        String source = StringUtils.lowerCase(jwtSource);
        switch (source) {
            case SOURCE_ENV:
                return getJwtFromEnv();
            case SOURCE_FILE:
                return getJwtFromFile();
            case SOURCE_CRED:
                return getJwtFromCredProvider();
            default:
                if (StringUtils.isNotBlank(source)) {
                    LOG.warn("JwTokenRetrieverDefault.retrieve(): unsupported source='{}'", source);
                }
                return Optional.empty();
        }
    }

    private Optional<String> getJwtFromEnv() {
        if (StringUtils.isBlank(jwtEnvVar)) {
            LOG.warn("JwTokenRetrieverDefault.getJwtFromEnv(): '{}' is not configured.", JWT_ENV);
            return Optional.empty();
        }

        String token = StringUtils.trimToEmpty(System.getenv(jwtEnvVar));
        return StringUtils.isBlank(token) ? Optional.empty() : Optional.of(token);
    }

    private Optional<String> getJwtFromFile() {
        if (StringUtils.isBlank(jwtFilePath)) {
            LOG.warn("JwTokenRetrieverDefault.getJwtFromFile(): '{}' is not configured.", JWT_FILE);
            return Optional.empty();
        }

        File jwtFile = new File(jwtFilePath);
        if (!jwtFile.canRead()) {
            return cachedJwt;
        }

        if (jwtFile.lastModified() == jwtFileLastModified && cachedJwt.isPresent()) {
            return cachedJwt;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(jwtFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
                    cachedJwt           = Optional.of(line.trim());
                    jwtFileLastModified = jwtFile.lastModified();
                    break;
                }
            }
        } catch (IOException e) {
            LOG.error("JwTokenRetrieverDefault.getJwtFromFile(): failed to read JWT from file={}", jwtFilePath, e);
        }

        return cachedJwt;
    }

    private Optional<String> getJwtFromCredProvider() {
        if (StringUtils.isBlank(jwtCredFilePath) || StringUtils.isBlank(jwtCredAlias)) {
            LOG.warn("JwTokenRetrieverDefault.getJwtFromCredProvider(): '{}' or '{}' is not configured.",
                    JWT_CRED_FILE, JWT_CRED_ALIAS);
            return Optional.empty();
        }

        long now = System.currentTimeMillis();
        if ((now - jwtCredFileLastCheckedAt) <= CRED_CHECK_INTERVAL_MS && cachedJwt.isPresent()) {
            return cachedJwt;
        }

        try {
            String token = StringUtils.trimToEmpty(
                    SecurityUtil.getPassword(configuration, jwtCredAlias, jwtCredPathPropertyName));
            if (StringUtils.isNotBlank(token)) {
                cachedJwt = Optional.of(token);
            }
        } catch (Exception e) {
            LOG.error("JwTokenRetrieverDefault.getJwtFromCredProvider(): failed to read JWT from credential provider.", e);
        } finally {
            jwtCredFileLastCheckedAt = now;
        }

        return cachedJwt;
    }
}

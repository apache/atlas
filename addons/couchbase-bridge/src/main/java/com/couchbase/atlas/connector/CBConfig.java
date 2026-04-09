/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.atlas.connector;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.SecurityConfig;
import com.couchbase.client.java.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class CBConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(CBConfig.class);

    private static final Map<String, String> ENV                 = System.getenv();
    private static final Integer             DCP_FIELD_MIN_OCCUR = Integer.valueOf(ENV.getOrDefault("DCP_FIELD_MIN_OCCUR", "0"));
    private static final Float               DCP_SAMPLE_RATIO    = Float.valueOf(ENV.getOrDefault("DCP_SAMPLE_RATIO", "1"));
    private static final Short               DCP_FIELD_THRESHOLD = Short.valueOf(ENV.getOrDefault("DCP_FIELD_THRESHOLD", "0"));
    private static       Client              mockDcpClient;
    private static       Cluster             cluster;

    private CBConfig() {
        // to block instantiation
    }

    public static String address() {
        return ENV.getOrDefault("CB_CLUSTER", "couchbase://localhost");
    }

    public static String username() {
        return ENV.getOrDefault("CB_USERNAME", "Administrator");
    }

    public static String password() {
        return ENV.getOrDefault("CB_PASSWORD", "password");
    }

    public static String bucket() {
        return ENV.getOrDefault("CB_BUCKET", "default");
    }

    public static Collection<String> collections() {
        String collections = ENV.get("CB_COLLECTIONS");

        if (collections == null) {
            return null;
        }

        return Arrays.stream(collections.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    public static String dcpPort() {
        return ENV.getOrDefault("DCP_PORT", "11210");
    }

    /**
     * @return Percentage of DCP messages to be analyzed in form of a short between 0 and 1.
     */
    public static Float dcpSampleRatio() {
        return DCP_SAMPLE_RATIO;
    }

    /**
     * @return a threshold that indicates in what percentage of analyzed messages per collection a field must appear before it is sent to Atlas
     */
    public static Short dcpFieldThreshold() {
        return DCP_FIELD_THRESHOLD;
    }

    public static Integer dcpFieldMinOccurences() {
        return DCP_FIELD_MIN_OCCUR;
    }

    public static Cluster cluster() {
        if (cluster == null) {
            cluster = Cluster.connect(address(), username(), password());
        }

        return cluster;
    }

    public static Client dcpClient() {
        if (mockDcpClient != null) {
            LOGGER.debug("Using mock DCP client");

            return mockDcpClient;
        }

        Client.Builder builder = Client.builder()
                .collectionsAware(true)
                .seedNodes(String.format("%s:%s", address(), dcpPort()))
                .connectionString(address())
                .credentials(username(), password());

        String bucket = bucket();

        if (!(bucket == null || bucket.equals("*") || bucket.isEmpty())) {
            LOGGER.debug("Monitoring bucket `{}`", bucket);

            builder.bucket(bucket);

            Collection<String> collections = collections();

            if (collections != null && !collections.isEmpty()) {
                LOGGER.debug("Monitoring collections: {}", String.join(", ", collections));

                builder.collectionNames(collections);
            }
        }

        if (enableTLS()) {
            LOGGER.debug("Using native TLS");

            builder.securityConfig(SecurityConfig.builder().enableNativeTls(true).build());
        }

        return builder.build();
    }

    protected static void dcpClient(Client mockDcpClient) {
        CBConfig.mockDcpClient = mockDcpClient;
    }

    private static boolean enableTLS() {
        return Boolean.parseBoolean(ENV.getOrDefault("CB_ENABLE_TLS", "false"));
    }
}

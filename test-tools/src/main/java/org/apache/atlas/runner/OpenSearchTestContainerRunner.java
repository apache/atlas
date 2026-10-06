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
package org.apache.atlas.runner;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Starts a single-node OpenSearch cluster via Testcontainers for integration tests.
 * <p>
 * The container image is configurable so the same suite can be validated against multiple OpenSearch server
 * versions (co-primary targets 3.7/3.8 and the secondary 2.x compatibility target) without code changes:
 * <ul>
 *     <li>{@code -Dopensearch.docker.image=opensearchproject/opensearch:3.8.0} — full image override, or</li>
 *     <li>{@code -Dopensearch.docker.version=3.8.0} — version tag override for the default image repository.</li>
 * </ul>
 * The default remains {@value #DEFAULT_VERSION} so existing 3.7 coverage is preserved when nothing is set.
 */
public final class OpenSearchTestContainerRunner {
    public static final String IMAGE_PROPERTY   = "opensearch.docker.image";
    public static final String VERSION_PROPERTY = "opensearch.docker.version";
    public static final String DEFAULT_REPO     = "opensearchproject/opensearch";
    public static final String DEFAULT_VERSION  = "3.7.0";

    private static final DockerImageName OPENSEARCH_IMAGE = DockerImageName.parse(resolveImageName());

    private static GenericContainer<?> container;

    private OpenSearchTestContainerRunner() {
    }

    /**
     * Resolves the OpenSearch container image from system properties, falling back to
     * {@code opensearchproject/opensearch:3.7.0}. Package-visible for unit testing the resolution rules.
     */
    static String resolveImageName() {
        String image = System.getProperty(IMAGE_PROPERTY);

        if (image != null && !image.trim().isEmpty()) {
            return image.trim();
        }

        String version = System.getProperty(VERSION_PROPERTY);

        if (version == null || version.trim().isEmpty()) {
            version = DEFAULT_VERSION;
        }

        return DEFAULT_REPO + ":" + version.trim();
    }

    public static String getImageName() {
        return OPENSEARCH_IMAGE.asCanonicalNameString();
    }

    public static boolean isDockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    @SuppressWarnings("resource")
    public static synchronized void start() {
        if (container != null && container.isRunning()) {
            applySystemProperties();
            return;
        }

        container = new GenericContainer<>(OPENSEARCH_IMAGE)
                .withExposedPorts(9200)
                .withEnv("discovery.type", "single-node")
                .withEnv("plugins.security.disabled", "true")
                .withEnv("DISABLE_INSTALL_DEMO_CONFIG", "true")
                .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                .waitingFor(Wait.forHttp("/")
                        .forPort(9200)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        container.start();
        applySystemProperties();
    }

    public static synchronized void stop() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }

    public static String getHost() {
        ensureRunning();
        return container.getHost();
    }

    public static int getPort() {
        ensureRunning();
        return container.getMappedPort(9200);
    }

    private static void ensureRunning() {
        if (container == null || !container.isRunning()) {
            throw new IllegalStateException("OpenSearch test container is not running");
        }
    }

    private static void applySystemProperties() {
        String host = container.getHost();
        int port = container.getMappedPort(9200);
        System.setProperty("opensearch.host", host);
        System.setProperty("opensearch.port", String.valueOf(port));
        System.setProperty("opensearch.test.host", host);
        System.setProperty("opensearch.test.port", String.valueOf(port));
    }
}

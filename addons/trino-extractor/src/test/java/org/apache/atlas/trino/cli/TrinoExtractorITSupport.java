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
package org.apache.atlas.trino.cli;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Shared helpers for live Trino extractor integration tests (tarball subprocess + Atlas REST).
 */
final class TrinoExtractorITSupport {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoExtractorITSupport.class);

    static final String DEFAULT_TRINO_JDBC_URL = "jdbc:trino://localhost:8080/";
    static final String DEFAULT_ATLAS_USER     = "admin";
    static final String DEFAULT_ATLAS_PASS     = "atlasR0cks!";
    static final String DEFAULT_NAMESPACE      = "dev";
    static final String DEFAULT_CATALOG        = "hive";
    static final String DEFAULT_SCHEMA         = "hr";
    static final String DEFAULT_TABLE          = "trino_pii_lab";
    static final String DEFAULT_COLUMN         = "ssn";
    static final String DEFAULT_HIVE_NAMESPACE = "cm";

    private TrinoExtractorITSupport() {
    }

    static LiveStackConfig loadLiveStackConfig() {
        String atlasRestUrl = firstNonBlank(System.getenv("ATLAS_REST_URL"), System.getenv("ATLAS_URL"));

        if (atlasRestUrl == null) {
            throw new SkipException("Set ATLAS_REST_URL (or ATLAS_URL) to run TrinoExtractorIT against a live Atlas");
        }

        LiveStackConfig config = new LiveStackConfig();
        config.atlasRestUrl   = normalizeAtlasUrl(atlasRestUrl);
        config.atlasUsername  = firstNonBlank(System.getenv("ATLAS_USERNAME"), System.getenv("ATLAS_USER"), DEFAULT_ATLAS_USER);
        config.atlasPassword  = firstNonBlank(System.getenv("ATLAS_PASSWORD"), System.getenv("ATLAS_PASS"), DEFAULT_ATLAS_PASS);
        config.trinoJdbcUrl   = firstNonBlank(System.getenv("TRINO_JDBC_URL"), DEFAULT_TRINO_JDBC_URL);
        config.trinoNamespace = firstNonBlank(System.getenv("ATLAS_TRINO_NAMESPACE"), DEFAULT_NAMESPACE);
        config.catalog        = firstNonBlank(System.getenv("TRINO_EXTRACTOR_CATALOG"), DEFAULT_CATALOG);
        config.schema         = firstNonBlank(System.getenv("TRINO_EXTRACTOR_SCHEMA"), DEFAULT_SCHEMA);
        config.table          = firstNonBlank(System.getenv("TRINO_EXTRACTOR_TABLE"), DEFAULT_TABLE);
        config.column         = firstNonBlank(System.getenv("TRINO_EXTRACTOR_COLUMN"), DEFAULT_COLUMN);
        config.hiveNamespace  = firstNonBlank(System.getenv("ATLAS_HIVE_NAMESPACE"), DEFAULT_HIVE_NAMESPACE);

        if (!isAtlasReachable(config)) {
            throw new SkipException("Atlas is not reachable at " + config.atlasRestUrl);
        }

        return config;
    }

    static Path prepareExtractorWorkDir(LiveStackConfig config) throws Exception {
        Path tarball = resolveTarball();
        Path workDir = Files.createTempDirectory("atlas-trino-extractor-it-");
        unpackTarball(tarball, workDir);
        writeExtractorProperties(workDir, config, null);
        makeExecutable(workDir.resolve("bin").resolve("run-trino-extractor.sh"));
        LOG.info("Prepared extractor workdir={} tarball={}", workDir, tarball);
        return workDir;
    }

    static void deleteWorkDir(Path workDir) throws IOException {
        if (workDir != null) {
            FileUtils.deleteDirectory(workDir.toFile());
        }
    }

    static void assertTarballJerseyClasspath(Path workDir) throws IOException {
        Path libDir = workDir.resolve("lib");
        assertTrue(Files.isDirectory(libDir), "tarball lib/ directory missing");

        List<String> jerseyClientJars = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(libDir, "jersey-client*.jar")) {
            for (Path jar : stream) {
                jerseyClientJars.add(jar.getFileName().toString());
            }
        }

        assertFalse(jerseyClientJars.isEmpty(), "jersey-client jar missing from tarball lib/");
        assertTrue(jerseyClientJars.stream().anyMatch(name -> name.contains("jersey-client-1.19")),
                "expected jersey-client 1.19 in tarball lib/, found: " + jerseyClientJars);
        assertFalse(jerseyClientJars.stream().anyMatch(name -> name.matches("jersey-client-1\\.9\\.jar")),
                "jersey-client 1.9 must not be in tarball lib/: " + jerseyClientJars);
    }

    static int runExtractorJava(Path workDir, LiveStackConfig config, String... javaArgs) throws IOException, InterruptedException {
        Files.createDirectories(workDir.resolve("log"));

        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-Datlas.log.dir=" + workDir.resolve("log"));
        command.add("-Datlas.log.file=atlas-trino-extractor.log");
        command.add("-Dlogback.configurationFile=atlas-trino-extractor-logback.xml");
        command.add("-Datlas.properties=atlas-trino-extractor.properties");
        command.add("-cp");
        command.add(buildClasspath(workDir));
        command.add("org.apache.atlas.trino.cli.TrinoExtractor");
        command.addAll(List.of(javaArgs));

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(workDir.toFile());
        processBuilder.redirectErrorStream(true);
        processBuilder.environment().put("ATLAS_USERNAME", config.atlasUsername);
        processBuilder.environment().put("ATLAS_PASSWORD", config.atlasPassword);

        Process process = processBuilder.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String output = reader.lines().collect(Collectors.joining(System.lineSeparator()));

            if (!output.isEmpty()) {
                LOG.debug("TrinoExtractor java output:\n{}", output);
            }
        }

        return process.waitFor();
    }

    static int runExtractorScript(Path workDir, LiveStackConfig config, String... extraArgs) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(workDir.resolve("bin").resolve("run-trino-extractor.sh").toString());
        command.addAll(List.of(extraArgs));

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(workDir.toFile());
        processBuilder.redirectErrorStream(true);
        processBuilder.environment().put("ATLAS_USERNAME", config.atlasUsername);
        processBuilder.environment().put("ATLAS_PASSWORD", config.atlasPassword);

        Process process = processBuilder.start();
        String output;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            output = reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }

        int exitCode = process.waitFor();

        if (exitCode != 0) {
            LOG.error("run-trino-extractor.sh {} exit={} output:\n{}", extraArgs, exitCode, output);
            Path logFile = workDir.resolve("log").resolve("atlas-trino-extractor.log");

            if (Files.isRegularFile(logFile)) {
                LOG.error("extractor log tail:\n{}", tailFile(logFile, 8000));
            }
        } else {
            LOG.info("run-trino-extractor.sh {} completed successfully", List.of(extraArgs));
        }

        return exitCode;
    }

    static void rewriteProperties(Path workDir, LiveStackConfig config, Map<String, String> overrides) throws IOException {
        writeExtractorProperties(workDir, config, overrides);
    }

    static String trinoQualifiedName(LiveStackConfig config, String type, String... parts) {
        switch (type) {
            case "trino_instance":
                return config.trinoNamespace;
            case "trino_catalog":
                return String.format(Locale.ROOT, "%s@%s", parts[0], config.trinoNamespace);
            case "trino_schema":
                return String.format(Locale.ROOT, "%s.%s@%s", parts[0], parts[1], config.trinoNamespace);
            case "trino_table":
                return String.format(Locale.ROOT, "%s.%s.%s@%s", parts[0], parts[1], parts[2], config.trinoNamespace);
            case "trino_column":
                return String.format(Locale.ROOT, "%s.%s.%s.%s@%s", parts[0], parts[1], parts[2], parts[3], config.trinoNamespace);
            default:
                throw new IllegalArgumentException("unsupported type: " + type);
        }
    }

    static void assertEntityExists(LiveStackConfig config, String typeName, String qualifiedName) throws IOException {
        assertEquals(getEntityStatus(config, typeName, qualifiedName), 200,
                typeName + " missing for qualifiedName=" + qualifiedName);
    }

    static void assertEntityAbsent(LiveStackConfig config, String typeName, String qualifiedName) throws IOException {
        assertEquals(getEntityStatus(config, typeName, qualifiedName), 404,
                typeName + " still present for qualifiedName=" + qualifiedName);
    }

    static String getEntityBody(LiveStackConfig config, String typeName, String qualifiedName) throws IOException {
        String requestUrl = config.atlasRestUrl + "api/atlas/v2/entity/uniqueAttribute/type/" + typeName
                + "?attr:qualifiedName=" + urlEncode(qualifiedName);
        HttpURLConnection connection = openGet(requestUrl, config);

        try {
            int status = connection.getResponseCode();

            if (status != 200) {
                return null;
            }

            return readBody(connection);
        } finally {
            connection.disconnect();
        }
    }

    static String getEntityGuid(LiveStackConfig config, String typeName, String qualifiedName) throws IOException {
        String body = getEntityBody(config, typeName, qualifiedName);
        return parseEntityGuid(body);
    }

    private static String parseEntityGuid(String body) {
        if (body == null) {
            return null;
        }

        int entityIdx = body.indexOf("\"entity\"");
        int searchFrom = entityIdx >= 0 ? entityIdx : 0;
        int guidIdx = body.indexOf("\"guid\":\"", searchFrom);

        if (guidIdx < 0) {
            return null;
        }

        int start = guidIdx + 8;
        int end   = body.indexOf('"', start);

        return end > start ? body.substring(start, end) : null;
    }

    static void deleteEntityByGuid(LiveStackConfig config, String guid) throws IOException {
        if (guid == null) {
            return;
        }

        HttpURLConnection connection = openDelete(config.atlasRestUrl + "api/atlas/v2/entity/guid/" + guid, config);

        try {
            connection.getResponseCode();
        } finally {
            connection.disconnect();
        }
    }

    static void seedStaleTrinoTable(LiveStackConfig config, String tableName) throws IOException {
        String schemaQn = trinoQualifiedName(config, "trino_schema", config.catalog, config.schema);
        String tableQn  = trinoQualifiedName(config, "trino_table", config.catalog, config.schema, tableName);

        deleteIfExists(config, "trino_table", tableQn);

        String schemaGuid = getEntityGuid(config, "trino_schema", schemaQn);
        assertTrue(schemaGuid != null, "schema must exist before stale trino_table test: " + schemaQn);

        String payload = "{\"entity\":{\"typeName\":\"trino_table\",\"attributes\":{\"qualifiedName\":\"" + tableQn + "\",\"name\":\"" + tableName + "\"},\"relationshipAttributes\":{\"trinoschema\":{\"guid\":\"" + schemaGuid + "\",\"typeName\":\"trino_schema\"}}}}";

        postJson(config, "api/atlas/v2/entity", payload);
        assertEntityExists(config, "trino_table", tableQn);
    }

    static void seedStaleTrinoSchema(LiveStackConfig config, String schemaName) throws IOException {
        String catalogQn = trinoQualifiedName(config, "trino_catalog", config.catalog);
        String schemaQn  = trinoQualifiedName(config, "trino_schema", config.catalog, schemaName);

        deleteIfExists(config, "trino_schema", schemaQn);

        String catalogGuid = getEntityGuid(config, "trino_catalog", catalogQn);
        assertTrue(catalogGuid != null, "catalog must exist before stale trino_schema test: " + catalogQn);

        String payload = "{\"entity\":{\"typeName\":\"trino_schema\",\"attributes\":{\"qualifiedName\":\"" + schemaQn + "\",\"name\":\"" + schemaName + "\"},\"relationshipAttributes\":{\"catalog\":{\"guid\":\"" + catalogGuid + "\",\"typeName\":\"trino_catalog\"}}}}";

        postJson(config, "api/atlas/v2/entity", payload);
        assertEntityExists(config, "trino_schema", schemaQn);
    }

    static void seedStaleTrinoCatalog(LiveStackConfig config, String catalogName) throws IOException {
        String catalogQn = trinoQualifiedName(config, "trino_catalog", catalogName);

        deleteIfExists(config, "trino_catalog", catalogQn);

        String instanceGuid = getEntityGuid(config, "trino_instance", config.trinoNamespace);
        assertTrue(instanceGuid != null, "trino_instance must exist before stale trino_catalog test");

        String payload = "{\"entity\":{\"typeName\":\"trino_catalog\",\"attributes\":{\"qualifiedName\":\"" + catalogQn + "\",\"name\":\"" + catalogName + "\",\"connectorType\":\"hive\"},\"relationshipAttributes\":{\"instance\":{\"guid\":\"" + instanceGuid + "\",\"typeName\":\"trino_instance\"}}}}";

        postJson(config, "api/atlas/v2/entity", payload);
        assertEntityExists(config, "trino_catalog", catalogQn);
    }

    static boolean isEnabled(String envName) {
        return "1".equals(System.getenv(envName)) || "true".equalsIgnoreCase(System.getenv(envName));
    }

    private static void deleteIfExists(LiveStackConfig config, String typeName, String qualifiedName) throws IOException {
        if (getEntityStatus(config, typeName, qualifiedName) == 200) {
            deleteEntityByGuid(config, getEntityGuid(config, typeName, qualifiedName));
        }
    }

    private static String buildClasspath(Path workDir) throws IOException {
        List<String> entries = new ArrayList<>();
        entries.add(workDir.resolve("conf").toString());

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(workDir.resolve("lib"), "*.jar")) {
            for (Path jar : stream) {
                entries.add(jar.toString());
            }
        }

        return String.join(":", entries);
    }

    private static int getEntityStatus(LiveStackConfig config, String typeName, String qualifiedName) throws IOException {
        String requestUrl = config.atlasRestUrl + "api/atlas/v2/entity/uniqueAttribute/type/" + typeName
                + "?attr:qualifiedName=" + urlEncode(qualifiedName);
        HttpURLConnection connection = openGet(requestUrl, config);

        try {
            return connection.getResponseCode();
        } finally {
            connection.disconnect();
        }
    }

    private static void postJson(LiveStackConfig config, String path, String json) throws IOException {
        HttpURLConnection connection = openPost(config.atlasRestUrl + path, config);

        try {
            connection.getOutputStream().write(json.getBytes(StandardCharsets.UTF_8));
            int status = connection.getResponseCode();
            String body  = readResponseBody(connection, status);

            if (status < 200 || status >= 300) {
                throw new IOException("POST " + path + " failed: HTTP " + status + " body=" + body);
            }
        } finally {
            connection.disconnect();
        }
    }

    private static String readResponseBody(HttpURLConnection connection, int status) throws IOException {
        java.io.InputStream stream = status >= 400 ? connection.getErrorStream() : connection.getInputStream();

        if (stream == null) {
            return "";
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining());
        }
    }

    private static boolean isAtlasReachable(LiveStackConfig config) {
        try {
            HttpURLConnection connection = openGet(config.atlasRestUrl + "api/atlas/admin/version", config);

            try {
                return connection.getResponseCode() == 200;
            } finally {
                connection.disconnect();
            }
        } catch (IOException e) {
            LOG.warn("Atlas reachability check failed: {}", e.toString());
            return false;
        }
    }

    private static HttpURLConnection openGet(String requestUrl, LiveStackConfig config) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(requestUrl).openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(10_000);
        connection.setReadTimeout(30_000);
        addBasicAuth(connection, config);
        return connection;
    }

    private static HttpURLConnection openPost(String requestUrl, LiveStackConfig config) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(requestUrl).openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setConnectTimeout(10_000);
        connection.setReadTimeout(30_000);
        connection.setRequestProperty("Content-Type", "application/json");
        addBasicAuth(connection, config);
        return connection;
    }

    private static HttpURLConnection openDelete(String requestUrl, LiveStackConfig config) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(requestUrl).openConnection();
        connection.setRequestMethod("DELETE");
        connection.setConnectTimeout(10_000);
        connection.setReadTimeout(30_000);
        addBasicAuth(connection, config);
        return connection;
    }

    private static void addBasicAuth(HttpURLConnection connection, LiveStackConfig config) {
        String credentials = config.atlasUsername + ":" + config.atlasPassword;
        String encoded     = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", "Basic " + encoded);
    }

    private static String readBody(HttpURLConnection connection) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining());
        }
    }

    private static Path resolveTarball() throws IOException {
        String explicitTarball = System.getenv("TRINO_EXTRACTOR_TARBALL");

        if (explicitTarball != null && !explicitTarball.isEmpty()) {
            Path path = Paths.get(explicitTarball);

            if (!Files.isRegularFile(path)) {
                throw new SkipException("TRINO_EXTRACTOR_TARBALL does not exist: " + explicitTarball);
            }

            return path;
        }

        Path moduleDir = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        List<Path> candidates = List.of(
                moduleDir.resolve("target/trino-extractor-dist"),
                moduleDir.resolve("../../distro/target").normalize());

        for (Path dir : candidates) {
            Path match = findTarballInDirectory(dir);

            if (match != null) {
                return match;
            }
        }

        throw new SkipException("Trino extractor tarball not found. Build with "
                + "mvn -pl addons/trino-extractor -Ptrino-extractor-it package "
                + "or set TRINO_EXTRACTOR_TARBALL");
    }

    private static Path findTarballInDirectory(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            return null;
        }

        try (Stream<Path> paths = Files.list(directory)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().contains("trino-extractor"))
                    .filter(path -> path.getFileName().toString().endsWith(".tar.gz"))
                    .sorted()
                    .findFirst()
                    .orElse(null);
        }
    }

    private static void unpackTarball(Path tarball, Path destination) throws IOException, InterruptedException {
        Files.createDirectories(destination);

        ProcessBuilder processBuilder = new ProcessBuilder(
                "tar", "xzf", tarball.toAbsolutePath().toString(),
                "-C", destination.toAbsolutePath().toString(),
                "--strip-components=1");
        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();
        process.waitFor();
        assertEquals(process.exitValue(), 0, "failed to unpack tarball " + tarball);
    }

    private static void writeExtractorProperties(Path workDir, LiveStackConfig config, Map<String, String> overrides) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("atlas.rest.address", config.atlasRestUrl);
        properties.setProperty("atlas.trino.jdbc.address", config.trinoJdbcUrl);
        properties.setProperty("atlas.trino.jdbc.user", firstNonBlank(System.getenv("TRINO_JDBC_USER"), "admin"));
        properties.setProperty("atlas.trino.namespace", config.trinoNamespace);
        properties.setProperty("atlas.trino.catalogs.registered", config.catalog);

        if (overrides != null) {
            for (Map.Entry<String, String> entry : overrides.entrySet()) {
                if (entry.getValue() == null) {
                    properties.remove(entry.getKey());
                } else {
                    properties.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }

        Path confDir = workDir.resolve("conf");
        Files.createDirectories(confDir);

        try (OutputStream out = Files.newOutputStream(confDir.resolve("atlas-trino-extractor.properties"))) {
            properties.store(out, "TrinoExtractorIT");
        }
    }

    private static void makeExecutable(Path script) throws IOException {
        if (!Files.exists(script)) {
            return;
        }

        try {
            Files.setPosixFilePermissions(script, EnumSet.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE,
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.GROUP_EXECUTE,
                    PosixFilePermission.OTHERS_READ,
                    PosixFilePermission.OTHERS_EXECUTE));
        } catch (UnsupportedOperationException e) {
            LOG.debug("POSIX permissions not supported for {}", script);
        }
    }

    private static String tailFile(Path file, int maxChars) throws IOException {
        String content = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        return content.length() <= maxChars ? content : content.substring(content.length() - maxChars);
    }

    private static String normalizeAtlasUrl(String url) {
        return url.endsWith("/") ? url : url + "/";
    }

    private static String urlEncode(String value) {
        try {
            return java.net.URLEncoder.encode(value, "UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }

        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }

        return null;
    }

    static final class LiveStackConfig {
        String atlasRestUrl;
        String atlasUsername;
        String atlasPassword;
        String trinoJdbcUrl;
        String trinoNamespace;
        String catalog;
        String schema;
        String table;
        String column;
        String hiveNamespace;
    }
}

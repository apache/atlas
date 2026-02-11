/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.resources.ResourceBuilder;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.ResourceAttributes;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase;
import org.apache.atlas.security.SecurityProperties;
import org.apache.atlas.util.AccessAuditLogsIndexCreator;
import org.apache.atlas.web.service.EmbeddedServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.IndexTemplatesExistRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.atlas.repository.Constants.INDEX_PREFIX;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;

/**
 * Driver for running Metadata as a standalone server with embedded jetty server.
 */
public final class Atlas {
    private static final Logger LOG = LoggerFactory.getLogger(Atlas.class);
    private static final String APP_PATH = "app";
    private static final String APP_PORT = "port";
    private static final String ATLAS_HOME = "atlas.home";
    private static final String ATLAS_DATA = "atlas.data";
    private static final String ATLAS_LOG_DIR = "atlas.log.dir";
    public static final String ATLAS_SERVER_HTTPS_PORT = "atlas.server.https.port";
    public static final String ATLAS_SERVER_HTTP_PORT = "atlas.server.http.port";


    private static EmbeddedServer server;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LOG.info("==> Shutdown of Atlas");

                    shutdown();
                } catch (Exception e) {
                    LOG.error("Failed to shutdown", e);
                } finally {
                    LOG.info("<== Shutdown of Atlas");
                }
            }
        });
    }

    private static void shutdown() {
        server.stop();
    }

    /**
     * Prevent users from constructing this.
     */
    private Atlas() {
    }

    protected static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        Option opt;

        opt = new Option(APP_PATH, true, "Application Path");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(APP_PORT, true, "Application Port");
        opt.setRequired(false);
        options.addOption(opt);

        return new GnuParser().parse(options, args);
    }

    public static void main(String[] args) throws Exception {

        // Initialize OpenTelemetry as early as possible
        OpenTelemetry openTelemetry = initializeOpenTelemetry();
        // Install OpenTelemetry in logback appender
        io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender.install(
                openTelemetry);

        CommandLine cmd = parseArgs(args);
        PropertiesConfiguration buildConfiguration = new PropertiesConfiguration("atlas-buildinfo.properties");
        String appPath = "webapp/target/atlas-webapp-" + getProjectVersion(buildConfiguration);

        if (cmd.hasOption(APP_PATH)) {
            appPath = cmd.getOptionValue(APP_PATH);
        }

        setApplicationHome();
        Configuration configuration = ApplicationProperties.get();
        final String enableTLSFlag = configuration.getString(SecurityProperties.TLS_ENABLED);
        final String appHost = configuration.getString(SecurityProperties.BIND_ADDRESS, EmbeddedServer.ATLAS_DEFAULT_BIND_ADDRESS);

        if (!isLocalAddress(InetAddress.getByName(appHost))) {
            String msg =
                "Failed to start Atlas server. Address " + appHost
                    + " does not belong to this host. Correct configuration parameter: "
                    + SecurityProperties.BIND_ADDRESS;
            LOG.error(msg);
            throw new IOException(msg);
        }

        final int appPort = getApplicationPort(cmd, enableTLSFlag, configuration);
        System.setProperty(AtlasConstants.SYSTEM_PROPERTY_APP_PORT, String.valueOf(appPort));
        final boolean enableTLS = isTLSEnabled(enableTLSFlag, appPort);
        configuration.setProperty(SecurityProperties.TLS_ENABLED, String.valueOf(enableTLS));
        configuration.setProperty("atlas.graph.kafka.bootstrap.servers", configuration.getProperty("atlas.kafka.bootstrap.servers"));
        showStartupInfo(buildConfiguration, enableTLS, appPort);
        if (configuration.getProperty("atlas.graph.index.search.backend").equals("elasticsearch")) {
            initElasticsearch();
        }

        if (configuration.getString("atlas.authorizer.impl").equalsIgnoreCase("atlas")) {
            initAccessAuditElasticSearch(configuration);
        }

        server = EmbeddedServer.newServer(appHost, appPort, appPath, enableTLS);
        installLogBridge();

        server.start();
    }

    private static void setApplicationHome() {
        if (System.getProperty(ATLAS_HOME) == null) {
            System.setProperty(ATLAS_HOME, "target");
        }
        if (System.getProperty(ATLAS_DATA) == null) {
            System.setProperty(ATLAS_DATA, "target/data");
        }
        if (System.getProperty(ATLAS_LOG_DIR) == null) {
            System.setProperty(ATLAS_LOG_DIR, "target/logs");
        }
    }

    private static OpenTelemetry initializeOpenTelemetry() {
        // Check if OTel should be disabled (useful for local development without a collector)
        String otelDisabled = System.getenv("OTEL_SDK_DISABLED");
        if ("true".equalsIgnoreCase(otelDisabled)) {
            return OpenTelemetry.noop();
        }

        String otelResourceAttributes = System.getenv("OTEL_RESOURCE_ATTRIBUTES");

        List<String> customResourceAttr = null;

        if (otelResourceAttributes != null) {
            // Split the environment variable by commas
            String[] values = otelResourceAttributes.split(",");
            customResourceAttr = Arrays.asList(values);
        } else {
            customResourceAttr = Arrays.asList(AtlasConfiguration.OTEL_RESOURCE_ATTRIBUTES.getStringArray());
        }

        String serviceName = System.getenv("OTEL_SERVICE_NAME") != null ? System.getenv("OTEL_SERVICE_NAME") : AtlasConfiguration.OTEL_SERVICE_NAME.getString();
        String otelEndpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != null ? System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") : AtlasConfiguration.OTEL_EXPORTER_OTLP_ENDPOINT.getString();

        ResourceBuilder resourceBuilder = Resource.getDefault().toBuilder()
                .put(ResourceAttributes.SERVICE_NAME, serviceName);

        // Iterate through the ArrayList and add each attribute
        for (String attribute : customResourceAttr) {
            String[] keyValue = attribute.split("=");
            if (keyValue.length == 2) {
                String key = keyValue[0].trim();
                String value = keyValue[1].trim();
                resourceBuilder.put(key, value);
            }
        }

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .setSampler(Sampler.alwaysOn())
                        .build())
                .setLoggerProvider(
                        SdkLoggerProvider.builder()
                                .setResource(resourceBuilder.build())
                                .addLogRecordProcessor(
                                        BatchLogRecordProcessor.builder(
                                                        OtlpGrpcLogRecordExporter.builder()
                                                                .setEndpoint(otelEndpoint)
                                                                .build()
                                                )
                                                .build()
                                )
                                .build()
                )
                .build();

        // Add hook to close SDK, which flushes logs
        Runtime.getRuntime().addShutdownHook(new Thread(sdk::close));

        return sdk;
    }

    public static String getProjectVersion(PropertiesConfiguration buildConfiguration) {
        return buildConfiguration.getString("project.version");
    }

    static int getApplicationPort(CommandLine cmd, String enableTLSFlag, Configuration configuration) {
        String optionValue = cmd.hasOption(APP_PORT) ? cmd.getOptionValue(APP_PORT) : null;

        final int appPort;

        if (StringUtils.isNotEmpty(optionValue)) {
            appPort = Integer.valueOf(optionValue);
        } else {
            // default : atlas.enableTLS is true
            appPort = getPortValue(configuration, enableTLSFlag);
        }

        return appPort;
    }

    private static int getPortValue(Configuration configuration, String enableTLSFlag) {
        int appPort;

        assert configuration != null;
        appPort = StringUtils.isEmpty(enableTLSFlag) || enableTLSFlag.equals("true") ?
            configuration.getInt(ATLAS_SERVER_HTTPS_PORT, 21443) :
            configuration.getInt(ATLAS_SERVER_HTTP_PORT, 21000);
        return appPort;
    }

    private static boolean isTLSEnabled(String enableTLSFlag, int appPort) {
        return Boolean.valueOf(StringUtils.isEmpty(enableTLSFlag) ?
                System.getProperty(SecurityProperties.TLS_ENABLED, (appPort % 1000) == 443 ? "true" : "false") : enableTLSFlag);
    }

    private static boolean isLocalAddress(InetAddress addr) {
        // Check if the address is any local or loop back
        boolean local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();

        // Check if the address is defined on any interface
        if (!local) {
            try {
                local = NetworkInterface.getByInetAddress(addr) != null;
            } catch (SocketException e) {
                local = false;
            }
        }
        return local;
    }

    private static void showStartupInfo(PropertiesConfiguration buildConfiguration, boolean enableTLS, int appPort) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("\n############################################");
        buffer.append("############################################");
        buffer.append("\n                               Atlas Server (STARTUP)");
        buffer.append("\n");
        try {
            final Iterator<String> keys = buildConfiguration.getKeys();
            while (keys.hasNext()) {
                String key = keys.next();
                buffer.append('\n').append('\t').append(key).
                        append(":\t").append(buildConfiguration.getProperty(key));
            }
        } catch (Throwable e) {
            buffer.append("*** Unable to get build info ***");
        }
        buffer.append("\n############################################");
        buffer.append("############################################");
        LOG.info(buffer.toString());
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        LOG.info("Server starting with TLS ? {} on port {}", enableTLS, appPort);
        LOG.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
    }

    private static void installLogBridge() {
        // Optionally remove existing handlers attached to j.u.l root logger
        SLF4JBridgeHandler.removeHandlersForRootLogger();  // (since SLF4J 1.6.5)

        // add SLF4JBridgeHandler to j.u.l's root logger, should be done once during
        // the initialization phase of your application
        SLF4JBridgeHandler.install();
    }

    private static void initAccessAuditElasticSearch(Configuration configuration) throws IOException, AtlasException {
        AccessAuditLogsIndexCreator indexCreator = new AccessAuditLogsIndexCreator(configuration);
        indexCreator.start();
    }

    private static void initElasticsearch() throws IOException {
        RestHighLevelClient esClient = AtlasElasticsearchDatabase.getClient();
        IndexTemplatesExistRequest indexTemplateExistsRequest = new IndexTemplatesExistRequest("atlan-template");
        boolean exists = false;
        try {
            exists = esClient.indices().existsTemplate(indexTemplateExistsRequest, RequestOptions.DEFAULT);
            if (exists) {
                LOG.info("atlan-template es index template exists!");
            } else {
                LOG.info("atlan-template es index template does not exists!");
            }
        } catch (Exception es) {
            LOG.error("Caught exception: ", es.toString());
        }
        if (!exists) {
            String vertexIndex = INDEX_PREFIX + VERTEX_INDEX;
            PutIndexTemplateRequest request = new PutIndexTemplateRequest("atlan-template");
            request.patterns(Arrays.asList(vertexIndex));
            String atlasHomeDir  = System.getProperty("atlas.home");
            String elasticsearchSettingsFilePath = (org.apache.commons.lang3.StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir) + File.separator + "elasticsearch" + File.separator + "es-settings.json";
            File elasticsearchSettingsFile  = new File(elasticsearchSettingsFilePath);
            String jsonString  = new String(Files.readAllBytes(elasticsearchSettingsFile.toPath()), StandardCharsets.UTF_8);
            request.settings(jsonString, XContentType.JSON);

            String elasticsearchMappingsFilePath = (org.apache.commons.lang3.StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir) + File.separator + "elasticsearch" + File.separator + "es-mappings.json";
            File elasticsearchMappingsFile  = new File(elasticsearchMappingsFilePath);
            String mappingsJsonString  = new String(Files.readAllBytes(elasticsearchMappingsFile.toPath()), StandardCharsets.UTF_8);
            request.mapping(mappingsJsonString, XContentType.JSON);

            try {
                AcknowledgedResponse putTemplateResponse = esClient.indices().putTemplate(request, RequestOptions.DEFAULT);
                if (putTemplateResponse.isAcknowledged()) {
                    LOG.info("Atlan index template created.");
                } else {
                    LOG.error("error creating atlan index template");
                }
            } catch (Exception e) {
                LOG.error("Caught exception: ", e.toString());
                throw e;
            }
        }
    }
}

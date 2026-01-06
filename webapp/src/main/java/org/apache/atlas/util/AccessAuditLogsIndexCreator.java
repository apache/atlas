/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.util;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.atlas.repository.audit.ESBasedAuditRepository.INDEX_WRITE_BACKEND_CONF;
import static org.apache.atlas.repository.audit.ESBasedAuditRepository.getESHosts;

public class AccessAuditLogsIndexCreator extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(AccessAuditLogsIndexCreator.class);

    private static final String ES_CONFIG_USERNAME = "atlas.audit.elasticsearch.user";
    private static final String ES_CONFIG_PASSWORD = "atlas.audit.elasticsearch.password";
    private static final String ES_CONFIG_PORT = "atlas.audit.elasticsearch.port";
    private static final String ES_CONFIG_PROTOCOL = "atlas.audit.elasticsearch.protocol";
    private static final String ES_CONFIG_INDEX = "atlas.audit.elasticsearch.index";
    private static final String ES_TIME_INTERVAL = "atlas.audit.elasticsearch.time.interval";
    private static final String ES_NO_SHARDS = "atlas.audit.elasticsearch.no.shards";
    private static final String ES_NO_REPLICA = "atlas.audit.elasticsearch.no.replica";
    private static final String ES_CREDENTIAL_PROVIDER_PATH = "atlas.credential.provider.path";
    private static final String ES_CREDENTIAL_ALIAS = "atlas.audit.elasticsearch.credential.alias";
    private static final String ES_BOOTSTRAP_MAX_RETRY = "atlas.audit.elasticsearch.max.retry";

    private static final String DEFAULT_INDEX_NAME = "ranger-audit";
    private static final String ES_RANGER_AUDIT_SCHEMA_FILE = "atlas-auth-es-schema.json";

    private static final long DEFAULT_ES_TIME_INTERVAL_MS = 30000L;
    private static final int TRY_UNTIL_SUCCESS = -1;
    private static final int DEFAULT_ES_BOOTSTRAP_MAX_RETRY = 3;

    private final AtomicLong lastLoggedAt = new AtomicLong(0);
    private volatile RestClient client = null;
    private Long time_interval;

    private String user;
    private String password;
    List<HttpHost> hosts;
    private String protocol;
    private String index;
    private String es_ranger_audit_schema_json;

    private int port;
    private int max_retry;
    private int retry_counter = 0;
    private int no_of_replicas;
    private int no_of_shards;
    private boolean is_completed = false;

    public AccessAuditLogsIndexCreator(Configuration configuration) throws IOException, AtlasException {
        LOG.debug("Starting Ranger audit schema setup in ElasticSearch.");
        time_interval = configuration.getLong(ES_TIME_INTERVAL, DEFAULT_ES_TIME_INTERVAL_MS);
        user = configuration.getString(ES_CONFIG_USERNAME);

        hosts = getHttpHosts();
        port = getPort();

        protocol = configuration.getString(ES_CONFIG_PROTOCOL, "http");
        index = configuration.getString(ES_CONFIG_INDEX, DEFAULT_INDEX_NAME);
        password = configuration.getString(ES_CONFIG_PASSWORD);

        no_of_replicas = configuration.getInt(ES_NO_REPLICA, 1);
        no_of_shards = configuration.getInt(ES_NO_SHARDS, 1);
        max_retry = configuration.getInt(ES_BOOTSTRAP_MAX_RETRY, DEFAULT_ES_BOOTSTRAP_MAX_RETRY);

        String atlasHomeDir  = System.getProperty("atlas.home");
        String elasticsearchFilePath = (StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir) + File.separator + "elasticsearch" + File.separator + ES_RANGER_AUDIT_SCHEMA_FILE;
        Path es_schema_path = Paths.get(elasticsearchFilePath);
        es_ranger_audit_schema_json = new String(Files.readAllBytes(es_schema_path), StandardCharsets.UTF_8);

        String providerPath = configuration.getString(ES_CREDENTIAL_PROVIDER_PATH);
        String credentialAlias = configuration.getString(ES_CREDENTIAL_ALIAS, ES_CONFIG_PASSWORD);
        if (providerPath != null && credentialAlias != null) {
            if (StringUtils.isBlank(password) || "none".equalsIgnoreCase(password.trim())) {
                password = configuration.getString(ES_CONFIG_PASSWORD);
            }
        }
    }

    private String connectionString() {
        return String.format(Locale.ROOT,"User:%s, %s://%s:%s/%s", user, protocol, hosts, port, index);
    }

    @Override
    public void run() {
        LOG.debug("Started run method");
        if (CollectionUtils.isNotEmpty(hosts)) {
            LOG.debug("Elastic search hosts=" + hosts + ", index=" + index);
            while (!is_completed && (max_retry == TRY_UNTIL_SUCCESS || retry_counter < max_retry)) {
                try {
                    LOG.debug("Trying to acquire elastic search connection");
                    if (connect()) {
                        LOG.debug("Connection to elastic search established successfully");
                        if (createIndex()) {
                            is_completed = true;
                            break;
                        } else {
                            logErrorMessageAndWait("Error while performing operations on elasticsearch. ", null);
                        }
                    } else {
                        logErrorMessageAndWait(
                                "Cannot connect to elasticsearch kindly check the elasticsearch related configs. ",
                                null);
                    }
                } catch (Exception ex) {
                    logErrorMessageAndWait("Error while validating elasticsearch index ", ex);
                } finally {
                    try {
                        if (client != null) {
                            client.close();
                        }
                    } catch (IOException e) {
                        LOG.warn("AccessAuditLogsIndexCreator: Failed to close ES client", e);
                    }
                }
            }
        } else {
            LOG.error("elasticsearch hosts values are empty. Please set property "+ INDEX_WRITE_BACKEND_CONF);
        }

    }

    private synchronized boolean connect() {
        if (client == null) {
            synchronized (AccessAuditLogsIndexCreator.class) {
                if (client == null) {
                    try {
                        createClient();
                    } catch (Exception ex) {
                        LOG.error("Can't connect to elasticsearch server. host=" + hosts + ", index=" + index + ex);
                    }
                }
            }
        }
        return client != null;
    }

    private void createClient() {
        try {
            RestClientBuilder builder = RestClient.builder(hosts.get(0));
            builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                    .setConnectTimeout(AtlasConfiguration.INDEX_CLIENT_CONNECTION_TIMEOUT.getInt())
                    .setSocketTimeout(AtlasConfiguration.INDEX_CLIENT_SOCKET_TIMEOUT.getInt()));

            client = builder.build();
        } catch (Throwable t) {
            lastLoggedAt.updateAndGet(lastLoggedAt -> {
                long now = System.currentTimeMillis();
                long elapsed = now - lastLoggedAt;
                if (elapsed > TimeUnit.MINUTES.toMillis(1)) {
                    LOG.error("Can't connect to ElasticSearch server: " + connectionString() + t);
                    return now;
                } else {
                    return lastLoggedAt;
                }
            });
        }
    }

    private boolean createIndex() {
        boolean exists = false;
        if (client == null) {
            connect();
        }
        if (client != null) {
            try {
                Request request = new Request("HEAD", index);
                Response response = client.performRequest(request);

                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200) {
                    LOG.debug("Entity audits index exists!");
                    exists = true;
                } else {
                    LOG.debug("Entity audits index does not exist!");
                    exists = false;
                }

            } catch (Exception e) {
                LOG.warn("Index " + this.index + " not available.");
            }
            if (!exists) {
                LOG.debug("Index does not exist. Attempting to create index:" + this.index);
                try {
                    HttpEntity entity = new NStringEntity(es_ranger_audit_schema_json, ContentType.APPLICATION_JSON);
                    Request request = new Request("PUT", index);

                    /*if (this.no_of_shards >= 0 && this.no_of_replicas >= 0) {
                        request.settings(Settings.builder().put("number_of_shards", this.no_of_shards)
                                .put("number_of_replicas", this.no_of_replicas));
                    }*/

                    request.setEntity(entity);
                    Response response = client.performRequest(request);

                    if (response != null && response.getStatusLine().getStatusCode() == 200) {
                        LOG.debug("Index " + this.index + " created successfully.");
                        exists = true;
                    }
                } catch (Exception e) {
                    LOG.error("Unable to create Index. Reason:" + e.toString());
                    e.printStackTrace();
                }
            } else {
                LOG.debug("Index " + this.index + " is already created.");
            }
        }
        return exists;
    }

    private void logErrorMessageAndWait(String msg, Exception exception) {
        retry_counter++;
        String attemptMessage;
        if (max_retry != TRY_UNTIL_SUCCESS) {
            attemptMessage = (retry_counter == max_retry) ? ("Maximum attempts reached for setting up elasticsearch.")
                    : ("[retrying after " + time_interval + " ms]. No. of attempts left : "
                    + (max_retry - retry_counter) + " . Maximum attempts : " + max_retry);
        } else {
            attemptMessage = "[retrying after " + time_interval + " ms]";
        }
        StringBuilder errorBuilder = new StringBuilder();
        errorBuilder.append(msg);
        if (exception != null) {
            errorBuilder.append("Error : ".concat(exception.getMessage() + ". "));
        }
        errorBuilder.append(attemptMessage);
        LOG.error(errorBuilder.toString());
        try {
            Thread.sleep(time_interval);
        } catch (InterruptedException ex) {
            LOG.info("sleep interrupted: " + ex.getMessage());
        }
    }

    public static List<HttpHost> getHttpHosts() throws AtlasException {
        List<HttpHost> httpHosts = new ArrayList<>();

        String indexConf = getESHosts();
        String[] hosts = indexConf.split(",");
        for (String host : hosts) {
            host = host.trim();
            String[] hostAndPort = host.split(":");
            if (hostAndPort.length == 1) {
                httpHosts.add(new HttpHost(hostAndPort[0]));
            } else if (hostAndPort.length == 2) {
                httpHosts.add(new HttpHost(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
            }
        }
        return httpHosts;
    }


    private static int getPort() throws AtlasException {
        int port = 9200;
        String indexConf = getESHosts();
        try {
            String[] hosts = indexConf.split(",");
            String host = hosts[0];
            host = host.trim();
            String[] hostAndPort = host.split(":");
            port = Integer.parseInt(hostAndPort[1]);
        } catch (Exception e) {
            LOG.warn("Setting ES port to default {}", port);
        }

        return port;
    }
}

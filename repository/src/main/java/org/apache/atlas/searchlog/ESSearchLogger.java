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
package org.apache.atlas.searchlog;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.searchlog.SearchRequestLogData;
import org.apache.atlas.service.Service;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.AtlasConfiguration.INDEX_CLIENT_CONNECTION_TIMEOUT;
import static org.apache.atlas.AtlasConfiguration.INDEX_CLIENT_SOCKET_TIMEOUT;
import static org.apache.atlas.repository.audit.ESBasedAuditRepository.getHttpHosts;

@Component
public class ESSearchLogger implements SearchLogger, Service {
    private static final Logger LOG = LoggerFactory.getLogger(ESSearchLogger.class);

    public static final String INDEX_NAME          = "search_logs";
    public static final String MAPPINGS_FILE_NAME  = "es-search-logs-mappings.json";
    public static final String ENDPOINT_CREATE_DOC = INDEX_NAME + "/_doc";

    private RestClient lowLevelClient;

    @Override
    public void log(SearchRequestLogData searchRequestLogData) {
        try {
            searchRequestLogData.setCreated(System.currentTimeMillis());

            HttpEntity entity = new NStringEntity(AtlasType.toJson(searchRequestLogData), ContentType.APPLICATION_JSON);

            Request request = new Request("POST", ENDPOINT_CREATE_DOC);
            request.setEntity(entity);

            Response response = lowLevelClient.performRequest(request);
            int responseCode = response.getStatusLine().getStatusCode();

            if (responseCode != 200 && responseCode != 201) {
                String responseString = EntityUtils.toString(response.getEntity());
                Map<String, Object> responseMap = AtlasType.fromJson(responseString, Map.class);
                if ((boolean) responseMap.get("errors")) {
                    List<String> errors = new ArrayList<>();
                    List<Map<String, Object>> resultItems = (List<Map<String, Object>>) responseMap.get("items");
                    for (Map<String, Object> resultItem : resultItems) {
                        if (resultItem.get("index") != null) {
                            Map<String, Object> resultIndex = (Map<String, Object>) resultItem.get("index");
                            if (resultIndex.get("error") != null) {
                                errors.add(resultIndex.get("error").toString());
                            }
                        }
                    }
                    throw new AtlasException(errors.toString());
                }
                throw new Exception();
            }

        } catch (Exception e) {
            LOG.error("Unable to push search log to ES: {}", e.getMessage());
        }
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("ESSearchLogger: start!");
        setLowLevelClient();
        try {
            if (!indexExists()) {
                LOG.info("Create ES index for entity search logging in ES based logger");
                if (createIndex()) {
                    LOG.info("Create ES index with name {}", INDEX_NAME);
                } else {
                    LOG.info("Failed to create ES index with name {}", INDEX_NAME);
                }
            }
        } catch (IOException e) {
            LOG.error("ESSearchLogger: Failed to start", e);
            throw new AtlasException(e);
        }
    }

    @Override
    public void stop() throws AtlasException {
        try {
            LOG.info("ESSearchLogger: stop!");
            if (lowLevelClient != null) {
                lowLevelClient.close();
                lowLevelClient = null;
            }
        } catch (IOException e) {
            LOG.error("ESSearchLogger: Failed to close ES client", e);
            throw new AtlasException(e);
        }
    }

    private void setLowLevelClient() throws AtlasException {
        synchronized (ESSearchLogger.class) {
            if (lowLevelClient == null) {
                try {
                    List<HttpHost> httpHosts = getHttpHosts();

                    RestClientBuilder builder = RestClient.builder(httpHosts.get(0));
                    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                            .setConnectTimeout(AtlasConfiguration.INDEX_CLIENT_CONNECTION_TIMEOUT.getInt())
                            .setSocketTimeout(AtlasConfiguration.INDEX_CLIENT_SOCKET_TIMEOUT.getInt()));

                    lowLevelClient = builder.build();
                } catch (AtlasException e) {
                    LOG.error("Failed to initialize low level rest client for ES");
                    throw new AtlasException(e);
                }
            }
        }
    }

    private boolean indexExists() throws IOException {
        Request request = new Request("HEAD", INDEX_NAME);
        Response response = lowLevelClient.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200) {
            LOG.info("ESSearchLogger: Entity audits index exists!");
            return true;
        }
        LOG.info("ESSearchLogger: Entity audits index does not exist!");
        return false;
    }

    private boolean createIndex() throws IOException {
        LOG.info("ESSearchLogger: createIndex");
        String esMappingsString = getIndexMappings();

        HttpEntity entity = new NStringEntity(esMappingsString, ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", INDEX_NAME);
        request.setEntity(entity);
        Response response = lowLevelClient.performRequest(request);

        return response.getStatusLine().getStatusCode() == 200;
    }

    private String getIndexMappings() throws IOException {
        String atlasHomeDir = System.getProperty("atlas.home");
        String elasticsearchSettingsFilePath = (StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir) + File.separator + "elasticsearch" + File.separator + MAPPINGS_FILE_NAME;
        File elasticsearchSettingsFile  = new File(elasticsearchSettingsFilePath);
        return new String(Files.readAllBytes(elasticsearchSettingsFile.toPath()), StandardCharsets.UTF_8);
    }
}


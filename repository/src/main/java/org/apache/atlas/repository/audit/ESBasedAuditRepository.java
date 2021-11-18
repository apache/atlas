/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.audit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
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
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.inject.Singleton;

/**
 * This class provides cassandra support as the backend for audit storage support.
 */
@Singleton
@Component
@ConditionalOnAtlasProperty(property = "atlas.EntityAuditRepositorySearch.impl")
public class ESBasedAuditRepository extends AbstractStorageBasedAuditRepository {
    private static final Logger LOG = LoggerFactory.getLogger(ESBasedAuditRepository.class);
    public static final String INDEX_BACKEND_CONF = "atlas.graph.index.search.hostname";
    public static final String INDEX_NAME = "entity_audits";

    private RestClient lowLevelClient;

    @Override
    public void putEventsV1(List<EntityAuditEvent> events) throws AtlasException {

    }

    @Override
    public List<EntityAuditEvent> listEventsV1(String entityId, String startKey, short n) throws AtlasException {
        return null;
    }

    @Override
    public void putEventsV2(List<EntityAuditEventV2> events) throws AtlasBaseException {
        try {
            String requestPayload = "";
            HttpEntity entity = new NStringEntity(requestPayload, ContentType.APPLICATION_JSON);
            Request request = new Request("PUT", INDEX_NAME);
            request.setEntity(entity);
            Response response = lowLevelClient.performRequest(request);
        } catch (Exception e) {
            throw new AtlasBaseException(e);
        }
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String startKey, short maxResultCount) throws AtlasBaseException {
        return null;
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String sortByColumn, boolean sortOrderDesc, int offset, short limit) throws AtlasBaseException {
        return null;
    }

    @Override
    public Set<String> getEntitiesWithTagChanges(long fromTimestamp, long toTimestamp) throws AtlasBaseException {
        throw new NotImplementedException();
    }

    @Override
    public void start() throws AtlasException {
        initApplicationProperties();
        initializeSettings();
        startInternal();
    }

    void initializeSettings() {
    }

    @VisibleForTesting
    void startInternal() throws AtlasException {
        createSession();
    }

    void createSession() throws AtlasException {
        setLowLevelClient();
        try {
            boolean indexExists = checkIfIndexExists();
            if (!indexExists) {
                createAuditIndex();
            }
        } catch (IOException e) {
            LOG.error("error", e);
            throw new AtlasException(e);
        }

    }

    private boolean createAuditIndex() throws IOException {
        String esMappingsString = getAuditIndexMappings();
        HttpEntity entity = new NStringEntity(esMappingsString, ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", INDEX_NAME);
        request.setEntity(entity);
        Response response = lowLevelClient.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();;
        return statusCode == 200 ? true: false;
    }

    private String getAuditIndexMappings() throws IOException {
        String atlasHomeDir  = System.getProperty("atlas.home");
        String elasticsearchSettingsFilePath = (org.apache.commons.lang3.StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir) + File.separator + "elasticsearch" + File.separator + "es-audit-mappings.json";
        File elasticsearchSettingsFile  = new File(elasticsearchSettingsFilePath);
        String jsonString  = new String(Files.readAllBytes(elasticsearchSettingsFile.toPath()), StandardCharsets.UTF_8);
        return jsonString;
    }

     private boolean checkIfIndexExists() throws IOException {
         Request request = new Request("HEAD", INDEX_NAME);
         Response response = lowLevelClient.performRequest(request);
         int statusCode = response.getStatusLine().getStatusCode();;
         if (statusCode == 200) {
             return true;
         }
         return false;
     }

    @Override
    public void stop() throws AtlasException {
    }

    private void setLowLevelClient() throws AtlasException {
        if (lowLevelClient == null) {
            try {
                List<HttpHost> httpHosts = getHttpHosts();

                RestClientBuilder builder = RestClient.builder(httpHosts.get(0));
                builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(900000)
                        .setSocketTimeout(900000));

                lowLevelClient = builder.build();
            } catch (AtlasException e) {
                LOG.error("Failed to initialize low level rest client for ES");
                throw new AtlasException(e);
            }
        }
    }

    public static List<HttpHost> getHttpHosts() throws AtlasException {
        List<HttpHost> httpHosts = new ArrayList<>();
        Configuration configuration = ApplicationProperties.get();
        String indexConf = configuration.getString(INDEX_BACKEND_CONF);
        String[] hosts = indexConf.split(",");
        for (String host: hosts) {
            host = host.trim();
            String[] hostAndPort = host.split(":");
            if (hostAndPort.length == 1) {
                httpHosts.add(new HttpHost(hostAndPort[0]));
            } else if (hostAndPort.length == 2) {
                httpHosts.add(new HttpHost(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
            } else {
                throw new AtlasException("Invalid config");
            }
        }
        return httpHosts;
    }

}

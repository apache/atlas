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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.EntityAuditEvent;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.*;

import static java.nio.charset.Charset.defaultCharset;
import static org.apache.atlas.repository.Constants.DOMAIN_GUIDS;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.INDEX_BACKEND_CONF;
import static org.springframework.util.StreamUtils.copyToString;

/**
 * This class provides cassandra support as the backend for audit storage support.
 */
@Singleton
@Component
@Order(8)
@ConditionalOnAtlasProperty(property = "atlas.EntityAuditRepositorySearch.impl")
public class ESBasedAuditRepository extends AbstractStorageBasedAuditRepository {
    private static final Logger LOG = LoggerFactory.getLogger(ESBasedAuditRepository.class);
    public static final String INDEX_WRITE_BACKEND_CONF = "atlas.graph.index.search.write.hostname";
    private static final String TOTAL_FIELD_LIMIT = "atlas.index.audit.elasticsearch.total_field_limit";
    public static final String INDEX_NAME = "entity_audits";
    private static final String ENTITYID = "entityId";
    private static final String TYPE_NAME = "typeName";
    private static final String ENTITY_QUALIFIED_NAME = "entityQualifiedName";
    private static final String CREATED = "created";
    private static final String TIMESTAMP = "timestamp";
    private static final String EVENT_KEY = "eventKey";
    private static final String ACTION = "action";
    private static final String USER = "user";
    private static final String DETAIL = "detail";
    private static final String ENTITY = "entity";
    private static final String bulkMetadata = String.format("{ \"index\" : { \"_index\" : \"%s\" } }%n", INDEX_NAME);
    private static final Set<String> ALLOWED_LINKED_ATTRIBUTES = new HashSet<>(Arrays.asList(DOMAIN_GUIDS));
    private static final String ENTITY_AUDITS_INDEX = "entity_audits";

    /*
    *    created   → event creation time
         timestamp → entity modified timestamp
         eventKey  → entityId:timestamp
    * */

    private RestClient lowLevelClient;
    private final Configuration configuration;
    private EntityGraphRetriever entityGraphRetriever;

    @Inject
    public ESBasedAuditRepository(Configuration configuration, EntityGraphRetriever entityGraphRetriever) {
        this.configuration = configuration;
        this.entityGraphRetriever = entityGraphRetriever;
    }

    @Override
    public void putEventsV1(List<EntityAuditEvent> events) throws AtlasException {

    }

    @Override
    public List<EntityAuditEvent> listEventsV1(String entityId, String startKey, short n) throws AtlasException {
        return null;
    }

    @Override
    public void putEventsV2(List<EntityAuditEventV2> events) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("pushInES");

        try {
            if (CollectionUtils.isEmpty(events)) {
                return;
            }

            Map<String, String> requestContextHeaders = RequestContext.get().getRequestContextHeaders();
            String entityPayloadTemplate = getQueryTemplate(requestContextHeaders);

            StringBuilder bulkRequestBody = new StringBuilder();
            for (EntityAuditEventV2 event : events) {
                String created = String.format("%s", event.getTimestamp());
                String auditDetailPrefix = EntityAuditListenerV2.getV2AuditPrefix(event.getAction());
                String details = event.getDetails().substring(auditDetailPrefix.length());

                AtlasEntity auditEntity = event.getEntity();

                if (auditEntity == null) {
                    LOG.warn("Audit entity is null for event (entityId={}, action={}); skipping ES audit record",
                            event.getEntityId(), event.getAction());
                    continue;
                }

                String typeName = auditEntity.getTypeName();
                long   updateTimestamp;

                if (auditEntity.getUpdateTime() != null) {
                    updateTimestamp = auditEntity.getUpdateTime().getTime();
                } else {
                    updateTimestamp = event.getTimestamp();
                    LOG.warn("Entity updateTime is null for audit event (entityId={}, type={}); using event timestamp as fallback",
                            event.getEntityId(), typeName);
                }

                String bulkItem = MessageFormat.format(entityPayloadTemplate,
                        event.getEntityId(),
                        event.getAction(),
                        details,
                        event.getUser(),
                        event.getEntityId() + ":" + updateTimestamp,
                        event.getEntityQualifiedName(),
                        typeName,
                        created,
                        "" + updateTimestamp);

                bulkRequestBody.append(bulkMetadata);
                bulkRequestBody.append(bulkItem);
                bulkRequestBody.append("\n");
            }
            String endpoint = INDEX_NAME + "/_bulk";
            HttpEntity entity = new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);
            Request request = new Request("POST", endpoint);
            request.setEntity(entity);

            int maxRetries = AtlasConfiguration.ES_MAX_RETRIES.getInt();
            long initialRetryDelay = AtlasConfiguration.ES_RETRY_DELAY_MS.getLong();

            for (int retryCount = 0; retryCount < maxRetries; retryCount++) {
                Response response = null;
                try {
                    response = lowLevelClient.performRequest(request);
                    int statusCode = response.getStatusLine().getStatusCode();

                    // Accept any 2xx status code as a success.
                    if (statusCode >= 200 && statusCode < 300) {
                        String responseString = EntityUtils.toString(response.getEntity());
                        Map<String, Object> responseMap = AtlasType.fromJson(responseString, Map.class);

                        if ((boolean) responseMap.get("errors")) {
                            LOG.error("Elasticsearch returned errors for bulk audit event request. Full response: {}", responseString);
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
                            throw new AtlasBaseException("Error pushing entity audits to ES: " + errors);
                        }
                        return; // Success, exit the method.
                    }

                    String responseBody = EntityUtils.toString(response.getEntity());

                    if ((statusCode >= 500 && statusCode < 600) || statusCode==429) {
                        LOG.warn("Failed to push entity audits to ES due to server error ({}). Retrying... ({}/{}) Response: {}",
                                statusCode, retryCount + 1, maxRetries, responseBody);
                    } else {
                        throw new AtlasBaseException("Unable to push entity audits to ES. Status code: " + statusCode + ", Response: " + responseBody);
                    }

                } catch (IOException e) {
                    LOG.warn("Failed to push entity audits to ES due to IOException. Retrying... ({}/{})", retryCount + 1, maxRetries, e);
                }

                if (retryCount < maxRetries - 1) {
                    try {
                        long exponentialBackoffDelay = initialRetryDelay * (long) Math.pow(2, retryCount);
                        Thread.sleep(exponentialBackoffDelay);
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        throw new AtlasBaseException("ES audit push interrupted during retry delay", interruptedException);
                    }
                }
            }

            LOG.error("Failed to push entity audits to ES after {} retries", maxRetries);
            throw new AtlasBaseException("Unable to push entity audits to ES after " + maxRetries + " retries");

        } catch (Exception e) {
            if (e instanceof AtlasBaseException) {
                throw e;
            }
            throw new AtlasBaseException("Unable to push entity audits to ES", e);
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    private String getQueryTemplate(Map<String, String> requestContextHeaders) {
        StringBuilder template = new StringBuilder();

        template.append("'{'\"entityId\":\"{0}\",\"action\":\"{1}\",\"detail\":{2},\"user\":\"{3}\", \"eventKey\":\"{4}\", " +
                        "\"entityQualifiedName\": {5}, \"typeName\": \"{6}\",\"created\":{7}, \"timestamp\":{8}");

        if (MapUtils.isNotEmpty(requestContextHeaders)) {
            template.append(",")
                    .append("\"").append("headers").append("\"")
                    .append(":")
                    .append(AtlasType.toJson(requestContextHeaders).replaceAll("\\{", "'{").replaceAll("\\}", "'}"));

        }

        template.append("'}'");

        return template.toString();
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String startKey, short maxResultCount) throws AtlasBaseException {
        List<EntityAuditEventV2> ret;
        String queryTemplate = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"entityId\":\"%s\"}},{\"term\":{\"action\":\"%s\"}}]}}}";
        String queryWithEntityFilter = String.format(queryTemplate, entityId, auditAction);
        ret = searchEvents(queryWithEntityFilter).getEntityAudits();

        return ret;
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String sortByColumn, boolean sortOrderDesc, int offset, short limit) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityAuditSearchResult searchEvents(String queryString) throws AtlasBaseException {
        try {
            String response = performSearchOnIndex(queryString);
            return getResultFromResponse(response);
        } catch (IOException e) {
            throw new AtlasBaseException(e);
        }
    }

    private EntityAuditSearchResult getResultFromResponse(String responseString) throws AtlasBaseException {
        List<EntityAuditEventV2> entityAudits = new ArrayList<>();
        EntityAuditSearchResult searchResult = new EntityAuditSearchResult();
        Map<String, Object> responseMap = AtlasType.fromJson(responseString, Map.class);
        Map<String, Object> hits_0 = (Map<String, Object>) responseMap.get("hits");
        List<LinkedHashMap> hits_1 = (List<LinkedHashMap>) hits_0.get("hits");
        Map<String, AtlasEntityHeader> existingLinkedEntities = searchResult.getLinkedEntities();

        for (LinkedHashMap hit : hits_1) {
            Map source = (Map) hit.get("_source");
            String entityGuid = (String) source.get(ENTITYID);
            EntityAuditEventV2 event = new EntityAuditEventV2();
            event.setEntityId(entityGuid);
            event.setAction(EntityAuditEventV2.EntityAuditActionV2.fromString((String) source.get(ACTION)));
            event.setDetail((Map<String, Object>) source.get(DETAIL));
            event.setUser((String) source.get(USER));
            event.setCreated((long) source.get(CREATED));
            if (source.get(TIMESTAMP) != null) {
                event.setTimestamp((long) source.get(TIMESTAMP));
            }
            if (source.get(TYPE_NAME) != null) {
                event.setTypeName((String) source.get(TYPE_NAME));
            }

            event.setEntityQualifiedName((String) source.get(ENTITY_QUALIFIED_NAME));

            String eventKey = (String) source.get(EVENT_KEY);
            if (StringUtils.isEmpty(eventKey)) {
                eventKey = event.getEntityId() + ":" + event.getTimestamp();
            }

            Map<String, Object> detail = event.getDetail();
            if (detail != null && detail.containsKey("attributes")) {
                Map<String, Object> attributes = (Map<String, Object>) detail.get("attributes");

                for (Map.Entry<String, Object> entry: attributes.entrySet()) {
                    if (ALLOWED_LINKED_ATTRIBUTES.contains(entry.getKey())) {
                        List<String> guids = (List<String>) entry.getValue();

                        if (guids != null && !guids.isEmpty()){
                            for (String guid: guids){
                                if(!existingLinkedEntities.containsKey(guid)){
                                    try {
                                        AtlasEntityHeader entityHeader = fetchAtlasEntityHeader(guid);
                                        if (entityHeader != null) {
                                            existingLinkedEntities.put(guid, entityHeader);
                                        }
                                    } catch (AtlasBaseException e) {
                                        LOG.error("Error while fetching entity header for guid: {}", guid, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            event.setHeaders((Map<String, String>) source.get("headers"));

            event.setEventKey(eventKey);
            entityAudits.add(event);
        }
        Map<String, Object> aggregationsMap = (Map<String, Object>) responseMap.get("aggregations");
        Map<String, Object> countObject = (Map<String, Object>) hits_0.get("total");
        int totalCount = (int) countObject.get("value");
        searchResult.setEntityAudits(entityAudits);
        searchResult.setLinkedEntities(existingLinkedEntities);
        searchResult.setAggregations(aggregationsMap);
        searchResult.setTotalCount(totalCount);
        searchResult.setCount(entityAudits.size());
        return searchResult;
    }

    private AtlasEntityHeader fetchAtlasEntityHeader(String domainGUID) throws AtlasBaseException {
        try {
            AtlasEntityHeader entityHeader = entityGraphRetriever.toAtlasEntityHeader(domainGUID);
            return entityHeader;
        } catch (AtlasBaseException e) {
            throw new AtlasBaseException(e);
        }
    }

    private String performSearchOnIndex(String queryString) throws IOException {
        HttpEntity entity = new NStringEntity(queryString, ContentType.APPLICATION_JSON);
        String endPoint = INDEX_NAME + "/_search";
        Request request = new Request("GET", endPoint);
        request.setEntity(entity);
        Response response = lowLevelClient.performRequest(request);
        return EntityUtils.toString(response.getEntity());
    }

    @Override
    public Set<String> getEntitiesWithTagChanges(long fromTimestamp, long toTimestamp) throws AtlasBaseException {
        throw new NotImplementedException();
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("ESBasedAuditRepo - start!");
        initApplicationProperties();
        startInternal();
    }

    @VisibleForTesting
    void startInternal() throws AtlasException {
        createSession();
    }

    void createSession() throws AtlasException {
        LOG.info("Create ES Session in ES Based Audit Repo");
        setLowLevelClient();
        try {
            boolean indexExists = checkIfIndexExists();
            if (!indexExists) {
                LOG.info("Create ES index for entity audits in ES Based Audit Repo");
                createAuditIndex();
            }
            if (shouldUpdateFieldLimitSetting()) {
                LOG.info("Updating ES total field limit");
                updateFieldLimit();
            }
            updateMappingsIfChanged();
        } catch (IOException e) {
            LOG.error("error", e);
            throw new AtlasException(e);
        }

    }

    private boolean checkIfIndexExists() throws IOException {
        Request request = new Request("HEAD", INDEX_NAME);
        Response response = lowLevelClient.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200) {
            LOG.info("Entity audits index exists!");
            return true;
        }
        LOG.info("Entity audits index does not exist!");
        return false;
    }

    private boolean createAuditIndex() throws IOException {
        LOG.info("ESBasedAuditRepo - createAuditIndex!");
        String esMappingsString = getAuditIndexMappings();
        HttpEntity entity = new NStringEntity(esMappingsString, ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", INDEX_NAME);
        request.setEntity(entity);
        Response response = lowLevelClient.performRequest(request);
        return isSuccess(response);
    }

    private boolean shouldUpdateFieldLimitSetting() {
        JsonNode currentFieldLimit;
        try {
            currentFieldLimit = getIndexFieldLimit();
        } catch (IOException e) {
            LOG.error("Problem while retrieving the index field limit!", e);
            return false;
        }
        Integer fieldLimitFromConfigurationFile = configuration.getInt(TOTAL_FIELD_LIMIT, 0);
        return currentFieldLimit == null || fieldLimitFromConfigurationFile > currentFieldLimit.asInt();
    }

    private JsonNode getIndexFieldLimit() throws IOException {
        Request request = new Request("GET", INDEX_NAME + "/_settings");
        Response response = lowLevelClient.performRequest(request);
        ObjectMapper objectMapper = new ObjectMapper();
        String fieldPath = String.format("/%s/settings/index/mapping/total_fields/limit", INDEX_NAME);

        return objectMapper.readTree(copyToString(response.getEntity().getContent(), Charset.defaultCharset())).at(fieldPath);
    }

    private void updateFieldLimit() {
        Request request = new Request("PUT", INDEX_NAME + "/_settings");
        String requestBody = String.format("{\"index.mapping.total_fields.limit\": %d}", configuration.getInt(TOTAL_FIELD_LIMIT));
        HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
        request.setEntity(entity);
        Response response;
        try {
            response = lowLevelClient.performRequest(request);
            if (response.getStatusLine().getStatusCode() != 200) {
                LOG.error("Error while updating the Elasticsearch total field limits! Error: " + copyToString(response.getEntity().getContent(), defaultCharset()));
            } else {
                LOG.info("ES total field limit has been updated");
            }
        } catch (IOException e) {
            LOG.error("Error while updating the field limit", e);
        }
    }

    private void updateMappingsIfChanged() throws IOException, AtlasException {
        LOG.info("ESBasedAuditRepo - updateMappings!");
        ObjectMapper mapper = new ObjectMapper();
        Map<String, JsonNode> activeIndexMappings = getActiveIndexMappings(mapper);
        JsonNode indexInformationFromConfigurationFile = mapper.readTree(getAuditIndexMappings());
        for (String activeAuditIndex : activeIndexMappings.keySet()) {
            if (!areConfigurationsSame(activeIndexMappings.get(activeAuditIndex), indexInformationFromConfigurationFile)) {
                Response response = updateMappings(indexInformationFromConfigurationFile);
                if (isSuccess(response)) {
                    LOG.info("ESBasedAuditRepo - Elasticsearch mappings have been updated for index: {}", activeAuditIndex);
                } else {
                    LOG.error("Error while updating the Elasticsearch indexes for index: {}", activeAuditIndex);
                    throw new AtlasException(copyToString(response.getEntity().getContent(), Charset.defaultCharset()));
                }
            }
        }
    }

    private Map<String, JsonNode> getActiveIndexMappings(ObjectMapper mapper) throws IOException {
        Request request = new Request("GET", INDEX_NAME);
        Response response = lowLevelClient.performRequest(request);
        String responseString = copyToString(response.getEntity().getContent(), Charset.defaultCharset());
        Map<String, JsonNode> indexMappings = new TreeMap<>();
        JsonNode rootNode = mapper.readTree(responseString);

        // Iterate over the index names and get the mappings
        for (Iterator<String> it = rootNode.fieldNames(); it.hasNext(); ) {
            String indexName = it.next();
            if (indexName.startsWith(ENTITY_AUDITS_INDEX)) {
                indexMappings.put(indexName, rootNode.get(indexName).get("mappings"));
            }
        }
        return indexMappings;
    }

    private boolean areConfigurationsSame(JsonNode activeIndexInformation, JsonNode indexInformationFromConfigurationFile) {
        return indexInformationFromConfigurationFile.get("mappings").equals(activeIndexInformation);
    }

    private Response updateMappings(JsonNode indexInformationFromConfigurationFile) throws IOException {
        Request request = new Request("PUT", INDEX_NAME + "/_mapping");
        HttpEntity entity = new NStringEntity(indexInformationFromConfigurationFile.get("mappings").toString(), ContentType.APPLICATION_JSON);
        request.setEntity(entity);
        return lowLevelClient.performRequest(request);
    }

    private String getAuditIndexMappings() throws IOException {
        String atlasHomeDir = System.getProperty("atlas.home");
        String atlasHome = StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir;
        File elasticsearchSettingsFile = Paths.get(atlasHome, "elasticsearch", "es-audit-mappings.json").toFile();
        return new String(Files.readAllBytes(elasticsearchSettingsFile.toPath()), StandardCharsets.UTF_8);
    }

    @Override
    public void stop() throws AtlasException {
        try {
            LOG.info("ESBasedAuditRepo - stop!");
            if (lowLevelClient != null) {
                lowLevelClient.close();
                lowLevelClient = null;
            }
        } catch (IOException e) {
            LOG.error("ESBasedAuditRepo - error while closing es lowlevel client", e);
            throw new AtlasException(e);
        }
    }

    private void setLowLevelClient() throws AtlasException {
        if (lowLevelClient == null) {
            try {
                LOG.info("ESBasedAuditRepo - setLowLevelClient!");
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
            } else {
                throw new AtlasException("Invalid config");
            }
        }
        return httpHosts;
    }

    public static String getESHosts() throws AtlasException {
        Configuration configuration = ApplicationProperties.get();
        //get es write hosts if available (ES Isolation)
        String esHostNames = configuration.getString(INDEX_WRITE_BACKEND_CONF);
        if (StringUtils.isNotEmpty(esHostNames)) {
            return esHostNames;
        } else {
            return configuration.getString(INDEX_BACKEND_CONF);
        }
    }

    private boolean isSuccess(Response response) {
        return response.getStatusLine().getStatusCode() == 200;
    }
}



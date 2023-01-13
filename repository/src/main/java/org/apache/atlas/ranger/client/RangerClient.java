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
package org.apache.atlas.ranger.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.multipart.impl.MultiPartWriter;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasBaseClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.ranger.RangerPolicyList;
import org.apache.atlas.ranger.RangerRoleList;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static javax.ws.rs.HttpMethod.DELETE;
import static javax.ws.rs.HttpMethod.GET;
import static javax.ws.rs.HttpMethod.POST;
import static javax.ws.rs.HttpMethod.PUT;
import static org.apache.atlas.accesscontrol.AccessControlUtil.RESOURCE_PREFIX;

@Component
public class RangerClient {
    private static final Logger LOG = LoggerFactory.getLogger(RangerClient.class);

    private static final String RANGER_CLIENT_RETRIES_KEY = "ranger.client.retries";
    private static final String RANGER_CLIENT_SLEEP_INTERVAL_MS_KEY = "ranger.client.sleep.interval.ms";

    private static final String PROP_RANGER_BASE_URL = "atlas.ranger.base.url";
    private static final String PROP_RANGER_USERNAME = "atlas.ranger.username";
    private static final String PROP_RANGER_PASSWORD = "atlas.ranger.password";

    private static final String BASE_URI_DEFAULT = "http://localhost:8080/api/policy/";

    public static final  String POLICY_GET_BY_NAME = "public/v2/api/service/%s/policy/%s";
    public static final  String POLICY_GET_BY_ID = "service/plugins/policies/%s";
    public static final  String POLICY_DELETE_BY_ID = "service/plugins/policies/%s";
    public static final  String GET_USER_BY_NAME = "service/xusers/users";
    public static final  String CREATE_ROLE = "service/roles/roles";
    public static final  String GET_ROLE_BY_LOOKUP = "service/roles/lookup/roles";
    public static final  String UPDATE_ROLE = "service/roles/roles/%s";
    public static final  String DELETE_ROLE = "service/roles/roles/%s";
    public static final  String CREATE_POLICY = "service/public/v2/api/policy";
    public static final  String UPDATE_POLICY = "service/public/v2/api/policy/%s";
    public static final  String SEARCH_BY_RESOURCES = "service/plugins/policies";
    public static final  String SEARCH_BY_LABELS = "service/plugins/policies";

    protected static WebResource service;

    private static List<Integer> skipRetryCode = Arrays.asList(ClientResponse.Status.SERVICE_UNAVAILABLE.getStatusCode(),
            ClientResponse.Status.BAD_REQUEST.getStatusCode());

    public RangerClient() {
        try {
            if (service == null) {
                LOG.info("Initializing Ranger client");
                String BASE_URL = ApplicationProperties.get().getString(PROP_RANGER_BASE_URL, BASE_URI_DEFAULT);

                String basicAuthUser = ApplicationProperties.get().getString(PROP_RANGER_USERNAME, "admin");
                String basicAuthPassword = ApplicationProperties.get().getString(PROP_RANGER_PASSWORD);

                Configuration configuration = getClientProperties();
                Client client = getClient(configuration);

                if (StringUtils.isNotEmpty(basicAuthUser) && StringUtils.isNotEmpty(basicAuthPassword)) {
                    final HTTPBasicAuthFilter authFilter = new HTTPBasicAuthFilter(basicAuthUser, basicAuthPassword);
                    client.addFilter(authFilter);
                }

                service = client.resource(UriBuilder.fromUri(BASE_URL).build());
            }
        } catch (AtlasException e) {
            e.printStackTrace();
            LOG.error("Failed to initialize Ranger client: {}", e.getMessage());
        }
    }

    private static Client getClient(Configuration configuration) {
        DefaultClientConfig config = new DefaultClientConfig();
        // Enable POJO mapping feature
        config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        config.getClasses().add(JacksonJaxbJsonProvider.class);
        config.getClasses().add(MultiPartWriter.class);

        int readTimeout = configuration.getInt("atlas.ranger.client.readTimeoutMSecs", 60000);
        int connectTimeout = configuration.getInt("atlas.ranger.client.connectTimeoutMSecs", 60000);

        final URLConnectionClientHandler handler = new URLConnectionClientHandler();

        Client client = new Client(handler, config);
        client.setReadTimeout(readTimeout);
        client.setConnectTimeout(connectTimeout);
        return client;
    }

    protected static Configuration getClientProperties() throws AtlasException {
        return ApplicationProperties.get();
    }

    public RangerPolicy getPolicyById(String policyId) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(POLICY_GET_BY_ID, policyId), GET, Response.Status.OK);

        return callAPI(api, RangerPolicy.class, null);
    }


    public String getPolicyByServicePolicyName(String serviceName, String policyName) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(POLICY_GET_BY_NAME, serviceName, policyName), GET, Response.Status.OK);

        return callAPI(api, String.class, null);
    }

    public String getUserByUserName(String userName) throws AtlasServiceException {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("name", userName);

        MultivaluedMap<String, String> queryParams = toQueryParams(attrs, null);

        AtlasBaseClient.API api = new AtlasBaseClient.API(GET_USER_BY_NAME, GET, Response.Status.OK);

        return callAPI(api, String.class, queryParams);
    }

    public RangerRole createRole(RangerRole rangerRole) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(CREATE_ROLE, POST, Response.Status.OK);

        return callAPI(api, RangerRole.class, rangerRole);
    }

    public RangerRoleList getRole(String roleName) throws AtlasServiceException {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("roleNamePartial", roleName);

        MultivaluedMap<String, String> queryParams = toQueryParams(attrs, null);

        AtlasBaseClient.API api = new AtlasBaseClient.API(GET_ROLE_BY_LOOKUP, GET, Response.Status.OK);

        return callAPI(api, RangerRoleList.class, queryParams);
    }

    public RangerRoleList getRole(long roleId) throws AtlasServiceException {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("roleId", String.valueOf(roleId));

        MultivaluedMap<String, String> queryParams = toQueryParams(attrs, null);

        AtlasBaseClient.API api = new AtlasBaseClient.API(GET_ROLE_BY_LOOKUP, GET, Response.Status.OK);

        return callAPI(api, RangerRoleList.class, queryParams);
    }

    public RangerRole updateRole(RangerRole rangerRole) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(UPDATE_ROLE, rangerRole.getId()), PUT, Response.Status.OK);

        return callAPI(api, RangerRole.class, rangerRole);
    }

    public void deleteRole(long roleId) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(DELETE_ROLE, roleId), DELETE, Response.Status.NO_CONTENT);

        callAPI(api, (Class<?>)null, null);
    }

    public RangerPolicy createPolicy(RangerPolicy rangerPolicy) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(CREATE_POLICY, POST, Response.Status.OK);

        return callAPI(api, RangerPolicy.class, rangerPolicy);
    }

    public RangerPolicy updatePolicy(RangerPolicy rangerPolicy) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(UPDATE_POLICY, rangerPolicy.getId()), PUT, Response.Status.OK);

        return callAPI(api, RangerPolicy.class, rangerPolicy);
    }

    public RangerPolicyList searchPoliciesByResources(Map<String, String> resources, Map<String, String> attributes) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = resourcesToQueryParams(resources);
        queryParams = toQueryParams(attributes, queryParams);

        AtlasBaseClient.API api = new AtlasBaseClient.API(SEARCH_BY_RESOURCES, GET, Response.Status.OK);

        return callAPI(api, RangerPolicyList.class, queryParams);
    }

    public RangerPolicyList getPoliciesByLabel(Map<String, String> attributes) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = toQueryParams(attributes, null);

        AtlasBaseClient.API api = new AtlasBaseClient.API(SEARCH_BY_LABELS, GET, Response.Status.OK);

        return callAPI(api, RangerPolicyList.class, queryParams);
    }

    public void deletePolicyById(Long policyId) throws AtlasServiceException {
        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(POLICY_DELETE_BY_ID, policyId), DELETE, Response.Status.NO_CONTENT);

        callAPI(api, (Class<?>)null, null);
    }

    private MultivaluedMap<String, String> resourcesToQueryParams(Map<String, String> attributes) {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();

        if (MapUtils.isNotEmpty(attributes)) {
            for (Map.Entry<String, String> e : attributes.entrySet()) {
                queryParams.putSingle(RESOURCE_PREFIX + e.getKey(), e.getValue());
            }
        }

        return queryParams;
    }

    private MultivaluedMap<String, String> toQueryParams(Map<String, String> attributes,
                                                         MultivaluedMap<String, String> queryParams) {
        if (queryParams == null) {
            queryParams = new MultivaluedMapImpl();
        }

        if (MapUtils.isNotEmpty(attributes)) {
            for (Map.Entry<String, String> e : attributes.entrySet()) {
                queryParams.putSingle(e.getKey(), e.getValue());
            }
        }

        return queryParams;
    }

    public static <T> T callAPI(AtlasBaseClient.API api, Class<T> responseType, MultivaluedMap<String, String> queryParams)
            throws AtlasServiceException {
        return callAPIWithResource(api, getResource(api, queryParams), null, responseType);
    }

    public static <T> T callAPI(AtlasBaseClient.API api, Class<T> responseType, Object requestObject, String... params)
            throws AtlasServiceException {
        return callAPIWithResource(api, getResource(api, params), requestObject, responseType);
    }

    protected static <T> T callAPIWithResource(AtlasBaseClient.API api, WebResource resource, Object requestObject, Class<T> responseType) throws AtlasServiceException {
        GenericType<T> genericType = null;
        if (responseType != null) {
            genericType = new GenericType<>(responseType);
        }
        return callAPIWithResource(api, resource, requestObject, genericType);
    }

    protected static <T> T callAPIWithResource(AtlasBaseClient.API api, WebResource resource, Object requestObject, GenericType<T> responseType) throws AtlasServiceException {
        ClientResponse clientResponse = null;
        int i = 0;
        do {
            if (LOG.isDebugEnabled()) {
                LOG.debug("------------------------------------------------------");
                LOG.debug("Call         : {} {}", api.getMethod(), api.getNormalizedPath());
                LOG.debug("Content-type : {} ", api.getConsumes());
                LOG.debug("Accept       : {} ", api.getProduces());
                if (requestObject != null) {
                    LOG.debug("Request      : {}", requestObject);
                }
            }

            WebResource.Builder requestBuilder = resource.getRequestBuilder();

            // Set content headers
            requestBuilder
                    .accept(api.getProduces())
                    .type(api.getConsumes())
                    .header("Expect", "100-continue");

            clientResponse = requestBuilder.method(api.getMethod(), ClientResponse.class, requestObject);

            LOG.debug("HTTP Status  : {}", clientResponse.getStatus());

            if (!LOG.isDebugEnabled()) {
                LOG.info("method={} path={} contentType={} accept={} status={}", api.getMethod(),
                        api.getNormalizedPath(), api.getConsumes(), api.getProduces(), clientResponse.getStatus());
            }

            if (clientResponse.getStatus() == api.getExpectedStatus().getStatusCode()) {
                if (responseType == null) {
                    return null;
                }
                try {
                    if(api.getProduces().equals(MediaType.APPLICATION_OCTET_STREAM)) {
                        return (T) clientResponse.getEntityInputStream();
                    } else if (responseType.getRawClass().equals(ObjectNode.class)) {
                        String stringEntity = clientResponse.getEntity(String.class);
                        try {
                            JsonNode jsonObject = AtlasJson.parseToV1JsonNode(stringEntity);
                            LOG.debug("Response     : {}", jsonObject);
                            LOG.debug("------------------------------------------------------");
                            return (T) jsonObject;
                        } catch (IOException e) {
                            throw new AtlasServiceException(api, e);
                        }
                    } else {
                        T entity = clientResponse.getEntity(responseType);
                        LOG.debug("Response     : {}", entity);
                        LOG.debug("------------------------------------------------------");
                        return entity;
                    }
                } catch (ClientHandlerException e) {
                    throw new AtlasServiceException(api, e);
                }
            } else if (!skipRetryCode.contains(clientResponse.getStatus())) {
                break;
            } else {
                LOG.error("Got a service unavailable when calling: {}, will retry..", resource);
                sleepBetweenRetries();
            }

            i++;
        } while (i < getNumberOfRetries());

        throw new AtlasServiceException(api, clientResponse);
    }


    protected static WebResource getResource(AtlasBaseClient.API api, String... pathParams) {
        return getResource(service, api, pathParams);
    }

    // Modify URL to include the query params
    private static WebResource getResource(AtlasBaseClient.API api, MultivaluedMap<String, String> queryParams) {
        WebResource resource = service.path(api.getNormalizedPath());
        resource = appendQueryParams(queryParams, resource);
        return resource;
    }

    // Modify URL to include the path params
    private static WebResource getResource(WebResource service, AtlasBaseClient.API api, String... pathParams) {
        WebResource resource = service.path(api.getNormalizedPath());
        resource = appendPathParams(resource, pathParams);
        return resource;
    }

    private static WebResource appendQueryParams(MultivaluedMap<String, String> queryParams, WebResource resource) {
        if (null != queryParams && !queryParams.isEmpty()) {
            for (Map.Entry<String, List<String>> entry : queryParams.entrySet()) {
                for (String value : entry.getValue()) {
                    if (StringUtils.isNotBlank(value)) {
                        resource = resource.queryParam(entry.getKey(), value);
                    }
                }
            }
        }
        return resource;
    }

    private static WebResource appendPathParams(WebResource resource, String[] pathParams) {
        if (pathParams != null) {
            for (String pathParam : pathParams) {
                resource = resource.path(pathParam);
            }
        }
        return resource;
    }

    private static int getNumberOfRetries() {
        try {
            return ApplicationProperties.get().getInt(RANGER_CLIENT_RETRIES_KEY, 3);
        } catch (AtlasException e) {
            LOG.error("Failed to get retry count from configuration, defaulting to 3: {}", e.getMessage());
            return 3;
        }
    }

    private static int getSleepBetweenRetriesMs() throws AtlasException {
        return ApplicationProperties.get().getInt(RANGER_CLIENT_SLEEP_INTERVAL_MS_KEY, 5000);
    }

    static void sleepBetweenRetries() {
        try {
            Thread.sleep(getSleepBetweenRetriesMs());
        } catch (Exception e) {
            LOG.error("Failed to get sleep time from configuration defaulting to 5000 ms: {}", e.getMessage());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                LOG.error("Failed to sleep between retries: {}", e.getMessage());
            }
        }
    }
}
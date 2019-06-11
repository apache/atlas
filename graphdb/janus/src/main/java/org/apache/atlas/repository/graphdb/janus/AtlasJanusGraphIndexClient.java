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
package org.apache.atlas.repository.graphdb.janus;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.commons.configuration.Configuration;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.util.NamedList;
import org.janusgraph.diskstorage.solr.Solr6Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.atlas.repository.Constants.FREETEXT_REQUEST_HANDLER;

public class AtlasJanusGraphIndexClient implements AtlasGraphIndexClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusGraphIndexClient.class);

    private final AtlasGraph    graph;
    private final Configuration configuration;


    public AtlasJanusGraphIndexClient(AtlasGraph graph, Configuration configuration) {
        this.graph         = graph;
        this.configuration = configuration;
    }

    @Override
    public void applySearchWeight(String collectionName, Map<String, Integer> attributeName2SearchWeightMap) {
        SolrClient solrClient = null;

        try {
            solrClient = Solr6Index.getSolrClient(); // get solr client using same settings as that of Janus Graph

            if (solrClient == null) {
                LOG.warn("AtlasJanusGraphIndexClient.applySearchWeight(): Non SOLR index stores are not supported yet.");

                return;
            }

            //1) try updating request handler
            //2) if update fails, try creating request handler

            int       maxAttempts         = configuration != null ? configuration.getInt("index.client.apply.search.weight.max.attempts", 3) : 3;
            int       retryWaitIntervalMs = configuration != null ? configuration.getInt("index.client.apply.search.weight.retry.interval.ms", 1000) : 1000;
            Throwable lastExcp            = null;

            for (int i = 0; i < maxAttempts; i++) {
                if (i > 0) {
                    LOG.warn("Attempt #{} failed! Waiting for {}ms before retry", i, retryWaitIntervalMs);

                    try {
                        Thread.sleep(retryWaitIntervalMs);
                    } catch (Exception excp) {
                        // ignore
                    }
                }

                try {
                    LOG.info("Attempting to update free text request handler {} for collection {}", FREETEXT_REQUEST_HANDLER, collectionName);

                    updateFreeTextRequestHandler(solrClient, collectionName, attributeName2SearchWeightMap);
                    LOG.info("Successfully updated free text request handler {} for collection {}..", FREETEXT_REQUEST_HANDLER, collectionName);

                    return;
                } catch (Throwable t) {
                    lastExcp = t;

                    LOG.warn("Error encountered in updating request handler {} for collection {}. Will attempt to create one", FREETEXT_REQUEST_HANDLER, collectionName);
                }

                try {
                    LOG.info("Attempting to create free text request handler {} for collection {}", FREETEXT_REQUEST_HANDLER, collectionName);

                    createFreeTextRequestHandler(solrClient, collectionName, attributeName2SearchWeightMap);
                    LOG.info("Successfully created free text request handler {} for collection {}", FREETEXT_REQUEST_HANDLER, collectionName);

                    return;
                } catch (Throwable t) {
                    lastExcp = t;

                    LOG.warn("Error encountered in creating request handler {} for collection {}", FREETEXT_REQUEST_HANDLER, collectionName, t);
                }
            }

            String msg = String.format("Error encountered in creating/updating request handler %s for collection %s", FREETEXT_REQUEST_HANDLER, collectionName);

            throw lastExcp != null ? new RuntimeException(msg, lastExcp) : new RuntimeException(msg);
        } finally {
            LOG.debug("Releasing the solr client from usage.");
            Solr6Index.releaseSolrClient(solrClient);
        }
    }

    private V2Response validateResponseForSuccess(V2Response v2Response) throws AtlasBaseException {
        if(v2Response == null) {
            String msg = "Received in valid response .";
            LOG.error(msg);
            throw new AtlasBaseException(msg);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("V2 Response is {}", v2Response.toString());
        }
        NamedList<Object> response = v2Response.getResponse();
        Object errorMessages = response.get("errorMessages");
        if(errorMessages != null) {
            LOG.error("Error encountered in performing response handler action.");
            List<Object> errorObjects = (List<Object>) errorMessages;
            Map<Object, Object> errObject = (Map<Object, Object>) errorObjects.get(0);
            List<String> msgs = (List<String>) errObject.get("errorMessages");
            StringBuilder sb = new StringBuilder();
            for(String msg: msgs) {
                sb.append(msg);
            }
            String errors = sb.toString();
            String msg = String.format("Error encountered in performing response handler action. %s.", errors);
            LOG.error(msg);
            throw new AtlasBaseException(msg);
        } else {
            LOG.debug("Successfully performed response handler action. V2 Response is {}", v2Response.toString());
        }
        return v2Response;
    }

    private V2Response updateFreeTextRequestHandler(SolrClient solrClient, String collectionName, Map<String, Integer> attributeName2SearchWeightMap) throws IOException, SolrServerException, AtlasBaseException {
        String searchWeightString = generateSearchWeightString(graph.getManagementSystem(), collectionName, attributeName2SearchWeightMap);
        String payLoadString      = generatePayLoadForFreeText("update-requesthandler", FREETEXT_REQUEST_HANDLER, searchWeightString);

        return performRequestHandlerAction(collectionName, solrClient, payLoadString);
    }

    private V2Response createFreeTextRequestHandler(SolrClient solrClient, String collectionName, Map<String, Integer> attributeName2SearchWeightMap) throws IOException, SolrServerException, AtlasBaseException {
        String searchWeightString = generateSearchWeightString(graph.getManagementSystem(), collectionName, attributeName2SearchWeightMap);
        String payLoadString      = generatePayLoadForFreeText("create-requesthandler", FREETEXT_REQUEST_HANDLER, searchWeightString);

        return performRequestHandlerAction(collectionName, solrClient, payLoadString);
    }

    private String generateSearchWeightString(AtlasGraphManagement management, String indexName, Map<String, Integer> searchWeightsMap) {
        StringBuilder                   searchWeightBuilder = new StringBuilder();
        Set<Map.Entry<String, Integer>> searchWeightFields  = searchWeightsMap.entrySet();

        for (Map.Entry<String, Integer> entry : searchWeightFields) {
            AtlasPropertyKey propertyKey    = management.getPropertyKey(entry.getKey());
            String           indexFieldName = management.getIndexFieldName(indexName, propertyKey);

            searchWeightBuilder.append(" ")
                    .append(indexFieldName)
                    .append("^")
                    .append(entry.getValue().intValue());

        }

        return searchWeightBuilder.toString();
    }

    @VisibleForTesting
    static String generatePayLoadForFreeText(String action, String handlerName, String qfValue) {
        return String.format("{" +
                " %s :  { " +
                "       'name' : '%s', " +
                "       'class': 'solr.SearchHandler' , " +
                "       'defaults': " + "{" +
                "          'defType': 'edismax' , " +
                "          'rows':    100 , " +
                "          'lowercaseOperators': true , " +
                "          'qf': '%s' , " +
                "          'hl.fl': '*' , " +
                "          'hl.requireFieldMatch': true , " +
                "          'lowercaseOperators': true , " +
                "         }" +
                "    }" +
                "}", action, handlerName, qfValue);
    }

    private V2Response performRequestHandlerAction(String collectionName, SolrClient solrClient,
                                             String actionPayLoad) throws IOException, SolrServerException, AtlasBaseException {
        V2Request v2Request = new V2Request.Builder(String.format("/collections/%s/config", collectionName))
                .withMethod(SolrRequest.METHOD.POST)
                .withPayload(actionPayLoad)
                .build();
        return validateResponseForSuccess(v2Request.process(solrClient));
    }
}

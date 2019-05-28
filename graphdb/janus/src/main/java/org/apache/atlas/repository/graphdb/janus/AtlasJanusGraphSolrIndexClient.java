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
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.V2Request;
import org.janusgraph.diskstorage.solr.Solr6Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.atlas.repository.Constants.FREETEXT_REQUEST_HANDLER;

public class AtlasJanusGraphSolrIndexClient implements AtlasGraphIndexClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusGraphSolrIndexClient.class);

    private final SolrClient solrClient;
    private final AtlasGraph graph;


    public AtlasJanusGraphSolrIndexClient(AtlasGraph graph) {
        // get solr client using same settings as that of Janus Graph
        this.solrClient = Solr6Index.getSolrClient();
        this.graph      = graph;

        if(solrClient == null) {
            LOG.warn("Non SOLR index stores are not supported yet.");
        }
    }

    @Override
    public void applySearchWeight(String collectionName, Map<String, Integer> attributeName2SearchWeightMap) {
        //1) try updating request handler
        //2) if update fails, try creating request handler

        try {
            LOG.info("Attempting to update free text request handler {} for collection {}", FREETEXT_REQUEST_HANDLER, collectionName);

            updateSearchWeights(collectionName, attributeName2SearchWeightMap);

            LOG.info("Successfully updated free text request handler {} for collection {}..", FREETEXT_REQUEST_HANDLER, collectionName);

            return;
        } catch (Throwable t) {
            LOG.warn("Error encountered in updating request handler {} for collection {}. Attempting to create one", FREETEXT_REQUEST_HANDLER, collectionName, t);
        }

        try {
            LOG.info("Attempting to create free text request handler {} for collection {}", FREETEXT_REQUEST_HANDLER, collectionName);

            createFreeTextRequestHandler(collectionName, attributeName2SearchWeightMap);

            LOG.info("Successfully created free text request handler {} for collection {}", FREETEXT_REQUEST_HANDLER, collectionName);
        } catch (Throwable t) {
            String msg = String.format("Error encountered in creating the request handler '%s' for collection '%s'.", FREETEXT_REQUEST_HANDLER, collectionName);

            LOG.error(msg, t);

            throw new RuntimeException(msg, t);
        }
    }

    private void updateSearchWeights(String collectionName, Map<String, Integer> attributeName2SearchWeightMap) {
        try {
            updateFreeTextRequestHandler(collectionName, attributeName2SearchWeightMap);
        } catch (Throwable t) {
            String msg = String.format("Error encountered in updating the request handler '%s' for collection '%s'", FREETEXT_REQUEST_HANDLER, collectionName);

            LOG.error(msg, t);

            throw new RuntimeException(msg, t);
        }

        LOG.info("Updated free text request handler for collection {}.", collectionName);
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

    private void updateFreeTextRequestHandler(String collectionName, Map<String, Integer> attributeName2SearchWeightMap) throws IOException, SolrServerException {
        String searchWeightString = generateSearchWeightString(graph.getManagementSystem(), collectionName, attributeName2SearchWeightMap);
        String payLoadString      = generatePayLoadForFreeText("update-requesthandler", FREETEXT_REQUEST_HANDLER, searchWeightString);

        performRequestHandlerAction(collectionName, solrClient, payLoadString);
    }

    private void createFreeTextRequestHandler(String collectionName, Map<String, Integer> attributeName2SearchWeightMap) throws IOException, SolrServerException {
        String searchWeightString = generateSearchWeightString(graph.getManagementSystem(), collectionName, attributeName2SearchWeightMap);
        String payLoadString      = generatePayLoadForFreeText("create-requesthandler", FREETEXT_REQUEST_HANDLER, searchWeightString);

        performRequestHandlerAction(collectionName, solrClient, payLoadString);
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

    private void performRequestHandlerAction(String collectionName, SolrClient solrClient,
                                             String actionPayLoad) throws IOException, SolrServerException {
        V2Request v2Request = new V2Request.Builder(String.format("/collections/%s/config", collectionName))
                .withMethod(SolrRequest.METHOD.POST)
                .withPayload(actionPayLoad)
                .build();
        v2Request.process(solrClient);
    }
}

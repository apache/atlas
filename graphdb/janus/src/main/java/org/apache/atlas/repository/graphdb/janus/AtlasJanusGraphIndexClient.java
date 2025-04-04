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
import org.apache.atlas.model.discovery.AtlasAggregationEntry;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AggregationContext;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.util.NamedList;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static org.apache.atlas.repository.Constants.*;

public class AtlasJanusGraphIndexClient implements AtlasGraphIndexClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusGraphIndexClient.class);

    private static final FreqComparator FREQ_COMPARATOR              = new FreqComparator();
    private static final int            DEFAULT_SUGGESTION_COUNT     = 5;
    private static final int            MIN_FACET_COUNT_REQUIRED     = 1;
    private static final String         TERMS_PREFIX                 = "terms.prefix";
    private static final String         TERMS_FIELD                  = "terms.fl";
    private static final int            SOLR_HEALTHY_STATUS          = 0;
    private static final long           SOLR_STATUS_LOG_FREQUENCY_MS = 60000;//Prints SOLR DOWN status for every 1 min
    private static long                 prevSolrHealthCheckTime;


    private final Configuration configuration;


    public AtlasJanusGraphIndexClient(Configuration configuration) {
        this.configuration = configuration;
    }

    public boolean isHealthy() {
        boolean isHealthy   = false;
        long    currentTime = System.currentTimeMillis();

        try {
            boolean isElasticsearchBackend = configuration.getProperty("atlas.graph.index.search.backend").equals("elasticsearch");
            isHealthy = isElasticsearchBackend ? isElasticsearchHealthy() : isSolrHealthy();
        } catch (Exception exception) {
            if (LOG.isDebugEnabled()) {
                LOG.error("Error: isHealthy", exception);
            }
        }

        if (!isHealthy && (prevSolrHealthCheckTime == 0 || currentTime - prevSolrHealthCheckTime > SOLR_STATUS_LOG_FREQUENCY_MS)) {
            LOG.info("Index Health: Unhealthy!");

            prevSolrHealthCheckTime = currentTime;
        }

        return isHealthy;
    }

    @Override
    public void applySearchWeight(String collectionName, Map<String, Integer> indexFieldName2SearchWeightMap) {

    }


    @Override
    public Map<String, List<AtlasAggregationEntry>> getAggregatedMetrics(AggregationContext aggregationContext) {
        return Collections.EMPTY_MAP;
    }

    @Override
    public void applySuggestionFields(String collectionName, List<String> suggestionProperties) {
        LOG.info("Applied suggestion fields request handler for collection {}.", collectionName);
    }

    @Override
    public List<String> getSuggestions(String prefixString, String indexFieldName) {
        return Collections.EMPTY_LIST;
    }

    private boolean isSolrHealthy() throws SolrServerException, IOException {
        return true;
    }

    private boolean isElasticsearchHealthy() throws ElasticsearchException, IOException {
        RestHighLevelClient client = AtlasElasticsearchDatabase.getClient();
        ClusterHealthRequest request = new ClusterHealthRequest(Constants.INDEX_PREFIX + Constants.VERTEX_INDEX);
        ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        RestStatus restStatus = response.status();
        if (restStatus.toString().equals(ELASTICSEARCH_REST_STATUS_OK)){
            ClusterHealthStatus status = response.getStatus();
            if (status.toString().equals(ELASTICSEARCH_CLUSTER_STATUS_GREEN) || status.toString().equals(ELASTICSEARCH_CLUSTER_STATUS_YELLOW)) {
                return true;
            }
        } else {
            LOG.error("isElasticsearchHealthy => ES health check request timed out!");
        }
        return false;
    }

    private void graphManagementCommit(AtlasGraphManagement management) {
        try {
            management.commit();
        } catch (Exception ex) {
            LOG.warn("Graph transaction management commit failed; attempting rollback: {}", ex);

            graphManagementRollback(management);
        }
    }

    private void graphManagementRollback(AtlasGraphManagement management) {
        try {
            management.rollback();
        } catch (Exception ex) {
            LOG.warn("Graph transaction management rollback failed: {}", ex);
        }
    }

    @VisibleForTesting
    static List<String> getTopTerms(Map<String, TermFreq> termsMap) {
        final List<String> ret;

        if (MapUtils.isNotEmpty(termsMap)) {
            // Collect top high frequency terms.
            PriorityQueue<TermFreq> termsQueue = new PriorityQueue(termsMap.size(), FREQ_COMPARATOR);

            for (TermFreq term : termsMap.values()) {
                termsQueue.add(term);
            }

            ret = new ArrayList<>(DEFAULT_SUGGESTION_COUNT);

            while (!termsQueue.isEmpty()) {
                ret.add(termsQueue.poll().getTerm());

                if (ret.size() >= DEFAULT_SUGGESTION_COUNT) {
                    break;
                }
            }
        } else {
            ret = Collections.EMPTY_LIST;
        }

        return ret;
    }

    @VisibleForTesting
    static String generatePayLoadForFreeText(String action, String qfValue) {
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
                "}", action, FREETEXT_REQUEST_HANDLER, qfValue);
    }

    @VisibleForTesting
    String generatePayLoadForSuggestions(String suggestionFieldsString) {
        return String.format("{\n" +
                " update-requesthandler :  { \n" +
                "       'name' :    '%s', \n" +
                "       'class':    'solr.SearchHandler' , \n" +
                "       'startup':  'lazy' ,\n" +
                "       'defaults': " + "{ \n" +
                "          'terms':       true , \n" +
                "          'distrib':     false , \n" +
                "          'terms.limit': 5 , \n" +
                "           'terms.fl'  : \n" +
                "              [\n" +
                "              %s \n" +
                "           ] \n" +
                "         }\n" +
                "       'components': " + "[ \n" +
                "           'terms' \n" +
                "        ] \n" +
                "    } \n" +
                "}", Constants.TERMS_REQUEST_HANDLER, suggestionFieldsString);

    }

    @VisibleForTesting
    protected static String generateSearchWeightString(Map<String, Integer> indexFieldName2SearchWeightMap) {
        StringBuilder searchWeightBuilder = new StringBuilder();

        for (Map.Entry<String, Integer> entry : indexFieldName2SearchWeightMap.entrySet()) {
            searchWeightBuilder.append(" ")
                               .append(entry.getKey())
                               .append("^")
                               .append(entry.getValue().intValue());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("generateSearchWeightString(fieldsCount={}): ret={}", indexFieldName2SearchWeightMap.size(), searchWeightBuilder.toString());
        }

        return searchWeightBuilder.toString();
    }

    @VisibleForTesting
    protected static String generateSuggestionsString(List<String> suggestionIndexFieldNames) {
        StringBuilder    ret      = new StringBuilder();
        Iterator<String> iterator = suggestionIndexFieldNames.iterator();

        while(iterator.hasNext()) {
            ret.append("'").append(iterator.next()).append("'");

            if(iterator.hasNext()) {
                ret.append(", ");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("generateSuggestionsString(fieldsCount={}): ret={}", suggestionIndexFieldNames.size(), ret.toString());
        }

        return ret.toString();
    }

    private SolrResponse validateResponseForSuccess(SolrResponse solrResponse) throws AtlasBaseException {
        if(solrResponse == null) {
            String msg = "Received null response .";

            LOG.error(msg);

            throw new AtlasBaseException(msg);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("V2 Response is {}", solrResponse.toString());
        }

        NamedList<Object> response = solrResponse.getResponse();

        if(response != null) {
            Object errorMessages = response.get("errorMessages");

            if(errorMessages != null) {
                LOG.error("Error encountered in performing request handler create/update");

                List<Object>        errorObjects = (List<Object>) errorMessages;
                Map<Object, Object> errObject    = (Map<Object, Object>) errorObjects.get(0);
                List<String>        msgs         = (List<String>) errObject.get("errorMessages");
                StringBuilder       sb           = new StringBuilder();

                for(String msg: msgs) {
                    sb.append(msg);
                }

                String errors = sb.toString();
                String msg    = String.format("Error encountered in performing response handler action. %s.", errors);

                LOG.error(msg);

                throw new AtlasBaseException(msg);
            } else {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Successfully performed response handler action. V2 Response is {}", solrResponse.toString());
                }
            }

        } else {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Did not receive any response from SOLR.");
            }
        }

        return solrResponse;
    }

        static final class TermFreq {
        private final String term;
        private       long   freq;

        public TermFreq(String term, long freq) {
            this.term = term;
            this.freq = freq;
        }

        public final String getTerm() { return term; }

        public final long getFreq() { return freq; }

        public final void addFreq(long val) { freq += val; }
   }

    static class FreqComparator implements Comparator<TermFreq> {
        @Override
        public int compare(TermFreq lhs, TermFreq rhs) {
            return Long.compare(rhs.getFreq(), lhs.getFreq());
        }
    }
}

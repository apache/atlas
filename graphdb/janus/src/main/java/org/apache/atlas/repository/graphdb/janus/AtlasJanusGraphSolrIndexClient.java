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

import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.janusgraph.diskstorage.solr.Solr6Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class AtlasJanusGraphSolrIndexClient implements AtlasGraphIndexClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusGraphSolrIndexClient.class);
    private final SolrClient solrClient;

    public AtlasJanusGraphSolrIndexClient() throws Exception {
        //well, this is temp hack to get solr client using same settings as that of Janus Graph
        solrClient = Solr6Index.getSolrClient();
        if(solrClient == null) {
            LOG.warn("The indexing system is not solr based. Non SOLR based indexing systems are not supported yet.");
        }
    }

    @Override
    public void createCopyField(String collectionName, String srcFieldName, String mappedCopyFieldName) {
        if(solrClient == null) {
            LOG.error("The indexing system is not solr based. Copy fields can not be created in non SOLR based indexing systems. This request will be treated as no op.");
            return;
        }
        SchemaRequest.AddCopyField addCopyFieldRequest =
                new SchemaRequest.AddCopyField(srcFieldName, Arrays.asList(mappedCopyFieldName));
        SchemaResponse.UpdateResponse addCopyFieldResponse = null;
        try {
            addCopyFieldResponse = addCopyFieldRequest.process(solrClient, collectionName);
        } catch (SolrServerException | IOException e) {
            String msg = String.format("Error encountered in creating the copy field from %s to %s for collection %s.", srcFieldName, mappedCopyFieldName, collectionName);
            LOG.error(msg);
            throw new RuntimeException(msg, e);
        }
    }
}

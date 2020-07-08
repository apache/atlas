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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.janusgraph.util.encoding.LongEncoding;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

public class AtlasElasticsearchIndexQuery implements AtlasIndexQuery<AtlasJanusVertex, AtlasJanusEdge> {
    private AtlasJanusGraph graph;
    private RestHighLevelClient esClient;
    private String index;
    private SearchSourceBuilder sourceBuilder;
    private SearchResponse searchResponse;

    public AtlasElasticsearchIndexQuery(AtlasJanusGraph graph, RestHighLevelClient esClient, String index, SearchSourceBuilder sourceBuilder) {
        this.esClient = esClient;
        this.sourceBuilder = sourceBuilder;
        this.index = index;
        this.graph = graph;
        searchResponse = null;
    }

    private SearchRequest getSearchRequest(String index, SearchSourceBuilder sourceBuilder) {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    private Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> runQuery(SearchRequest searchRequest) {
        Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> result = null;
        try {
            searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Stream<Result<AtlasJanusVertex, AtlasJanusEdge>> resultStream = Arrays.stream(searchResponse.getHits().getHits())
                    .map(ResultImpl::new);
            result = resultStream.iterator();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> vertices() {
        SearchRequest searchRequest = getSearchRequest(index, sourceBuilder);
        return runQuery(searchRequest);
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> vertices(int offset, int limit, String sortBy, Order sortOrder) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> vertices(int offset, int limit) {
        sourceBuilder.from(offset);
        sourceBuilder.size(limit);
        SearchRequest searchRequest = getSearchRequest(index, sourceBuilder);
        return runQuery(searchRequest);
    }

    @Override
    public Long vertexTotals() {
        return searchResponse.getHits().getTotalHits().value;
    }

    public final class ResultImpl implements AtlasIndexQuery.Result<AtlasJanusVertex, AtlasJanusEdge> {
        private SearchHit hit;

        public ResultImpl(SearchHit hit) {
            this.hit = hit;
        }

        @Override
        public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> getVertex() {
            long vertexId = LongEncoding.decode(hit.getId());
            return graph.getVertex(String.valueOf(vertexId));
        }

        @Override
        public double getScore() {
            return hit.getScore();
        }
    }
}

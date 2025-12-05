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

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ESAliasRequestBuilder;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.idgenerator.DistributedIdGenerator;
import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQueryParameter;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.GraphIndexQueryParameters;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertexService;
import org.apache.atlas.repository.graphdb.janus.cassandra.ESConnector;
import org.apache.atlas.repository.graphdb.janus.query.AtlasJanusGraphQuery;
import org.apache.atlas.repository.graphdb.utils.IteratorToIterableAdapter;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.INDEX_SEARCH_CLIENT_NOT_INITIATED;
import static org.apache.atlas.AtlasErrorCode.RELATIONSHIP_CREATE_INVALID_PARAMS;
import static org.apache.atlas.ESAliasRequestBuilder.ESAliasAction.REMOVE;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getClient;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getLowLevelClient;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getUiClusterClient;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getNonUiClusterClient;
import static org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase.getGraphInstance;
import static org.apache.atlas.type.Constants.STATE_PROPERTY_KEY;

/**
 * Janus implementation of AtlasGraph.
 */
public class AtlasJanusGraph implements AtlasGraph<AtlasJanusVertex, AtlasJanusEdge> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusGraph.class);
    private static final Parameter[] EMPTY_PARAMETER_ARRAY  = new Parameter[0];


    private static       Configuration APPLICATION_PROPERTIES = null;

    private final ConvertGremlinValueFunction GREMLIN_VALUE_CONVERSION_FUNCTION = new ConvertGremlinValueFunction();
    private final Set<String>                 multiProperties                   = new HashSet<>();
    private final StandardJanusGraph          janusGraph;
    private final RestHighLevelClient         elasticsearchClient;
    private final RestClient                  restClient;

    private final RestClient esUiClusterClient;
    private final RestClient esNonUiClusterClient;

    private String CASSANDRA_HOSTNAME_PROPERTY = "atlas.graph.storage.hostname";
    private CqlSession cqlSession;
    private DynamicVertexService dynamicVertexService;

    private static DistributedIdGenerator CUSTOM_ID_GENERATOR;


    static {
        try {
            String hostName = ApplicationProperties.get().getString("atlas.graph.storage.hostname", "localhost");
            int port = ApplicationProperties.get().getInt("atlas.graph.storage.port", 9042);
            String podName = System.getenv("K8S_POD_NAME");

            if (podName == null || podName.isBlank()) {
                podName = "local-atlas-0";
                String message = "Pod name not found in env for DistributedIdGenerator for custom vertex ID generation, falling back to " + podName;
                LOG.warn(message);

                //LOG.error(message);
                //throw new RuntimeException(message);
            }

            if (LEAN_GRAPH_ENABLED) {
                CUSTOM_ID_GENERATOR = new DistributedIdGenerator(hostName, port, podName);
            }
        } catch (AtlasException e) {
            LOG.error("Failed to initialize DistributedIdGenerator for custom vertex ID generation");
            throw new RuntimeException(e);
        }
    }

    public DynamicVertexService getDynamicVertexRetrievalService() {
        return dynamicVertexService;
    }

    private final ThreadLocal<GremlinGroovyScriptEngine> scriptEngine = ThreadLocal.withInitial(() -> {
        DefaultImportCustomizer.Builder builder = DefaultImportCustomizer.build()
                                                                         .addClassImports(java.util.function.Function.class)
                                                                         .addMethodImports(__.class.getMethods())
                                                                         .addMethodImports(P.class.getMethods());
        return new GremlinGroovyScriptEngine(builder.create());
    });

    public AtlasJanusGraph() {
        this(getGraphInstance(), getClient(), getLowLevelClient(), getUiClusterClient(), getNonUiClusterClient());
    }

    public AtlasJanusGraph(JanusGraph graphInstance, RestHighLevelClient elasticsearchClient, RestClient restClient, RestClient esUiClusterClient, RestClient esNonUiClusterClient) {
        //determine multi-properties once at startup
        JanusGraphManagement mgmt = null;

        try {
            mgmt = graphInstance.openManagement();

            Iterable<PropertyKey> keys = mgmt.getRelationTypes(PropertyKey.class);

            for (PropertyKey key : keys) {
                if (key.cardinality() != Cardinality.SINGLE) {
                    multiProperties.add(key.name());
                }
            }
        } finally {
            if (mgmt != null) {
                mgmt.rollback();
            }
        }

        janusGraph = (StandardJanusGraph) graphInstance;
        this.restClient = restClient;
        this.elasticsearchClient = elasticsearchClient;
        this.esUiClusterClient = esUiClusterClient;
        this.esNonUiClusterClient = esNonUiClusterClient;

        if (LEAN_GRAPH_ENABLED) {
            initializeSchema();
            this.cqlSession = initializeCassandraSession();
            this.dynamicVertexService = new DynamicVertexService(cqlSession);
        }
    }

    private CqlSession initializeCassandraSession() {
        String hostname = null;
        try {
            hostname = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }

        String keyspace = AtlasConfiguration.ATLAS_CASSANDRA_VANILLA_KEYSPACE.getString();

        return getCQLBuilder(hostname)
                .withKeyspace(keyspace)
                .build();
    }

    private CqlSessionBuilder getCQLBuilder (String hostname) {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostname, 9042))
                .withConfigLoader(
                        DriverConfigLoader.programmaticBuilder()
                                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10))
                                .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(15))
                                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15))
                                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(20))
                                .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofMillis(500))
                                .withDuration(DefaultDriverOption.REQUEST_TRACE_ATTEMPTS, Duration.ofSeconds(20))
                                .build())
                .withLocalDatacenter("datacenter1");
    }

    private void initializeSchema() {
        String hostname = null;
        try {
            hostname = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }

        String keyspace = AtlasConfiguration.ATLAS_CASSANDRA_VANILLA_KEYSPACE.getString();
        String replFactor = AtlasConfiguration.CASSANDRA_REPLICATION_FACTOR_PROPERTY.getString();

        Map<String, String> replicationConfig =
                Map.of(
                        "class", "SimpleStrategy",
                        "replication_factor", replFactor);

        String replicationConfigString = replicationConfig.entrySet().stream()
                .map(entry -> String.format("'%s': '%s'", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(", "));

        String createKeyspaceQuery = String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {%s} AND durable_writes = true;",
                keyspace, replicationConfigString);

        executeWithRetry(hostname, SimpleStatement.builder(createKeyspaceQuery).setConsistencyLevel(DefaultConsistencyLevel.ALL).build());
        LOG.info("Ensured keyspace {} exists", keyspace);

        String createAssetsTable = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "id text, " +
                        "bucket int, " +
                        "json_data text, " +
                        "updated_at timestamp, " +
                        "PRIMARY KEY ((bucket),id)" +
                        ") WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': 4, 'max_threshold': 32};",
                keyspace, "assets");

        executeWithRetry(hostname, SimpleStatement.builder(createAssetsTable).setConsistencyLevel(DefaultConsistencyLevel.ALL).build());
        LOG.info("Ensured table {}.{} exists with SizeTieredCompactionStrategy", keyspace, "assets");
    }

    @Override
    public AtlasEdge<AtlasJanusVertex, AtlasJanusEdge> addEdge(AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> outVertex,
                                                               AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> inVertex,
                                                               String edgeLabel) throws AtlasBaseException {
        try {
            Vertex oV   = outVertex.getV().getWrappedElement();
            Vertex iV   = inVertex.getV().getWrappedElement();

            if (oV.id().equals(iV.id())) {
                LOG.error("Attempting to create a relationship between same vertex");
                throw new AtlasBaseException(RELATIONSHIP_CREATE_INVALID_PARAMS);
            }

            Edge   edge = oV.addEdge(edgeLabel, iV);

            return GraphDbObjectFactory.createEdge(this, edge);
        } catch (SchemaViolationException e) {
            throw new AtlasSchemaViolationException(e);
        }
    }

    @Override
    public AtlasGraphQuery<AtlasJanusVertex, AtlasJanusEdge> query() {
        return new AtlasJanusGraphQuery(this);
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex, AtlasEdge> V(final Object... vertexIds) {
        AtlasGraphTraversal traversal = new AtlasJanusGraphTraversal(this, getGraph().traversal());
        traversal.getBytecode().addStep(GraphTraversal.Symbols.V, vertexIds);
        traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, vertexIds));
        return traversal;
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex, AtlasEdge> E(final Object... edgeIds) {
        AtlasGraphTraversal traversal = new AtlasJanusGraphTraversal(this, getGraph().traversal());
        traversal.getBytecode().addStep(GraphTraversal.Symbols.E, edgeIds);
        traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, edgeIds));
        return traversal;
    }

    @Override
    public AtlasEdge getEdgeBetweenVertices(AtlasVertex fromVertex, AtlasVertex toVertex, String edgeLabel) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getEdgeBetweenVertices");
        try {
            GraphTraversal gt = V(fromVertex.getId()).outE(edgeLabel).where(__.otherV().hasId(toVertex.getId()));

            Edge gremlinEdge = getFirstActiveEdge(gt);
            return (gremlinEdge != null)
                    ? GraphDbObjectFactory.createEdge(this, gremlinEdge)
                    : null;
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    @Override
    public AtlasEdge<AtlasJanusVertex, AtlasJanusEdge> getEdge(String edgeId) {
        Iterator<Edge> it = getGraph().edges(edgeId);
        Edge           e  = getSingleElement(it, edgeId);

        return GraphDbObjectFactory.createEdge(this, e);
    }

    @Override
    public void removeEdge(AtlasEdge<AtlasJanusVertex, AtlasJanusEdge> edge) {
        Edge wrapped = edge.getE().getWrappedElement();

        wrapped.remove();
    }

    @Override
    public void removeVertex(AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> vertex) {
        Vertex wrapped = vertex.getV().getWrappedElement();

        wrapped.remove();
    }

    @Override
    public Iterable<AtlasEdge<AtlasJanusVertex, AtlasJanusEdge>> getEdges() {
        Iterator<Edge> edges = getGraph().edges();

        return wrapEdges(edges);
    }

    @Override
    public Iterable<AtlasVertex<AtlasJanusVertex, AtlasJanusEdge>> getVertices() {
        Iterator<Vertex> vertices = getGraph().vertices();

        return wrapVertices(vertices);
    }

    @Override
    public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> addVertex() {
        Vertex result = null;
        if (LEAN_GRAPH_ENABLED) {
            String id = generateCustomId();
            result = getGraph().addVertex(T.id, id);
        } else {
            result = getGraph().addVertex();
        }

        return GraphDbObjectFactory.createVertex(this, result);
    }

    @Override
    public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> addAssetVertex() {
        Vertex result = null;
        if (LEAN_GRAPH_ENABLED) {
            String id = generateCustomId();
            result = getGraph().addVertex(T.id, id, T.label, ASSET_VERTEX_LABEL);
        } else {
            result = getGraph().addVertex();
        }
        return GraphDbObjectFactory.createVertex(this, result);
    }

    @Override
    public AtlasIndexQueryParameter indexQueryParameter(String parameterName, String parameterValue) {
        return new AtlasJanusIndexQueryParameter(parameterName, parameterValue);
    }

    @Override
    public AtlasGraphIndexClient getGraphIndexClient() throws AtlasException {
        try {
            initApplicationProperties();

            return new AtlasJanusGraphIndexClient(APPLICATION_PROPERTIES);
        } catch (Exception e) {
            LOG.error("Error encountered in creating Graph Index Client.", e);
            throw new AtlasException(e);
        }
    }

    @Override
    public void commit() {
        getGraph().tx().commit();
    }

    @Override
    public void commit(AtlasTypeRegistry typeRegistry) {
        getGraph().tx().commit();

        commitIdOnly(typeRegistry);
    }

    private void commitIdOnly(AtlasTypeRegistry typeRegistry) {
        if (LEAN_GRAPH_ENABLED) {

            try {
                AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("commitIdOnly.callInsertVertices");
                // Extract updated vertices
                Set<AtlasVertex> updatedVertexList = RequestContext.get().getDifferentialGUIDS().stream()
                        .map(x -> ((AtlasVertex) RequestContext.get().getDifferentialVertex(x)))
                        .filter(Objects::nonNull)
                        .filter(AtlasVertex::isAssetVertex)
                        .collect(Collectors.toSet());

                // Extract SOFT deleted vertices
                updatedVertexList.addAll(RequestContext.get().getVerticesToSoftDelete().stream()
                        .map(x -> ((AtlasVertex) x))
                        .filter(Objects::nonNull)
                        .filter(AtlasVertex::isAssetVertex)
                        .collect(Collectors.toSet()));

                // Extract restored vertices
                if (!RequestContext.get().getRestoredVertices().isEmpty()) {
                    updatedVertexList.addAll(RequestContext.get().getRestoredVertices().stream()
                            .map(x -> ((AtlasVertex) x))
                            .filter(Objects::nonNull)
                            .filter(AtlasVertex::isAssetVertex)
                            .collect(Collectors.toSet()));
                }

                Map<String, Map<String, Object>> normalisedAttributes = normalizeAttributes(updatedVertexList, typeRegistry);

                if (CollectionUtils.isNotEmpty(updatedVertexList)) {
                    dynamicVertexService.insertVertices(normalisedAttributes);
                }
                RequestContext.get().endMetricRecord(recorder);

                recorder = RequestContext.get().startMetricRecord("commitIdOnly.callDropVertices");
                // Extract HARD/PURGE vertex Ids

                List<AtlasVertex> hardDeletedVertices = RequestContext.get().getVerticesToHardDelete().stream()
                        .map(x -> (AtlasVertex) x)
                        .filter(Objects::nonNull)
                        .filter(AtlasVertex::isAssetVertex)
                        .toList();

                List<String> purgedVertexIdsList = hardDeletedVertices.stream()
                        .map(AtlasElement::getIdForDisplay)
                        .toList();
                dynamicVertexService.dropVertices(purgedVertexIdsList);

                RequestContext.get().endMetricRecord(recorder);


                recorder = RequestContext.get().startMetricRecord("commitIdOnly.callInsertES");
                List<String> docIdsToDelete = hardDeletedVertices.stream()
                        .map(AtlasVertex::getDocId)
                        .toList();

                ESConnector.syncToEs(
                        getESPropertiesForUpdateFromMap(normalisedAttributes, typeRegistry),
                        true,
                        docIdsToDelete);

                RequestContext.get().endMetricRecord(recorder);
            } catch (AtlasBaseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Map<String, Map<String, Object>> normalizeAttributes(Set<AtlasVertex> vertices, AtlasTypeRegistry typeRegistry) {
        Map<String, Map<String, Object>> rt = new HashMap<>();

        for (AtlasVertex vertex : vertices) {
            String typeName = vertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);
            AtlasEntityType type = typeRegistry.getEntityTypeByName(typeName);

            Map<String, Object> allProperties = new HashMap<>(((AtlasJanusVertex) vertex).getDynamicVertex().getAllProperties());

            type.normalizeAttributeValuesForUpdate(allProperties);

            rt.put(vertex.getIdForDisplay(), allProperties);
        }

        return rt;
    }

    public Map<String, Map<String, Object>> getESPropertiesForUpdateFromMap(Map<String, Map<String, Object>> normalisedAttributes, AtlasTypeRegistry typeRegistry) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getESPropertiesForUpdateFromMap");
        if (MapUtils.isEmpty(normalisedAttributes)) {
            return null;
        }

        try {
            return normalisedAttributes.keySet().stream().collect(Collectors.toMap(
                    k -> k,
                    v -> getESPropertiesForUpdate(normalisedAttributes.get(v), typeRegistry)
            ));
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public Map<String, Map<String, Object>> getESPropertiesForUpdateFromVertices(Set<AtlasVertex> vertices, AtlasTypeRegistry typeRegistry) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getESPropertiesForUpdateFromVertices");
        if (CollectionUtils.isEmpty(vertices)) {
            return null;
        }
        try {
            return vertices.stream().collect(Collectors.toMap(
                    k -> k.getIdForDisplay(),
                    v -> getESPropertiesForUpdate(((AtlasJanusVertex) v).getDynamicVertex().getAllProperties(), typeRegistry)
            ));
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private Map<String, Object> getESPropertiesForUpdate(Map<String, Object> properties, AtlasTypeRegistry typeRegistry) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getESPropertiesForUpdate.filter");
        try {
            AtlasEntityType type = typeRegistry.getEntityTypeByName((String) properties.get(Constants.TYPE_NAME_PROPERTY_KEY));
            return getEligibleProperties(properties, type).stream()
                    .filter(k -> properties.get(k) != null)
                    .collect(Collectors.toMap(
                            k -> k,
                            v -> properties.get(v)
                    ));
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private List<String> getEligibleProperties(Map<String, Object> properties, AtlasEntityType type) {
        return properties.keySet().stream().filter(x ->
                        type.isAttributesForESSync(x) || x.startsWith(Constants.INTERNAL_PROPERTY_KEY_PREFIX))
                .toList();
    }

    @Override
    public void rollback() {
        getGraph().tx().rollback();
    }

    @Override
    public AtlasIndexQuery<AtlasJanusVertex, AtlasJanusEdge> indexQuery(String indexName, String graphQuery) {
        return indexQuery(indexName, graphQuery, 0, null);
    }

    @Override
    public AtlasIndexQuery<AtlasJanusVertex, AtlasJanusEdge> indexQuery(String indexName, String graphQuery, int offset) {
        return indexQuery(indexName, graphQuery, offset, null);
    }

    /**
     * Creates an index query.
     *
     * @param indexQueryParameters the parameterObject containing the information needed for creating the index.
     *
     */
    public AtlasIndexQuery<AtlasJanusVertex, AtlasJanusEdge> indexQuery(GraphIndexQueryParameters indexQueryParameters) {
        return indexQuery(indexQueryParameters.getIndexName(), indexQueryParameters.getGraphQueryString(), indexQueryParameters.getOffset(), indexQueryParameters.getIndexQueryParameters());
    }

    private AtlasIndexQuery<AtlasJanusVertex, AtlasJanusEdge> indexQuery(String indexName, String graphQuery, int offset, List<AtlasIndexQueryParameter> indexQueryParameterList) {
        String               prefix = getIndexQueryPrefix();
        JanusGraphIndexQuery query  = getGraph().indexQuery(indexName, graphQuery).setElementIdentifier(prefix).offset(offset);

        if(indexQueryParameterList != null && indexQueryParameterList.size() > 0) {
            for(AtlasIndexQueryParameter indexQueryParameter: indexQueryParameterList) {
                query = query.addParameter(new Parameter(indexQueryParameter.getParameterName(), indexQueryParameter.getParameterValue()));
            }
        }
        return new AtlasJanusIndexQuery(this, query);
    }

    public AtlasIndexQuery<AtlasJanusVertex, AtlasJanusEdge> elasticsearchQuery(String indexName, SearchSourceBuilder sourceBuilder) {
        assert elasticsearchClient != null;
        return new AtlasElasticsearchQuery(this, elasticsearchClient, INDEX_PREFIX + indexName, sourceBuilder);
    }

    public AtlasIndexQuery<AtlasJanusVertex, AtlasJanusEdge> elasticsearchQuery(String indexName, SearchParams searchParams) throws AtlasBaseException {
        if (restClient == null) {
            LOG.error("restClient is not initiated, failed to run query on ES");
            throw new AtlasBaseException(INDEX_SEARCH_CLIENT_NOT_INITIATED);
        }
        return new AtlasElasticsearchQuery(this, restClient, INDEX_PREFIX + indexName, searchParams, esUiClusterClient, esNonUiClusterClient);
    }

    @Override
    public void createOrUpdateESAlias(ESAliasRequestBuilder builder) throws AtlasBaseException {
        String aliasRequest = builder.build();

        HttpEntity entity = new NStringEntity(aliasRequest, ContentType.APPLICATION_JSON);
        Request request = new Request("POST", ES_API_ALIASES);
        request.setEntity(entity);

        Response response = null;
        try {
            response = restClient.performRequest(request);
        } catch (IOException e) {
            LOG.error("Failed to execute direct query on ES {}", e.getMessage());
            throw new AtlasBaseException(AtlasErrorCode.INDEX_ALIAS_FAILED, "creating/updating", e.getMessage());
        }

        int statusCode = response.getStatusLine().getStatusCode();;
        if (statusCode != 200) {
            throw new AtlasBaseException(AtlasErrorCode.INDEX_ALIAS_FAILED, "creating/updating", "Status code " + statusCode);
        }
    }

    @Override
    public void deleteESAlias(String indexName, String aliasName) throws AtlasBaseException {
        ESAliasRequestBuilder builder = new ESAliasRequestBuilder();
        builder.addAction(REMOVE, new ESAliasRequestBuilder.AliasAction(indexName, aliasName));

        String aliasRequest = builder.build();
        HttpEntity entity = new NStringEntity(aliasRequest, ContentType.APPLICATION_JSON);

        Request request = new Request("POST", ES_API_ALIASES);
        request.setEntity(entity);

        Response response = null;
        try {
            response = restClient.performRequest(request);
        } catch (IOException e) {
            LOG.error("Failed to execute direct query on ES {}", e.getMessage());
            throw new AtlasBaseException(AtlasErrorCode.INDEX_ALIAS_FAILED, "deleting", e.getMessage());
        }

        int statusCode = response.getStatusLine().getStatusCode();;
        if (statusCode != 200) {
            throw new AtlasBaseException(AtlasErrorCode.INDEX_ALIAS_FAILED, "deleting", "Status code " + statusCode);
        }
    }

    @Override
    public AtlasIndexQuery elasticsearchQuery(String indexName) throws AtlasBaseException {
        if (restClient == null) {
            LOG.error("restClient is not initiated, failed to run query on ES");
            throw new AtlasBaseException(INDEX_SEARCH_CLIENT_NOT_INITIATED);
        }
        return new AtlasElasticsearchQuery(this, indexName, restClient, esUiClusterClient, esNonUiClusterClient);
    }

    @Override
    public AtlasGraphManagement getManagementSystem() {
        return new AtlasJanusGraphManagement(this, getGraph().openManagement());
    }

    @Override
    public Set getOpenTransactions() {
        return janusGraph.getOpenTransactions();
    }

    @Override
    public void shutdown() {
        getGraph().close();
    }

    @Override
    public Set<String> getEdgeIndexKeys() {
        return getIndexKeys(Edge.class);
    }

    @Override
    public Set<String> getVertexIndexKeys() {
        return getIndexKeys(Vertex.class);
    }

    @Override
    public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> getVertex(String vertexId) {
        Iterator<Vertex> it     = getGraph().vertices(vertexId);
        Vertex           vertex = getSingleElement(it, vertexId);

        return GraphDbObjectFactory.createVertex(this, vertex);
    }

    @Override
    public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> getJanusVertex(String vertexId) {
        Iterator<Vertex> it     = getGraph().vertices(vertexId);
        Vertex           vertex = getSingleElement(it, vertexId);

        return GraphDbObjectFactory.createJanusVertex(this, vertex);
    }

    @Override
    public Set<AtlasVertex> getVertices(String... vertexIds) {
        Set<AtlasVertex> result = new HashSet<>();
        Iterator<Vertex> it     = getGraph().vertices(vertexIds);
        while( it.hasNext()) {
            Vertex vertex = it.next();
            if (vertex == null) {
                LOG.warn("Vertex with id {} not found", vertexIds);
            } else {
                result.add(GraphDbObjectFactory.createVertex(this, vertex));
            }
        }
        return result;
    }

    @Override
    public Iterable<AtlasVertex<AtlasJanusVertex, AtlasJanusEdge>> getVertices(String key, Object value) {
        AtlasGraphQuery<AtlasJanusVertex, AtlasJanusEdge> query = query();

        query.has(key, value);

        return query.vertices();
    }

    @Override
    public GremlinVersion getSupportedGremlinVersion() {
        return GremlinVersion.THREE;
    }

    @Override
    public void clear() {
        JanusGraph graph = getGraph();

        if (graph.isOpen()) {
            // only a shut down graph can be cleared
            graph.close();
        }

        try {
            JanusGraphFactory.drop(graph);
        } catch (BackendException ignoreEx) {
        }
    }

    public JanusGraph getGraph() {
        return this.janusGraph;
    }

    @Override
    public void exportToGson(OutputStream os) throws IOException {
        GraphSONMapper         mapper  = getGraph().io(IoCore.graphson()).mapper().create();
        GraphSONWriter.Builder builder = GraphSONWriter.build();

        builder.mapper(mapper);

        GraphSONWriter writer = builder.create();

        writer.writeGraph(os, getGraph());
    }

    @Override
    public GremlinGroovyScriptEngine getGremlinScriptEngine() {
        return scriptEngine.get();
    }

    @Override
    public void releaseGremlinScriptEngine(ScriptEngine scriptEngine) {
        if (scriptEngine instanceof GremlinGroovyScriptEngine) {
            try {
                ((GremlinGroovyScriptEngine) scriptEngine).reset();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public Object executeGremlinScript(String query, boolean isPath) throws AtlasBaseException {
        Object result = executeGremlinScript(query);

        return convertGremlinValue(result);
    }

    @Override
    public Object executeGremlinScript(ScriptEngine scriptEngine, Map<? extends String, ? extends Object> userBindings,
                                       String query, boolean isPath) throws ScriptException {
        Bindings bindings = scriptEngine.createBindings();

        bindings.putAll(userBindings);
        bindings.put("g", getGraph().traversal());

        Object result = scriptEngine.eval(query, bindings);

        return convertGremlinValue(result);
    }

    @Override
    public GroovyExpression generatePersisentToLogicalConversionExpression(GroovyExpression expr, AtlasType type) {
        //nothing special needed, value is stored in required type
        return expr;
    }

    @Override
    public boolean isPropertyValueConversionNeeded(AtlasType type) {
        return false;
    }

    @Override
    public boolean requiresInitialIndexedPredicate() {
        return false;
    }

    @Override
    public GroovyExpression getInitialIndexedPredicate(GroovyExpression parent) {
        return parent;
    }

    @Override
    public GroovyExpression addOutputTransformationPredicate(GroovyExpression expr, boolean isSelect, boolean isPath) {
        return expr;
    }

    @Override
    public boolean isMultiProperty(String propertyName) {
        return multiProperties.contains(propertyName);
    }

    public Iterable<AtlasVertex<AtlasJanusVertex, AtlasJanusEdge>> wrapVertices(Iterable<? extends Vertex> it) {

        return Iterables.transform(it,
                (Function<Vertex, AtlasVertex<AtlasJanusVertex, AtlasJanusEdge>>) input ->
                    GraphDbObjectFactory.createVertex(AtlasJanusGraph.this, input));

    }

    public Iterable<AtlasEdge<AtlasJanusVertex, AtlasJanusEdge>> wrapEdges(Iterator<? extends Edge> it) {
        Iterable<? extends Edge> iterable = new IteratorToIterableAdapter<>(it);

        return wrapEdges(iterable);
    }

    public Iterable<AtlasEdge<AtlasJanusVertex, AtlasJanusEdge>> wrapEdges(Iterable<? extends Edge> it) {

        return Iterables.transform(it,
                (Function<Edge, AtlasEdge<AtlasJanusVertex, AtlasJanusEdge>>) input ->
                        GraphDbObjectFactory.createEdge(AtlasJanusGraph.this, input));

    }

    public void addMultiProperties(Set<String> names) {
        multiProperties.addAll(names);
    }


    String getIndexFieldName(AtlasPropertyKey propertyKey, JanusGraphIndex graphIndex, Parameter ... parameters) {
        PropertyKey janusKey = AtlasJanusObjectFactory.createPropertyKey(propertyKey);
        if(parameters == null) {
            parameters = EMPTY_PARAMETER_ARRAY;
        }
        return janusGraph.getIndexSerializer().getDefaultFieldName(janusKey, parameters, graphIndex.getBackingIndex());
    }


    private String getIndexQueryPrefix() {
        final String ret;

        initApplicationProperties();

        if (APPLICATION_PROPERTIES == null) {
            ret = INDEX_SEARCH_VERTEX_PREFIX_DEFAULT;
        } else {
            ret = APPLICATION_PROPERTIES.getString(INDEX_SEARCH_VERTEX_PREFIX_PROPERTY, INDEX_SEARCH_VERTEX_PREFIX_DEFAULT);
        }

        return ret;
    }

    private Iterable<AtlasVertex<AtlasJanusVertex, AtlasJanusEdge>> wrapVertices(Iterator<? extends Vertex> it) {
        Iterable<? extends Vertex> iterable = new IteratorToIterableAdapter<>(it);

        return wrapVertices(iterable);
    }

    private static <T> T getSingleElement(Iterator<T> it, String id) {
        if (!it.hasNext()) {
            return null;
        }

        T element = it.next();

        if (it.hasNext()) {
            throw new RuntimeException("Multiple items were found with the id " + id);
        }

        return element;
    }

    private Object convertGremlinValue(Object rawValue) {
        if (rawValue instanceof Vertex) {
            return GraphDbObjectFactory.createVertex(this, (Vertex) rawValue);
        } else if (rawValue instanceof Edge) {
            return GraphDbObjectFactory.createEdge(this, (Edge) rawValue);
        } else if (rawValue instanceof Map) {
            Map<String, Object> rowValue = (Map<String, Object>) rawValue;

            return Maps.transformValues(rowValue, GREMLIN_VALUE_CONVERSION_FUNCTION);
        } else if (rawValue instanceof ImmutablePath) {
            ImmutablePath path = (ImmutablePath) rawValue;

            return convertGremlinValue(path.objects());
        } else if (rawValue instanceof List) {
            return Lists.transform((List) rawValue, GREMLIN_VALUE_CONVERSION_FUNCTION);
        } else if (rawValue instanceof Collection) {
            throw new UnsupportedOperationException("Unhandled collection type: " + rawValue.getClass());
        }

        return rawValue;
    }

    private Set<String> getIndexKeys(Class<? extends Element> janusGraphElementClass) {
        JanusGraphManagement      mgmt    = getGraph().openManagement();
        Iterable<JanusGraphIndex> indices = mgmt.getGraphIndexes(janusGraphElementClass);
        Set<String>               result  = new HashSet<String>();

        for (JanusGraphIndex index : indices) {
            result.add(index.name());
        }

        mgmt.commit();

        return result;

    }

    private Object executeGremlinScript(String gremlinQuery) throws AtlasBaseException {
        GremlinGroovyScriptEngine scriptEngine = getGremlinScriptEngine();

        try {
            Bindings bindings = scriptEngine.createBindings();

            bindings.put("graph", getGraph());
            bindings.put("g", getGraph().traversal());

            Object result = scriptEngine.eval(gremlinQuery, bindings);

            return result;
        } catch (ScriptException e) {
            throw new AtlasBaseException(AtlasErrorCode.GREMLIN_SCRIPT_EXECUTION_FAILED, e, gremlinQuery);
        } finally {
            releaseGremlinScriptEngine(scriptEngine);
        }
    }

    private void initApplicationProperties() {
        if (APPLICATION_PROPERTIES == null) {
            try {
                APPLICATION_PROPERTIES = ApplicationProperties.get();
            } catch (AtlasException ex) {
                // ignore
            }
        }
    }


    private final class ConvertGremlinValueFunction implements Function<Object, Object> {

        @Override
        public Object apply(Object input) {
            return convertGremlinValue(input);
        }
    }
    private Edge getFirstActiveEdge(GraphTraversal gt) {
        if (gt != null) {
            while (gt.hasNext()) {
                Edge gremlinEdge = (Edge) gt.next();
                if (gremlinEdge != null && gremlinEdge.property(STATE_PROPERTY_KEY).isPresent() &&
                        gremlinEdge.property(STATE_PROPERTY_KEY).value().equals(AtlasEntity.Status.ACTIVE.toString())
                ) {
                    return gremlinEdge;
                }
            }
        }

        return null;
    }

    private <T extends Statement<T>> ResultSet executeWithRetry(String hostname,
                                                                Statement<T> statement) {
        int MAX_RETRIES = 3;
        Duration INITIAL_BACKOFF = Duration.ofMillis(100);
        int retryCount = 0;
        Exception lastException;

        try (CqlSession tempCqlSession = getCQLBuilder(hostname).build();) {
            while (true) {
                try {
                    return tempCqlSession.execute(statement);
                } catch (DriverTimeoutException | WriteTimeoutException | NoHostAvailableException e) {
                    lastException = e;
                    retryCount++;
                    LOG.warn("Retry attempt {} for statement execution due to exception: {}", retryCount, e.toString());
                    if (retryCount >= MAX_RETRIES) {
                        break;
                    }
                    try {
                        long backoff = INITIAL_BACKOFF.toMillis() * (long)Math.pow(2, retryCount - 1);
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new AtlasBaseException("AtlasJAnusGraph: Interrupted during retry backoff", ie);
                    }
                }
            }
        } catch (AtlasBaseException be) {
            throw new RuntimeException(be);
        }

        LOG.error("AtlasJAnusGraph: Failed to execute statement after {} retries", MAX_RETRIES, lastException);
        throw new RuntimeException("AtlasJAnusGraph: Failed to execute statement after " + MAX_RETRIES + " retries", lastException);
    }

    private String generateCustomId() {
        return CUSTOM_ID_GENERATOR.nextId();
    }
}

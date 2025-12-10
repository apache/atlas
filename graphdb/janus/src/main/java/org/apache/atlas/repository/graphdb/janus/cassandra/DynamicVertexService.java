package org.apache.atlas.repository.graphdb.janus.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusVertex;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Main entry point for the batch vertex retrieval system.
 * Coordinates the retrieval process and delegates to specialized components.
 */

@Service
public class DynamicVertexService {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicVertexService.class);

    private final VertexDataRepository repository;
    private final JacksonVertexSerializer serializer;

    private static final int defaultBatchSize = AtlasConfiguration.ATLAS_CASSANDRA_BATCH_SIZE.getInt();

    public final static Set<String> VERTEX_CORE_PROPERTIES = new HashSet<>();

    /**
     * Creates a new BatchVertexRetrievalService with custom configuration.
     *
     * @param session The Cassandra session
     */
    @Inject
    public DynamicVertexService(CqlSession session) {
        this.repository = new CassandraVertexDataRepository(session);
        this.serializer = new JacksonVertexSerializer();

        VERTEX_CORE_PROPERTIES.add("__guid");
        VERTEX_CORE_PROPERTIES.add("__state");
        VERTEX_CORE_PROPERTIES.add("__typeName");
        VERTEX_CORE_PROPERTIES.add("qualifiedName");
        VERTEX_CORE_PROPERTIES.add("__u_qualifiedName");
    }

    /**
     * Retrieves vertex by its ID.
     *
     * @param vertexId The ID of vertex to retrieve
     * @return A DynamicVertex
     */
    public DynamicVertex retrieveVertex(String vertexId) throws AtlasBaseException {
        Map<String, DynamicVertex> ret = retrieveVertices(Collections.singletonList(vertexId));
        return ret.get(vertexId);
    }

    public void insertVertices(List<AtlasVertex> vertices) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(vertices)) {
            return;
        }

        Map<String, String> toInsert = new HashMap<>(vertices.size());
        vertices.stream()
                .filter(x -> ((AtlasJanusVertex) x).getDynamicVertex().hasProperties())
                .forEach(x -> toInsert.put(x.getIdForDisplay(), serializer.serialize(((AtlasJanusVertex) x).getDynamicVertex())));

        repository.insertVertices(toInsert);
    }

    public void insertVertices(Map<String, Map<String, Object>> allPropertiesMap) throws AtlasBaseException {
        if (MapUtils.isEmpty(allPropertiesMap)) {
            return;
        }
        Map<String, String> toInsert = new HashMap<>(allPropertiesMap.size());

        allPropertiesMap.keySet().forEach(x -> toInsert.put(x, serializer.serialize(allPropertiesMap.get(x))));

        repository.insertVertices(toInsert);
    }

    public void dropVertices(List<String> vertexIds) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(vertexIds)) {
            return;
        }
        repository.dropVertices(vertexIds);
    }

    /**
     * Retrieves multiple vertices by their IDs.
     *
     * @param vertexIds The list of vertex IDs to retrieve
     * @return A map of vertex ID to dynamic vertex data
     */
    public Map<String, DynamicVertex> retrieveVertices(List<String> vertexIds) throws AtlasBaseException {
        return retrieveVertices(vertexIds, defaultBatchSize);
    }

    /**
     * Retrieves multiple vertices by their IDs with custom batch size.
     *
     * @param vertexIds The list of vertex IDs to retrieve
     * @param batchSize The batch size to use
     * @return A map of vertex ID to dynamic vertex data
     */
    private Map<String, DynamicVertex> retrieveVertices(List<String> vertexIds, int batchSize) throws AtlasBaseException {
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, DynamicVertex> results = new HashMap<>();

        for (int i = 0; i < vertexIds.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, vertexIds.size());
            List<String> batch = vertexIds.subList(i, endIndex);

            // Use direct loading approach
            Map<String, DynamicVertex> batchResults = repository.fetchVerticesDirectly(batch);
            results.putAll(batchResults);
        }

        return results;
    }
}
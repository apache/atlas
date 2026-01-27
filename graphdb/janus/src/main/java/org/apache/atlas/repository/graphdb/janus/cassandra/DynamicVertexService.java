package org.apache.atlas.repository.graphdb.janus.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusVertex;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Main entry point for the batch vertex retrieval system.
 * Coordinates the retrieval process and delegates to specialized components.
 */

@Service
public class DynamicVertexService {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicVertexService.class);

    private final VertexDataRepository repository;
    private final JacksonVertexSerializer serializer;

    public final static Set<String> VERTEX_CORE_PROPERTIES = ConcurrentHashMap.newKeySet();

    static {
        VERTEX_CORE_PROPERTIES.add("__guid");
        VERTEX_CORE_PROPERTIES.add("__state");
        VERTEX_CORE_PROPERTIES.add("__typeName");
        VERTEX_CORE_PROPERTIES.add("qualifiedName");
        VERTEX_CORE_PROPERTIES.add("__u_qualifiedName");
    }

    /**
     * Creates a new BatchVertexRetrievalService with custom configuration.
     *
     * @param session The Cassandra session
     */
    @Inject
    public DynamicVertexService(CqlSession session) {
        this.repository = new CassandraVertexDataRepository(session);
        this.serializer = new JacksonVertexSerializer();
    }

    /**
     * Retrieves vertex by its ID.
     *
     * @param vertexId The ID of vertex to retrieve
     * @return A DynamicVertex
     */
    public DynamicVertex retrieveVertex(String vertexId) throws AtlasBaseException {
        try {
            return repository.fetchVertexAsync(vertexId).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
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

    public List<CompletableFuture<DynamicVertex>> retrieveVerticesAsync(List<String> vertexIds) {
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyList();
        }

        List<CompletableFuture<DynamicVertex>> futures = new ArrayList<>(vertexIds.size());
        for (String vertexId : vertexIds) {
            futures.add(repository.fetchVertexAsync(vertexId));
        }

        return futures;
    }

    public Map<String, DynamicVertex> retrieveVertices(List<String> vertexIds) {
        List<CompletableFuture<DynamicVertex>> futures = retrieveVerticesAsync(vertexIds);
        return mapVerticesByAtlasId(futures);
    }

    private Map<String, DynamicVertex> mapVerticesByAtlasId(List<CompletableFuture<DynamicVertex>> futures) {
        Map<String, DynamicVertex> vertexPropertiesMap = new HashMap<>();
        for (CompletableFuture<DynamicVertex> future : futures) {
            DynamicVertex vertex = future.join();
            if (vertex != null) {
                String atlasId = vertex.getProperty("_atlas_id", String.class);
                if (StringUtils.isNotEmpty(atlasId)) {
                    vertexPropertiesMap.put(atlasId, vertex);
                }
            }
        }
        return vertexPropertiesMap;
    }
}

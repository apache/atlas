package org.apache.atlas.discovery;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

@Component
public class VertexEdgeCache {

    private final ThreadLocal<Map<CachedVertexEdgesKey, List<AtlasEdge>>> edgeCache = ThreadLocal.withInitial(HashMap::new);

    public List<AtlasEdge> getEdges(AtlasVertex vertex, AtlasEdgeDirection direction, String edgeLabel) {
        CachedVertexEdgesKey key = new CachedVertexEdgesKey(vertex.getId(), direction, edgeLabel);
        Map<CachedVertexEdgesKey, List<AtlasEdge>> cache = edgeCache.get();
        if (cache.containsKey(key)) {
            return cache.get(key);
        } else {
            List<AtlasEdge> edges = newArrayList(vertex.getEdges(direction, edgeLabel));
            cache.put(key, edges);
            return edges;
        }
    }
}

package org.apache.hadoop.metadata.util;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Utility class for graph operations.
 */
public final class GraphUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GraphUtils.class);

    private GraphUtils() {
    }

    public static Vertex findVertex(Graph blueprintsGraph,
                                    String entityName, String entityType) {
        LOG.debug("Finding vertex for: name={}, type={}", entityName, entityType);

        GraphQuery query = blueprintsGraph.query()
                .has("entityName", entityName)
                .has("entityType", entityType);
        Iterator<Vertex> results = query.vertices().iterator();
        // returning one since name/type is unique
        return results.hasNext() ? results.next() : null;
    }

    public static  Map<String, String> extractProperties(Vertex entityVertex) {
        Map<String, String> properties = new HashMap<>();
        for (String key : entityVertex.getPropertyKeys()) {
            properties.put(key, String.valueOf(entityVertex.getProperty(key)));
        }

        return properties;
    }

    public static String vertexString(final Vertex vertex) {
        StringBuilder properties = new StringBuilder();
        for (String propertyKey : vertex.getPropertyKeys()) {
            properties.append(propertyKey)
                    .append("=").append(vertex.getProperty(propertyKey))
                    .append(", ");
        }

        return "v[" + vertex.getId() + "], Properties[" + properties + "]";
    }

    public static String edgeString(final Edge edge) {
        return "e[" + edge.getLabel() + "], ["
                + edge.getVertex(Direction.OUT).getProperty("name")
                + " -> " + edge.getLabel() + " -> "
                + edge.getVertex(Direction.IN).getProperty("name")
                + "]";
    }
}
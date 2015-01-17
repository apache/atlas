package org.apache.hadoop.metadata.util;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
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

    private static final String GUID_PROPERTY_KEY = "guid";
    private static final String TIMESTAMP_PROPERTY_KEY = "timestamp";

    private GraphUtils() {
    }

    public static Edge addEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        return addEdge(fromVertex, toVertex, edgeLabel, null);
    }

    public static Edge addEdge(Vertex fromVertex, Vertex toVertex,
                               String edgeLabel, String timestamp) {
        Edge edge = findEdge(fromVertex, toVertex, edgeLabel);

        Edge edgeToVertex = edge != null ? edge : fromVertex.addEdge(edgeLabel, toVertex);
        if (timestamp != null) {
            edgeToVertex.setProperty(TIMESTAMP_PROPERTY_KEY, timestamp);
        }

        return edgeToVertex;
    }

    public static Edge findEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        return findEdge(fromVertex, toVertex.getProperty(GUID_PROPERTY_KEY), edgeLabel);
    }

    public static Edge findEdge(Vertex fromVertex, Object toVertexName, String edgeLabel) {
        Edge edgeToFind = null;
        for (Edge edge : fromVertex.getEdges(Direction.OUT, edgeLabel)) {
            if (edge.getVertex(Direction.IN).getProperty(
                    GUID_PROPERTY_KEY).equals(toVertexName)) {
                edgeToFind = edge;
                break;
            }
        }

        return edgeToFind;
    }

    public static Vertex findVertex(Graph blueprintsGraph,
                                    String guid) {
        LOG.debug("Finding vertex for: guid={}", guid);

        GraphQuery query = blueprintsGraph.query().has("guid", guid);
        Iterator<Vertex> results = query.vertices().iterator();
        // returning one since name/type is unique
        return results.hasNext() ? results.next() : null;
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

    public static JSONObject vertexJSON(final Vertex vertex) throws JSONException {
        return GraphSONUtility.jsonFromElement(vertex, null, GraphSONMode.NORMAL);
    }

    public static String edgeString(final Edge edge) {
        return "e[" + edge.getLabel() + "], ["
                + edge.getVertex(Direction.OUT).getProperty("name")
                + " -> " + edge.getLabel() + " -> "
                + edge.getVertex(Direction.IN).getProperty("name")
                + "]";
    }
}
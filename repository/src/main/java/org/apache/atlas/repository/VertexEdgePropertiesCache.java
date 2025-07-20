package org.apache.atlas.repository;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.type.AtlasStructType;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY;

public class VertexEdgePropertiesCache {
    Map<String, Map<String, ArrayList<?>>> vertexProperties;
    Map<String, Map<String, Object>> edgeProperties;
    Map<String, Map<String, ArrayList<EdgeVertexReference>>> edgeLabelToVertexIds;

    public VertexEdgePropertiesCache() {
        this.vertexProperties = new HashMap<>();
        this.edgeProperties = new HashMap<>();
        this.edgeLabelToVertexIds = new HashMap<>();

    }

    public Map<String, ArrayList<?>> getVertexPropertiesById(String vertexId) {
        return vertexProperties.getOrDefault(vertexId, new HashMap<>());
    }

    public void addVertexProperties(String vertexId, Map<String, ArrayList<?>> properties) {
        vertexProperties.put(vertexId, properties);
    }

    public void addEdgeProperties(String edgeId, Map<String, Object> properties) {
        edgeProperties.put(edgeId, properties);
    }

    public <T>  ArrayList<?> getMultiValuedProperties(String vertexId, String propertyName) {
        Map<String, ArrayList<?>> properties = getVertexPropertiesById(vertexId);
        if (properties == null) {
            return null;
        }
        return properties.getOrDefault(propertyName, null);
    }

    public <T>  ArrayList<?> getMultiValuedProperties(String vertexId, String propertyName, Class<T> clazz ) {
        Map<String, ArrayList<?>> properties = getVertexPropertiesById(vertexId);
        ArrayList<T> result = new ArrayList<>();
        if (properties == null) {
            return null;
        }
         ArrayList<?> values =  properties.getOrDefault(propertyName, null);
        if (values == null || values.isEmpty()) {
            return null;
        }
        for (Object value : values) {
            result.add(clazz.cast(value));
        }
        return result;
    }

    public <Tp> Tp getPropertyValue(String elementId, String propertyName, Class<Tp> clazz) {
        Map<String, ArrayList<?>> vertexProperties = getVertexPropertiesById(elementId);
        Map<String , Object> edgeProperties = this.edgeProperties.get(elementId);
        if (vertexProperties == null && edgeProperties == null) {
            return null;
        }
        if(vertexProperties != null) {
            ArrayList<?> values = vertexProperties.getOrDefault(propertyName, null);
            if (values == null || values.isEmpty()) {
                return null;
            }
            Object value = values.get(0);
            if (clazz.isInstance(value)) {
                return clazz.cast(value);
            } else {
                throw new IllegalArgumentException("Property value is not of type " + clazz.getName());
            }
        } else {
            Object value = edgeProperties.get(propertyName);
            if (value == null) {
                return null;
            }
            if (clazz.isInstance(value)) {
                return clazz.cast(value);
            } else {
                throw new IllegalArgumentException("Property value is not of type " + clazz.getName());
            }
        }
    }

    public String getGuid(String vertexId) {
        return getPropertyValue(vertexId, GUID_PROPERTY_KEY, String.class);
    }

    public String getTypeName(String vertexId) {
        return getPropertyValue(vertexId, Constants.ENTITY_TYPE_PROPERTY_KEY, String.class);
    }

    public Map<String, Map<String, ArrayList<?>>> getVertexProperties() {
        return vertexProperties;
    }

    public void addEdgeLabelToVertexIds(String sourceVertexId, String edgeLabel, EdgeVertexReference targetElement) {
        edgeLabelToVertexIds
                .computeIfAbsent(sourceVertexId, k -> new HashMap<>())
                .computeIfAbsent(edgeLabel, k -> new ArrayList<>())
                .add(targetElement);
    }

    public ArrayList<EdgeVertexReference> getVertexEdgeReferencesByEdgeLabel(String sourceVertexId, String edgeLabel) {
        return edgeLabelToVertexIds.getOrDefault(sourceVertexId, new HashMap<>())
                .getOrDefault(edgeLabel, new ArrayList<>());
    }

    public EdgeVertexReference getReferenceVertexByEdgeLabelAndId(String sourceVertexId, String edgeLabel, String targetVertexId, String edgeId) {
        ArrayList<EdgeVertexReference> references = getVertexEdgeReferencesByEdgeLabel(sourceVertexId, edgeLabel);
        for (EdgeVertexReference reference : references) {
            if (reference.getReferenceVertexId().equals(targetVertexId) && reference.getEdge().id().toString().equals(edgeId)) {
                return reference;
            }
        }
        return null;
    }

    public List<Pair<String, Edge>> getCollectionElementsUsingRelationship(String vertexId, AtlasStructType.AtlasAttribute attribute) {
        String edgeLabel = attribute.getRelationshipEdgeLabel();
        ArrayList<EdgeVertexReference> references = getVertexEdgeReferencesByEdgeLabel(vertexId, edgeLabel);
        List<Pair<String, Edge>> ret = new ArrayList<>();
        for (EdgeVertexReference reference : references) {
            String targetVertexId = reference.getReferenceVertexId();
            Edge edge = reference.getEdge();
            ret.add(Pair.with(targetVertexId, edge));
        }
        return ret;
    }

    public Pair<String, Edge> getRelationShipElement(String vertexId, String edgeLabel) {
        ArrayList<EdgeVertexReference> references = getVertexEdgeReferencesByEdgeLabel(vertexId, edgeLabel);
        if (references == null || references.isEmpty()) {
            return null;
        }
        EdgeVertexReference reference = references.get(0);
        String targetVertexId = reference.getReferenceVertexId();
        Edge edge = reference.getEdge();
        return Pair.with(targetVertexId, edge);
    }



}

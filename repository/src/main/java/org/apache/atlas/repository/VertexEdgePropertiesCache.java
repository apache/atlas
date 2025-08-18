package org.apache.atlas.repository;

import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasStructType;
import org.apache.commons.collections.MapUtils;
import org.javatuples.Pair;

import java.util.*;

import static org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY;

public class VertexEdgePropertiesCache {
    Map<String, Map<String, List<?>>> vertexProperties;
    Map<String, Map<String, Object>> edgeProperties;
    Map<String, Map<String, List<EdgeVertexReference>>> edgeLabelToVertexIds;
    Map<String, AtlasVertex> vertexIdToVertexMap;

    public VertexEdgePropertiesCache() {
        this.vertexProperties = new HashMap<>();
        this.edgeProperties = new HashMap<>();
        this.edgeLabelToVertexIds = new HashMap<>();
        this.vertexIdToVertexMap = new HashMap<>();
    }

    public void addVertices(Map<String, AtlasVertex> vertices) {
        if (vertices != null) {
            for (Map.Entry<String, AtlasVertex> entry : vertices.entrySet()) {
                String vertexId = entry.getKey();
                AtlasVertex vertex = entry.getValue();
                vertexIdToVertexMap.put(vertexId, vertex);
            }
        }
    }

    public AtlasVertex getVertexById(String vertexId) {
        return vertexIdToVertexMap.get(vertexId);
    }

    public Map<String, List<?>> getVertexPropertiesById(String vertexId) {
        return vertexProperties.getOrDefault(vertexId, new HashMap<>());
    }

    public void addVertexProperties(String vertexId, Map<String, List<?>> properties) {
        vertexProperties.put(vertexId, properties);
    }

    public void addEdgeProperties(String edgeId, Map<String, Object> properties) {
        edgeProperties.put(edgeId, properties);
    }

    public <T>  List<?> getMultiValuedProperties(String vertexId, String propertyName) {
        Map<String, List<?>> properties = getVertexPropertiesById(vertexId);
        if (properties == null) {
            return null;
        }
        return properties.getOrDefault(propertyName, null);
    }

    public <T>  List<T> getMultiValuedProperties(String vertexId, String propertyName, Class<T> clazz ) {
        Map<String, List<?>> properties = getVertexPropertiesById(vertexId);
        List<T> result = new ArrayList<>();
        if (properties == null) {
            return null;
        }
         List<?> values =  properties.getOrDefault(propertyName, null);
        if (values == null || values.isEmpty()) {
            return null;
        }
        for (Object value : values) {
            result.add((T) value);
        }
        return result;
    }

    public <Tp> Tp getPropertyValue(String elementId, String propertyName, Class<Tp> clazz) {
        Map<String, List<?>> vertexProperties = getVertexPropertiesById(elementId);
        Map<String , Object> edgeProperties = this.edgeProperties.get(elementId);
        if (vertexProperties == null && edgeProperties == null) {
            return null;
        }
        if(MapUtils.isNotEmpty(vertexProperties)) {
            List<?> values = vertexProperties.getOrDefault(propertyName, null);
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

    public Map<String, Map<String, List<?>>> getVertexProperties() {
        return vertexProperties;
    }


    public boolean addEdgeLabelToVertexIds(String sourceVertexId, String edgeLabel, EdgeVertexReference targetElement, int maxEdgeCount) {
        List<EdgeVertexReference> targetElements = edgeLabelToVertexIds
                .computeIfAbsent(sourceVertexId, k -> new HashMap<>())
                .computeIfAbsent(edgeLabel, k -> new ArrayList<>());

        // Check if the maximum edge count is reached
        if (targetElements.size() >= maxEdgeCount) {
            return false;
        }

        for (EdgeVertexReference existingReference : targetElements) {
            if (existingReference.equals(targetElement)) {
                // Element already exists, don't add it
                return false;
            }
        }

        // Element doesn't exist, add it
        targetElements.add(targetElement);
        addEdgeProperties(targetElement.getEdgeId(), targetElement.getProperties());
        return true;
    }

    public List<EdgeVertexReference> getVertexEdgeReferencesByEdgeLabel(
            String sourceVertexId,
            String edgeLabel,
            AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection direction) {

        Map<String, List<EdgeVertexReference>> edgeMap = edgeLabelToVertexIds.get(sourceVertexId);
        if (edgeMap == null) {
            return Collections.emptyList();
        }

        List<EdgeVertexReference> references = edgeMap.get(edgeLabel);
        if (references == null || references.isEmpty()) {
            return Collections.emptyList();
        }

        return references.stream()
                .filter(reference -> !isDirectionMatch(reference, direction))
                .toList();
    }

    private boolean isDirectionMatch(
            EdgeVertexReference reference,
            AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection direction) {

        String referenceVertexId = reference.getReferenceVertexId();
        EdgeVertexReference.EdgeInfo edgeInfo = reference.getEdgeInfo();

        return switch (direction) {
            case IN -> edgeInfo.getInVertexId().equals(referenceVertexId);
            case OUT -> edgeInfo.getOutVertexId().equals(referenceVertexId);
            default -> false;
        };
    }

    public EdgeVertexReference getReferenceVertexByEdgeLabelAndId(String sourceVertexId, String edgeLabel, String targetVertexId, String edgeId, AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection direction) {
        List<EdgeVertexReference> references = getVertexEdgeReferencesByEdgeLabel(sourceVertexId, edgeLabel, direction);
        for (EdgeVertexReference reference : references) {
            if (reference.getReferenceVertexId().equals(targetVertexId) && reference.getEdgeId().equals(edgeId)) {
                return reference;
            }
        }
        return null;
    }

    public List<Pair<String, EdgeVertexReference.EdgeInfo>> getCollectionElementsUsingRelationship(String vertexId, AtlasStructType.AtlasAttribute attribute) {
        String edgeLabel = attribute.getRelationshipEdgeLabel();
        List<EdgeVertexReference> references = getVertexEdgeReferencesByEdgeLabel(vertexId, edgeLabel, attribute.getRelationshipEdgeDirection());
        List<Pair<String, EdgeVertexReference.EdgeInfo>> ret = new ArrayList<>();
        for (EdgeVertexReference reference : references) {
            String targetVertexId = reference.getReferenceVertexId();
            EdgeVertexReference.EdgeInfo edgeInfo = reference.getEdgeInfo();
            ret.add(Pair.with(targetVertexId, edgeInfo));
        }
        return ret;
    }

    public Pair<String, EdgeVertexReference.EdgeInfo> getRelationShipElement(String vertexId, String edgeLabel, AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection direction) {
        List<EdgeVertexReference> references = getVertexEdgeReferencesByEdgeLabel(vertexId, edgeLabel, direction);
        if (references == null || references.isEmpty()) {
            return null;
        }
        EdgeVertexReference reference = references.get(0);
        String targetVertexId = reference.getReferenceVertexId();
        EdgeVertexReference.EdgeInfo edge = reference.getEdgeInfo();
        return Pair.with(targetVertexId, edge);
    }



}

package org.apache.atlas.repository;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;

public class EdgeVertexReference {
    private final String referenceVertexId;
    private final String edgeId;
    private final String edgeLabel;
    private final String inVertexId;
    private final String outVertexId;
    private final  EdgeInfo edgeInfo;
    private Map<String, Object> properties;

    public EdgeVertexReference(String referenceVertexId,String edgeId, String edgeLabel, String inVertexId, String outVertexId, LinkedHashMap<Object, Object> properties) {
        this.referenceVertexId = referenceVertexId;
        this.edgeId = edgeId;
        this.edgeLabel = edgeLabel;
        this.inVertexId = inVertexId;
        this.outVertexId = outVertexId;
        this.edgeInfo = new EdgeInfo(edgeId, edgeLabel, inVertexId, outVertexId);

        setProperties(properties);
    }

    public EdgeInfo getEdgeInfo() {
        return edgeInfo;
    }

    public String getReferenceVertexId() {
        return referenceVertexId;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public <Tm> Tm getProperty(String propertyName, Class<Tm> clazz) {
        Object value = properties.get(propertyName);
        if (clazz.isInstance(value)) {
            return clazz.cast(value);
        }
        return null;
    }

    public Object getProperty(String propertyName) {
        return properties.get(propertyName);
    }

    public void setProperties(LinkedHashMap<Object, Object> properties) {
        // Remove the id and label from properties
        properties.remove(T.id);
        properties.remove(T.label);
        Map<String, Object> newProperties = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (entry.getKey() instanceof String) {
                newProperties.put((String) entry.getKey(), entry.getValue());
            }
        }
        this.properties = newProperties;

    }

    public AtlasRelationship toAtlasRelationship(VertexEdgePropertiesCache cache) {
        AtlasRelationship.AtlasRelationshipWithExtInfo relationshipWithExtInfo = new AtlasRelationship.AtlasRelationshipWithExtInfo();
        AtlasRelationship relationship = new AtlasRelationship();
        relationshipWithExtInfo.setRelationship(relationship);
        String typeName = getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);

        relationship.setGuid(getProperty(RELATIONSHIP_GUID_PROPERTY_KEY, String.class));
        relationship.setTypeName(typeName);

        relationship.setCreatedBy(getProperty(CREATED_BY_KEY, String.class));
        relationship.setUpdatedBy(getProperty(MODIFIED_BY_KEY, String.class));

        relationship.setCreateTime(new Date(getProperty(TIMESTAMP_PROPERTY_KEY, Long.class)));
        relationship.setUpdateTime(new Date(getProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class)));

        Long version = getProperty(VERSION_PROPERTY_KEY, Long.class);
        if (version != null) {
            relationship.setVersion(version);
        }

        Integer provenanceType = getProperty(PROVENANCE_TYPE_KEY, Integer.class);
        if (provenanceType != null) {
            relationship.setProvenanceType(provenanceType);
        }
        String state = getProperty(STATE_PROPERTY_KEY, String.class);
        if (state != null) {
            relationship.setStatus(AtlasRelationship.Status.valueOf(state));
        }

        String end1VertexId = outVertexId;
        String end2VertexId = inVertexId;

        // TODO: Keeping properties as null, need to evaluate later if we need these info
        relationship.setEnd1(new AtlasObjectId(cache.getGuid(end1VertexId), cache.getTypeName(end1VertexId), null));
        relationship.setEnd2(new AtlasObjectId(cache.getGuid(end2VertexId), cache.getTypeName(end2VertexId), null));

        relationship.setLabel(edgeLabel);
        relationship.setPropagateTags(AtlasRelationshipDef.PropagateTags.valueOf(getProperty(RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, String.class)));

        return relationshipWithExtInfo.getRelationship();
    }


    public String getEdgeId() {
        return edgeId;
    }

    public boolean equals(EdgeVertexReference other) {

        return edgeId.equals(other.edgeId) &&
               edgeLabel.equals(other.edgeLabel) &&
               inVertexId.equals(other.inVertexId) &&
               outVertexId.equals(other.outVertexId);

    }

    public class EdgeInfo {
        private final String edgeId;
        private final String edgeLabel;
        private final String inVertexId;
        private final String outVertexId;

        public EdgeInfo(String edgeId, String edgeLabel, String inVertexId, String outVertexId) {
            this.edgeId = edgeId;
            this.edgeLabel = edgeLabel;
            this.inVertexId = inVertexId;
            this.outVertexId = outVertexId;
        }

        public String getEdgeId() {
            return edgeId;
        }

        public String getEdgeLabel() {
            return edgeLabel;
        }

        public String getInVertexId() {
            return inVertexId;
        }

        public String getOutVertexId() {
            return outVertexId;
        }
    }
}
package org.apache.atlas.model.lineage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.instance.AtlasEntityHeader;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true, value = {"visitedEdges", "skippedEdges"})
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasLineageOnDemandInfo implements Serializable {
    private String baseEntityGuid;
    private Map<String, AtlasEntityHeader> guidEntityMap;
    private Set<LineageRelation> relations;
    private Set<String>                             visitedEdges;
    private Set<String>                             skippedEdges;
    private Map<String, LineageInfoOnDemand>        relationsOnDemand;
    private LineageOnDemandRequest                  lineageOnDemandPayload;
    private boolean                                 upstreamEntityLimitReached;
    private boolean                                 downstreamEntityLimitReached;

    public AtlasLineageOnDemandInfo() {
    }

    public enum LineageDirection {INPUT, OUTPUT, BOTH}

    /**
     * Captures lineage information for an entity instance like hive_table
     *
     * @param baseEntityGuid   guid of the lineage entity .
     * @param guidEntityMap    map of entity guid to AtlasEntityHeader (minimal entity info)
     * @param relations        list of lineage relations for the entity (fromEntityId -> toEntityId)
     */
    public AtlasLineageOnDemandInfo(String baseEntityGuid, Map<String, AtlasEntityHeader> guidEntityMap,
                            Set<LineageRelation> relations) {
        this.baseEntityGuid = baseEntityGuid;
        this.guidEntityMap = guidEntityMap;
        this.relations = relations;
    }

    public AtlasLineageOnDemandInfo(String baseEntityGuid, Map<String, AtlasEntityHeader> guidEntityMap,
                            Set<LineageRelation> relations, Set<String> visitedEdges, Set<String> skippedEdges,
                            Map<String, LineageInfoOnDemand> relationsOnDemand) {
        this.baseEntityGuid               = baseEntityGuid;
        this.guidEntityMap                = guidEntityMap;
        this.relations                    = relations;
        this.visitedEdges                 = visitedEdges;
        this.skippedEdges                 = skippedEdges;
        this.relationsOnDemand            = relationsOnDemand;
    }

    public String getBaseEntityGuid() {
        return baseEntityGuid;
    }

    public void setBaseEntityGuid(String baseEntityGuid) {
        this.baseEntityGuid = baseEntityGuid;
    }

    public Map<String, AtlasEntityHeader> getGuidEntityMap() {
        return guidEntityMap;
    }

    public void setGuidEntityMap(Map<String, AtlasEntityHeader> guidEntityMap) {
        this.guidEntityMap = guidEntityMap;
    }

    public Set<LineageRelation> getRelations() {
        return relations;
    }

    public void setRelations(Set<LineageRelation> relations) {
        this.relations = relations;
    }

    public Set<String> getVisitedEdges() {
        return visitedEdges;
    }

    public void setVisitedEdges(Set<String> visitedEdges) {
        this.visitedEdges = visitedEdges;
    }

    public Set<String> getSkippedEdges() {
        return skippedEdges;
    }

    public void setSkippedEdges(Set<String> skippedEdges) {
        this.skippedEdges = skippedEdges;
    }

    public Map<String, LineageInfoOnDemand> getRelationsOnDemand() {
        return relationsOnDemand;
    }

    public void setRelationsOnDemand(Map<String, LineageInfoOnDemand> relationsOnDemand) {
        this.relationsOnDemand = relationsOnDemand;
    }
    public LineageOnDemandRequest getLineageOnDemandPayload() {
        return lineageOnDemandPayload;
    }

    public void setLineageOnDemandPayload(LineageOnDemandRequest lineageOnDemandRequest) {
        this.lineageOnDemandPayload = lineageOnDemandRequest;
    }

    public boolean isUpstreamEntityLimitReached() {
        return upstreamEntityLimitReached;
    }

    public void setUpstreamEntityLimitReached(boolean upstreamEntityLimitReached) {
        this.upstreamEntityLimitReached = upstreamEntityLimitReached;
    }

    public boolean isDownstreamEntityLimitReached() {
        return downstreamEntityLimitReached;
    }

    public void setDownstreamEntityLimitReached(boolean downstreamEntityLimitReached) {
        this.downstreamEntityLimitReached = downstreamEntityLimitReached;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtlasLineageOnDemandInfo that = (AtlasLineageOnDemandInfo) o;
        return Objects.equals(baseEntityGuid, that.baseEntityGuid) &&
                Objects.equals(guidEntityMap, that.guidEntityMap) &&
                Objects.equals(relations, that.relations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseEntityGuid, guidEntityMap, relations);
    }

    @Override
    public String toString() {
        return "AtlasLineageInfo{" +
                "baseEntityGuid=" + baseEntityGuid +
                ", guidEntityMap=" + guidEntityMap +
                ", relations=" + relations +
                '}';
    }

    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true, value = {"inputRelationsReachedLimit", "outputRelationsReachedLimit", "fromCounter"})
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class LineageInfoOnDemand {
        @JsonProperty
        boolean                    hasMoreInputs;
        @JsonProperty
        boolean                    hasMoreOutputs;
        int                        inputRelationsCount;
        int                        outputRelationsCount;
        boolean                    isInputRelationsReachedLimit;
        boolean                    isOutputRelationsReachedLimit;
        @JsonProperty
        boolean                    hasUpstream;
        @JsonProperty
        boolean                    hasDownstream;
        LineageOnDemandConstraints onDemandConstraints;
        int                        fromCounter;  // Counter for relations to be skipped

        public LineageInfoOnDemand() { }

        public LineageInfoOnDemand(LineageOnDemandConstraints onDemandConstraints) {
            this.onDemandConstraints           = onDemandConstraints;
            this.hasMoreInputs                 = false;
            this.hasMoreOutputs                = false;
            this.inputRelationsCount           = 0;
            this.outputRelationsCount          = 0;
            this.isInputRelationsReachedLimit  = false;
            this.isOutputRelationsReachedLimit = false;
            this.hasUpstream                   = false;
            this.hasDownstream                 = false;
            this.fromCounter                   = 0;
        }

        public boolean isInputRelationsReachedLimit() {
            return isInputRelationsReachedLimit;
        }

        public void setInputRelationsReachedLimit(boolean isInputRelationsReachedLimit) {
            this.isInputRelationsReachedLimit = isInputRelationsReachedLimit;
        }

        public boolean isOutputRelationsReachedLimit() {
            return isOutputRelationsReachedLimit;
        }

        public void setOutputRelationsReachedLimit(boolean isOutputRelationsReachedLimit) {
            this.isOutputRelationsReachedLimit = isOutputRelationsReachedLimit;
        }

        public boolean hasMoreInputs() {
            return hasMoreInputs;
        }

        public void setHasMoreInputs(boolean hasMoreInputs) {
            this.hasMoreInputs = hasMoreInputs;
        }

        public boolean hasMoreOutputs() {
            return hasMoreOutputs;
        }

        public void setHasMoreOutputs(boolean hasMoreOutputs) {
            this.hasMoreOutputs = hasMoreOutputs;
        }

        public boolean hasUpstream() {
            return hasUpstream;
        }

        public void setHasUpstream(boolean hasUpstream) {
            this.hasUpstream = hasUpstream;
        }

        public boolean hasDownstream() {
            return hasDownstream;
        }

        public void setHasDownstream(boolean hasDownstream) {
            this.hasDownstream = hasDownstream;
        }

        public int getFromCounter() {
            return fromCounter;
        }

        public void incrementFromCounter() {
            fromCounter++;
        }

        public int getInputRelationsCount() {
            return inputRelationsCount;
        }

        public void incrementInputRelationsCount() {
            if (hasMoreInputs) {
                return;
            }

            if (isInputRelationsReachedLimit) {
                setHasMoreInputs(true);
                return;
            }

            this.inputRelationsCount++;

            if (inputRelationsCount == onDemandConstraints.getInputRelationsLimit()) {
                this.setInputRelationsReachedLimit(true);
                return;
            }
        }

        public int getOutputRelationsCount() {
            return outputRelationsCount;
        }

        public void incrementOutputRelationsCount() {
            if (hasMoreOutputs) {
                return;
            }

            if (isOutputRelationsReachedLimit) {
                setHasMoreOutputs(true);
                return;
            }

            this.outputRelationsCount++;

            if (outputRelationsCount == onDemandConstraints.getOutputRelationsLimit()) {
                this.setOutputRelationsReachedLimit(true);
                return;
            }
        }

        @Override
        public String toString() {
            return "LineageInfoOnDemand{" +
                    "hasMoreInputs='" + hasMoreInputs + '\'' +
                    ", hasMoreOutputs='" + hasMoreOutputs + '\'' +
                    ", inputRelationsCount='" + inputRelationsCount + '\'' +
                    ", outputRelationsCount='" + outputRelationsCount + '\'' +
                    ", hasUpstream='" + hasUpstream + '\'' +
                    ", hasDownstream='" + hasDownstream + '\'' +
                    '}';
        }

    }

    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class LineageRelation {
        private String fromEntityId;
        private String toEntityId;
        private String relationshipId;
        private String processId;

        public LineageRelation() {
        }

        public LineageRelation(String fromEntityId, String toEntityId, final String relationshipId) {
            this.fromEntityId = fromEntityId;
            this.toEntityId = toEntityId;
            this.relationshipId = relationshipId;
        }

        public LineageRelation(String fromEntityId, String toEntityId, final String relationshipId, String processId) {
            this(fromEntityId, toEntityId, relationshipId);
            this.processId = processId;
        }

        public String getFromEntityId() {
            return fromEntityId;
        }

        public void setFromEntityId(String fromEntityId) {
            this.fromEntityId = fromEntityId;
        }

        public String getToEntityId() {
            return toEntityId;
        }

        public void setToEntityId(String toEntityId) {
            this.toEntityId = toEntityId;
        }

        public String getRelationshipId() {
            return relationshipId;
        }

        public void setRelationshipId(final String relationshipId) {
            this.relationshipId = relationshipId;
        }

        public String getProcessId() {
            return processId;
        }

        public void setProcessId(String processId) {
            this.processId = processId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LineageRelation that = (LineageRelation) o;
            return Objects.equals(fromEntityId, that.fromEntityId) &&
                    Objects.equals(toEntityId, that.toEntityId) &&
                    Objects.equals(relationshipId, that.relationshipId) &&
                    Objects.equals(processId, that.processId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fromEntityId, toEntityId, relationshipId, processId);
        }

        @Override
        public String toString() {
            return "LineageRelation{" +
                    "fromEntityId='" + fromEntityId + '\'' +
                    ", toEntityId='" + toEntityId + '\'' +
                    ", relationshipId='" + relationshipId + '\'' +
                    ", processId='" + processId + '\'' +
                    '}';
        }
    }

}

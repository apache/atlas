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
package org.apache.atlas.model.lineage;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.atlas.model.instance.AtlasEntityHeader;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true, value = {"visitedEdges"})
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasLineageInfo implements Serializable {
    private String baseEntityGuid;
    private LineageDirection lineageDirection;
    private int lineageDepth;
    private int limit;
    private int offset;
    private Long remainingUpstreamVertexCount;
    private Long remainingDownstreamVertexCount;
    private boolean hasMoreUpstreamVertices = false;
    private boolean hasMoreDownstreamVertices = false;
    private Map<String, AtlasEntityHeader> guidEntityMap;
    private Set<LineageRelation> relations;
    private final Map<String, Map<LineageDirection, Boolean>> vertexChildrenInfo = new HashMap<>();
    private Set<String>                             visitedEdges;
    private Map<String, LineageInfoOnDemand>        relationsOnDemand;
    private Map<String, LineageOnDemandConstraints> lineageOnDemandPayload;

    public AtlasLineageInfo() {
    }

    public enum LineageDirection {INPUT, OUTPUT, BOTH}

    /**
     * Captures lineage information for an entity instance like hive_table
     *
     * @param baseEntityGuid   guid of the lineage entity .
     * @param guidEntityMap    map of entity guid to AtlasEntityHeader (minimal entity info)
     * @param relations        list of lineage relations for the entity (fromEntityId -> toEntityId)
     * @param lineageDirection direction of lineage, can be INPUT, OUTPUT or INPUT_AND_OUTPUT
     * @param lineageDepth     lineage depth to be fetched.
     * @param offset
     */
    public AtlasLineageInfo(String baseEntityGuid, Map<String, AtlasEntityHeader> guidEntityMap,
                            Set<LineageRelation> relations, LineageDirection lineageDirection,
                            int lineageDepth, int limit, int offset) {
        this.baseEntityGuid = baseEntityGuid;
        this.lineageDirection = lineageDirection;
        this.lineageDepth = lineageDepth;
        this.limit = limit;
        this.guidEntityMap = guidEntityMap;
        this.relations = relations;
        this.offset = offset;
    }

    public AtlasLineageInfo(String baseEntityGuid, Map<String, AtlasEntityHeader> guidEntityMap,
                            Set<LineageRelation> relations, Set<String> visitedEdges, Map<String, LineageInfoOnDemand> relationsOnDemand, LineageDirection lineageDirection, int lineageDepth) {
        this.baseEntityGuid               = baseEntityGuid;
        this.lineageDirection             = lineageDirection;
        this.lineageDepth                 = lineageDepth;
        this.guidEntityMap                = guidEntityMap;
        this.relations                    = relations;
        this.visitedEdges                 = visitedEdges;
        this.relationsOnDemand            = relationsOnDemand;
    }

    public void calculateRemainingUpstreamVertexCount(Long totalUpstreamVertexCount) {
        remainingUpstreamVertexCount = Math.max(totalUpstreamVertexCount - offset - limit, 0);
    }

    public void calculateRemainingDownstreamVertexCount(Long totalUpstreamVertexCount) {
        remainingDownstreamVertexCount = Math.max(totalUpstreamVertexCount - offset - limit, 0);
    }

    public void setRemainingUpstreamVertexCount(long remainingUpstreamVertexCount) {
        this.remainingUpstreamVertexCount = remainingUpstreamVertexCount;
    }

    public void setRemainingDownstreamVertexCount(long remainingDownstreamVertexCount) {
        this.remainingDownstreamVertexCount = remainingDownstreamVertexCount;
    }

    public boolean getHasMoreUpstreamVertices() {
        return hasMoreUpstreamVertices;
    }

    public void setHasMoreUpstreamVertices(boolean hasMoreUpstreamVertices) {
        this.hasMoreUpstreamVertices = hasMoreUpstreamVertices;
    }

    public boolean getHasMoreDownstreamVertices() {
        return hasMoreDownstreamVertices;
    }

    public void setHasMoreDownstreamVertices(boolean hasMoreDownstreamVertices) {
        this.hasMoreDownstreamVertices = hasMoreDownstreamVertices;
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

    public LineageDirection getLineageDirection() {
        return lineageDirection;
    }

    public void setLineageDirection(LineageDirection lineageDirection) {
        this.lineageDirection = lineageDirection;
    }

    public int getLineageDepth() {
        return lineageDepth;
    }

    public void setLineageDepth(int lineageDepth) {
        this.lineageDepth = lineageDepth;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public Long getRemainingUpstreamVertexCount() {
        return remainingUpstreamVertexCount;
    }

    public Long getRemainingDownstreamVertexCount() {
        return remainingDownstreamVertexCount;
    }

    public Map<String, Map<LineageDirection, Boolean>> getVertexChildrenInfo() {
        return vertexChildrenInfo;
    }

    public void setHasChildrenForDirection(String guid, LineageChildrenInfo lineageChildrenInfo) {
        Map<LineageDirection, Boolean> entityChildrenMap = vertexChildrenInfo.get(guid);
        if (entityChildrenMap == null) {
            entityChildrenMap = new HashMap<>();
        }
        entityChildrenMap.put(lineageChildrenInfo.getDirection(), lineageChildrenInfo.getHasMoreChildren());

        vertexChildrenInfo.put(guid, entityChildrenMap);
    }

    public Set<String> getVisitedEdges() {
        return visitedEdges;
    }

    public void setVisitedEdges(Set<String> visitedEdges) {
        this.visitedEdges = visitedEdges;
    }

    public Map<String, LineageInfoOnDemand> getRelationsOnDemand() {
        return relationsOnDemand;
    }

    public void setRelationsOnDemand(Map<String, LineageInfoOnDemand> relationsOnDemand) {
        this.relationsOnDemand = relationsOnDemand;
    }
    public Map<String, LineageOnDemandConstraints> getLineageOnDemandPayload() {
        return lineageOnDemandPayload;
    }

    public void setLineageOnDemandPayload(Map<String, LineageOnDemandConstraints> lineageOnDemandPayload) {
        this.lineageOnDemandPayload = lineageOnDemandPayload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtlasLineageInfo that = (AtlasLineageInfo) o;
        return lineageDepth == that.lineageDepth &&
                Objects.equals(baseEntityGuid, that.baseEntityGuid) &&
                lineageDirection == that.lineageDirection &&
                Objects.equals(guidEntityMap, that.guidEntityMap) &&
                Objects.equals(relations, that.relations) &&
                Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseEntityGuid, lineageDirection, lineageDepth, guidEntityMap, relations, limit);
    }

    @Override
    public String toString() {
        return "AtlasLineageInfo{" +
                "baseEntityGuid=" + baseEntityGuid +
                ", guidEntityMap=" + guidEntityMap +
                ", relations=" + relations +
                ", lineageDirection=" + lineageDirection +
                ", lineageDepth=" + lineageDepth +
                ", limit=" + limit +
                '}';
    }

    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true, value = {"inputRelationsReachedLimit", "outputRelationsReachedLimit"})
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
        boolean                    hasMoreInputChildren;
        @JsonProperty
        boolean                    hasMoreOutputChildren;
        LineageOnDemandConstraints onDemandConstraints;

        public LineageInfoOnDemand() { }

        public LineageInfoOnDemand(LineageOnDemandConstraints onDemandConstraints) {
            this.onDemandConstraints           = onDemandConstraints;
            this.hasMoreInputs                 = false;
            this.hasMoreOutputs                = false;
            this.inputRelationsCount           = 0;
            this.outputRelationsCount          = 0;
            this.isInputRelationsReachedLimit  = false;
            this.isOutputRelationsReachedLimit = false;
            this.hasMoreInputChildren          = false;
            this.hasMoreOutputChildren         = false;
        }

        public boolean isInputRelationsReachedLimit() {
            return isInputRelationsReachedLimit;
        }

        public void setInputRelationsReachedLimit(boolean inputRelationsReachedLimit) {
            isInputRelationsReachedLimit = inputRelationsReachedLimit;
        }

        public boolean isOutputRelationsReachedLimit() {
            return isOutputRelationsReachedLimit;
        }

        public void setOutputRelationsReachedLimit(boolean outputRelationsReachedLimit) {
            isOutputRelationsReachedLimit = outputRelationsReachedLimit;
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

        public boolean hasMoreInputChildren() {
            return hasMoreInputChildren;
        }

        public void setHasMoreInputChildren(boolean hasMoreInputChildren) {
            this.hasMoreInputChildren = hasMoreInputChildren;
        }

        public boolean hasMoreOutputChildren() {
            return hasMoreOutputChildren;
        }

        public void setHasMoreOutputChildren(boolean hasMoreOutputChildren) {
            this.hasMoreOutputChildren = hasMoreOutputChildren;
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
                    ", hasMoreInputChildren='" + hasMoreInputChildren + '\'' +
                    ", hasMoreOutputChildren='" + hasMoreOutputChildren + '\'' +
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

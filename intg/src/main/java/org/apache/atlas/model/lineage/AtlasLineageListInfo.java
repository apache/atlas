package org.apache.atlas.model.lineage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.instance.AtlasEntityHeader;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.HashSet;
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
public class AtlasLineageListInfo implements Serializable {
    private HashSet<AtlasEntityHeader>              entities;
    private Map<String, LineageListEntityInfo>      entityInfoMap;
    private LineageListRequest                      searchParameters;
    private Set<String>                             visitedEdges;
    private Set<String>                             skippedEdges;
    private long                                    count;

    public AtlasLineageListInfo() {
    }

    /**
     * Captures lineage list information for an entity instance like hive_table
     *
     * @param entities   list of entities
     * @param entityInfoMap    map of entity guid to AtlasEntityHeader (minimal entity info)
     */
    public AtlasLineageListInfo(HashSet<AtlasEntityHeader> entities, Map<String, LineageListEntityInfo> entityInfoMap) {
        this.entities         = entities;
        this.entityInfoMap    = entityInfoMap;
    }

    /**
     * Captures lineage list information for an entity instance like hive_table
     *
     * @param entities   list of entities
     * @param entityInfoMap    map of entity guid to AtlasEntityHeader (minimal entity info)
     */
    public AtlasLineageListInfo(HashSet<AtlasEntityHeader> entities, Map<String, LineageListEntityInfo> entityInfoMap,
                                Set<String> visitedEdges, Set<String> skippedEdges, long count) {
        this.entities                     = entities;
        this.entityInfoMap                = entityInfoMap;
        this.visitedEdges                 = visitedEdges;
        this.skippedEdges                 = skippedEdges;
        this.count                        = count;
    }

    public HashSet<AtlasEntityHeader> getEntities() {
        return entities;
    }

    public void setEntities(HashSet<AtlasEntityHeader> entities) {
        this.entities = entities;
    }

    public Map<String, LineageListEntityInfo> getEntityInfoMap() {
        return entityInfoMap;
    }

    public void setEntityInfoMap(Map<String, LineageListEntityInfo> entityInfoMap) {
        this.entityInfoMap = entityInfoMap;
    }

    public LineageListRequest getSearchParameters() {
        return searchParameters;
    }

    public void setSearchParameters(LineageListRequest searchParameters) {
        this.searchParameters = searchParameters;
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

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void incrementBy(long count) {
        this.count += count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtlasLineageListInfo that = (AtlasLineageListInfo) o;
        return Objects.equals(entities, that.entities) &&
                Objects.equals(entityInfoMap, that.entityInfoMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entities, entityInfoMap);
    }


    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true, value = {"inputRelationsReachedLimit", "outputRelationsReachedLimit",
            "fromCounter", "size"})
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class LineageListEntityInfo {
        @JsonProperty
        boolean                    hasMoreInputs;
        @JsonProperty
        boolean                    hasMoreOutputs;
        int                        inputRelationsCount;
        int                        outputRelationsCount;
        boolean                    isInputRelationsReachedLimit;
        boolean                    isOutputRelationsReachedLimit;
        int                        fromCounter;  // Counter for relations to be skipped
        int                        size;

        public LineageListEntityInfo() {}

        public LineageListEntityInfo(int size) {
            this.hasMoreInputs                 = false;
            this.hasMoreOutputs                = false;
            this.inputRelationsCount           = 0;
            this.outputRelationsCount          = 0;
            this.isInputRelationsReachedLimit  = false;
            this.isOutputRelationsReachedLimit = false;
            this.fromCounter                   = 0;
            this.size                          = size;
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

            if (inputRelationsCount == size) {
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

            if (outputRelationsCount == size) {
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
                    '}';
        }

    }

    @Override
    public String toString() {
        return "AtlasLineageInfo{" +
                "entities=" + entities +
                ", entityInfoMap=" + entityInfoMap +
                '}';
    }

}

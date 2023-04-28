package org.apache.atlas.model.lineage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasEntityHeaderLineageReponse;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.*;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true, value = {"visitedEdges", "skippedEdges", "traversalQueue"})
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasLineageListInfo implements Serializable {
    private Set<AtlasEntityHeaderLineageReponse>    entities;
    private Map<String, LineageListEntityInfo>      entityInfoMap;
    private LineageListRequest                      searchParameters;

    public AtlasLineageListInfo() {}

    /**
     * Captures lineage list information for an entity instance like hive_table
     *
     * @param entities   list of entities
     * @param entityInfoMap    map of entity guid to AtlasEntityHeader (minimal entity info)
     */
    public AtlasLineageListInfo(Set<AtlasEntityHeaderLineageReponse> entities, Map<String, LineageListEntityInfo> entityInfoMap) {
        this.entities         = entities;
        this.entityInfoMap    = entityInfoMap;
    }

    public Set<AtlasEntityHeaderLineageReponse> getEntities() {
        return entities;
    }

    public void setEntities(Set<AtlasEntityHeaderLineageReponse> entities) {
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
        boolean                    hasMore;
        int                        relationsCount;
        boolean                    isRelationsReachedLimit;
        int                        fromCounter;  // Counter for relations to be skipped
        int                        size;

        public LineageListEntityInfo() {}

        public LineageListEntityInfo(int size) {
            this.hasMore                       = false;
            this.relationsCount                = 0;
            this.isRelationsReachedLimit       = false;
            this.fromCounter                   = 0;
            this.size                          = size;
        }

        public boolean isHasMore() {
            return hasMore;
        }

        public void setHasMore(boolean hasMore) {
            this.hasMore = hasMore;
        }

        public int getRelationsCount() {
            return relationsCount;
        }

        public void setRelationsCount(int relationsCount) {
            this.relationsCount = relationsCount;
        }

        public boolean isRelationsReachedLimit() {
            return isRelationsReachedLimit;
        }

        public void setRelationsReachedLimit(boolean relationsReachedLimit) {
            isRelationsReachedLimit = relationsReachedLimit;
        }

        public int getFromCounter() {
            return fromCounter;
        }

        public void incrementFromCounter() {
            fromCounter++;
        }


        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public void incrementRelationsCount() {
            this.relationsCount++;
            if (relationsCount == size)
                this.setRelationsReachedLimit(true);
        }

        @Override
        public String toString() {
            return "LineageListEntityInfo{" +
                    "hasMore=" + hasMore +
                    ", relationsCount=" + relationsCount +
                    ", isRelationsReachedLimit=" + isRelationsReachedLimit +
                    ", fromCounter=" + fromCounter +
                    ", size=" + size +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "AtlasLineageListInfo{" +
                "entities=" + entities +
                ", entityInfoMap=" + entityInfoMap +
                '}';
    }

}

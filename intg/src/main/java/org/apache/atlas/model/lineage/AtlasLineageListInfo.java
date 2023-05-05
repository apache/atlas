package org.apache.atlas.model.lineage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
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
    private List<AtlasEntityHeaderLineageReponse>   entities;
    private boolean                                 hasMore;
    private int                                     entityCount;
    private LineageListRequest                      searchParameters;

    public AtlasLineageListInfo() {}

    /**
     * Captures lineage list information for an entity instance like hive_table
     *
     * @param entities   list of entities
     */
    public AtlasLineageListInfo(List<AtlasEntityHeaderLineageReponse> entities) {
        this.entities         = entities;
    }

    public List<AtlasEntityHeaderLineageReponse> getEntities() {
        return entities;
    }

    public void setEntities(List<AtlasEntityHeaderLineageReponse> entities) {
        this.entities = entities;
    }


    public LineageListRequest getSearchParameters() {
        return searchParameters;
    }

    public void setSearchParameters(LineageListRequest searchParameters) {
        this.searchParameters = searchParameters;
    }

    public boolean isHasMore() {
        return hasMore;
    }

    public void setHasMore(boolean hasMore) {
        this.hasMore = hasMore;
    }

    public int getEntityCount() {
        return entityCount;
    }

    public void setEntityCount(int entityCount) {
        this.entityCount = entityCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtlasLineageListInfo that = (AtlasLineageListInfo) o;
        return hasMore == that.hasMore && entityCount == that.entityCount && Objects.equals(entities, that.entities) && Objects.equals(searchParameters, that.searchParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entities, hasMore, entityCount, searchParameters);
    }

    @Override
    public String toString() {
        return "AtlasLineageListInfo{" +
                "entities=" + entities +
                ", hasMore=" + hasMore +
                ", relationsCount=" + entityCount +
                ", searchParameters=" + searchParameters +
                '}';
    }

}

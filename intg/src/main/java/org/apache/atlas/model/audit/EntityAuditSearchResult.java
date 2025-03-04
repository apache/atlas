package org.apache.atlas.model.audit;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.Clearable;
import org.apache.atlas.model.instance.AtlasEntityHeader;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class EntityAuditSearchResult implements Serializable, Clearable  {

    public EntityAuditSearchResult() { }

    private List<EntityAuditEventV2> entityAudits;
    private Map<String, Object> aggregations;
    private int count;
    private int totalCount;
    private Map<String, AtlasEntityHeader> linkedEntities = new HashMap<>(0);

    public List<EntityAuditEventV2> getEntityAudits() {
        return entityAudits;
    }

    public void setEntityAudits(List<EntityAuditEventV2> entityAudits) {
        this.entityAudits = entityAudits;
    }

    public Map<String, Object> getAggregations() {
        return aggregations;
    }

    public void setAggregations(Map<String, Object> aggregations) {
        this.aggregations = aggregations;
    }

    public long getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public Map<String, AtlasEntityHeader> getLinkedEntities() { return linkedEntities; }

    public void setLinkedEntities(Map<String, AtlasEntityHeader> linkedEntities) { this.linkedEntities = linkedEntities != null ? linkedEntities : new HashMap<>(0); }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        EntityAuditSearchResult that = (EntityAuditSearchResult) o;

        return Objects.equals(entityAudits, that.entityAudits) &&
                Objects.equals(aggregations, that.aggregations) &&
                Objects.equals(count, that.count) &&
                Objects.equals(totalCount, that.totalCount) &&
                Objects.equals(linkedEntities, that.linkedEntities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityAudits, aggregations, count, totalCount, linkedEntities);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EntityAuditSearchResult{");
        sb.append("entityAudits='").append(entityAudits).append('\'');
        sb.append(", aggregations='").append(aggregations).append('\'');
        sb.append(", linkedEntities='").append(linkedEntities).append('\'');
        sb.append(", count=").append(count);
        sb.append(", totalCount=").append(totalCount);
        sb.append('}');

        return sb.toString();
    }

    @Override
    public void clear() {
        entityAudits = null;
        aggregations = null;
        count = 0;
        totalCount = 0;
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.instance;

import org.apache.atlas.model.PList;
import org.apache.atlas.model.SearchFilter.SortType;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * An instance of an entity - like hive_table, hive_database.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasEntity extends AtlasStruct implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Status of the entity - can be active or deleted. Deleted entities are not removed from Atlas store.
     */
    public enum Status { STATUS_ACTIVE, STATUS_DELETED }

    private String guid       = null;
    private Status status     = Status.STATUS_ACTIVE;
    private String createdBy  = null;
    private String updatedBy  = null;
    private Date   createTime = null;
    private Date   updateTime = null;
    private Long   version    = null;

    @JsonIgnore
    private static AtomicLong s_nextId = new AtomicLong(System.nanoTime());

    public AtlasEntity() {
        this(null, null);
    }

    public AtlasEntity(String typeName) {
        this(typeName, null);
    }

    public AtlasEntity(AtlasEntityDef entityDef) {
        this(entityDef != null ? entityDef.getName() : null, null);
    }

    public AtlasEntity(String typeName, Map<String, Object> attributes) {
        super(typeName, attributes);

        setGuid(nextInternalId());
        setStatus(null);
        setCreatedBy(null);
        setUpdatedBy(null);
        setCreateTime(null);
        setUpdateTime(null);
        setVersion(null);
    }

    public AtlasEntity(AtlasEntity other) {
        super(other);

        if (other != null) {
            setGuid(other.getGuid());
            setStatus(other.getStatus());
            setCreatedBy(other.getCreatedBy());
            setUpdatedBy(other.getUpdatedBy());
            setCreateTime(other.getCreateTime());
            setUpdateTime(other.getUpdateTime());
            setVersion(other.getVersion());
        }
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    @Override
    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasEntity{");
        sb.append("guid='").append(guid).append('\'');
        sb.append(", status=").append(status);
        sb.append(", createdBy='").append(createdBy).append('\'');
        sb.append(", updatedBy='").append(updatedBy).append('\'');
        dumpDateField(", createTime=", createTime, sb);
        dumpDateField(", updateTime=", updateTime, sb);
        sb.append(", version=").append(version);
        sb.append(", ");
        super.toString(sb);
        sb.append('}');

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }

        AtlasEntity that = (AtlasEntity) o;
        return Objects.equals(guid, that.guid) &&
                status == that.status &&
                Objects.equals(createdBy, that.createdBy) &&
                Objects.equals(updatedBy, that.updatedBy) &&
                Objects.equals(createTime, that.createTime) &&
                Objects.equals(updateTime, that.updateTime) &&
                Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), guid, status, createdBy, updatedBy, createTime, updateTime, version);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    /**
     * REST serialization friendly list.
     */
    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    @XmlSeeAlso(AtlasEntity.class)
    public static class AtlasEntities extends PList<AtlasEntity> {
        private static final long serialVersionUID = 1L;

        public AtlasEntities() {
            super();
        }

        public AtlasEntities(List<AtlasEntity> list) {
            super(list);
        }

        public AtlasEntities(List list, long startIndex, int pageSize, long totalCount,
                             SortType sortType, String sortBy) {
            super(list, startIndex, pageSize, totalCount, sortType, sortBy);
        }
    }

    @JsonIgnore
    public boolean validate(String id) {
        try {
            long l = Long.parseLong(id);
            return l < 0;
        } catch (NumberFormatException ne) {
            return false;
        }
    }

    @JsonIgnore
    public boolean isUnassigned() {
        return guid != null && guid.length() > 0 && guid.charAt(0) == '-';
    }

    @JsonIgnore
    public boolean isAssigned() {
        return isAssigned(guid);
    }

    @JsonIgnore
    public static boolean isAssigned(String guid) {
        try {
            UUID.fromString(guid);
        } catch (IllegalArgumentException e) {
            return false;
        }

        return true;
    }

    private String nextInternalId() {
        return "-" + Long.toString(s_nextId.getAndIncrement());
    }
}

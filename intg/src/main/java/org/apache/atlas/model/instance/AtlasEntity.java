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
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    public enum Status { ACTIVE, DELETED }

    private String guid       = null;
    private Status status     = Status.ACTIVE;
    private String createdBy  = null;
    private String updatedBy  = null;
    private Date   createTime = null;
    private Date   updateTime = null;
    private Long   version    = 0L;

    private List<AtlasClassification> classifications;

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

    public AtlasEntity(String typeName, String attrName, Object attrValue) {
        super(typeName, attrName, attrValue);

        init();
    }

    public AtlasEntity(String typeName, Map<String, Object> attributes) {
        super(typeName, attributes);

        init();
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
            setClassifications(other.getClassifications());
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

    public List<AtlasClassification> getClassifications() { return classifications; }

    public void setClassifications(List<AtlasClassification> classifications) { this.classifications = classifications; }


    private void init() {
        setGuid(nextInternalId());
        setStatus(null);
        setCreatedBy(null);
        setUpdatedBy(null);
        setCreateTime(null);
        setUpdateTime(null);
        setClassifications(null);
    }

    private static String nextInternalId() {
        return "-" + Long.toString(s_nextId.getAndIncrement());
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
        sb.append(", classifications=[");
        AtlasBaseTypeDef.dumpObjects(classifications, sb);
        sb.append(']');
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
                Objects.equals(version, that.version) &&
                Objects.equals(classifications, that.classifications);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), guid, status, createdBy, updatedBy, createTime, updateTime, version,
                            classifications);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    /**
     * An instance of an entity along with extended info - like hive_table, hive_database.
     */
    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasEntityExtInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private Map<String, AtlasEntity> referredEntities;


        public AtlasEntityExtInfo() {
            setReferredEntities(null);
        }

        public AtlasEntityExtInfo(AtlasEntity referredEntity) {
            addReferredEntity(referredEntity);
        }

        public AtlasEntityExtInfo(Map<String, AtlasEntity> referredEntities) {
            setReferredEntities(referredEntities);
        }

        public AtlasEntityExtInfo(AtlasEntityExtInfo other) {
            if (other != null) {
                setReferredEntities(other.getReferredEntities());
            }
        }

        public Map<String, AtlasEntity> getReferredEntities() { return referredEntities; }

        public void setReferredEntities(Map<String, AtlasEntity> referredEntities) { this.referredEntities = referredEntities; }

        @JsonIgnore
        public final void addReferredEntity(AtlasEntity entity) {
            addReferredEntity(entity.getGuid(), entity);
        }

        @JsonIgnore
        public final void addReferredEntity(String guid, AtlasEntity entity) {
            Map<String, AtlasEntity> r = this.referredEntities;

            if (r == null) {
                r = new HashMap<>();

                this.referredEntities = r;
            }

            if (guid != null) {
                r.put(guid, entity);
            }
        }

        @JsonIgnore
        public final AtlasEntity removeReferredEntity(String guid) {
            Map<String, AtlasEntity> r = this.referredEntities;

            return r != null && guid != null ? r.remove(guid) : null;
        }

        @JsonIgnore
        public final AtlasEntity getReferredEntity(String guid) {
            Map<String, AtlasEntity> r = this.referredEntities;

            return r != null && guid != null ? r.get(guid) : null;
        }

        @JsonIgnore
        public AtlasEntity getEntity(String guid) {
            return getReferredEntity(guid);
        }

        @JsonIgnore
        public AtlasEntity removeEntity(String guid) {
            Map<String, AtlasEntity> r = this.referredEntities;

            return r != null && guid != null ? r.remove(guid) : null;
        }

        public void updateEntityGuid(String oldGuid, String newGuid) {
            AtlasEntity entity = getEntity(oldGuid);

            if (entity != null) {
                entity.setGuid(newGuid);

                if(removeEntity(oldGuid) != null) {
                    addReferredEntity(newGuid, entity);
                }
            }
        }

        public boolean hasEntity(String guid) {
            return getEntity(guid) != null;
        }

        public void compact() {
            // for derived classes to implement their own logic
        }

        public StringBuilder toString(StringBuilder sb) {
            if (sb == null) {
                sb = new StringBuilder();
            }

            sb.append("AtlasEntityExtInfo{");
            sb.append("referredEntities={");
            AtlasBaseTypeDef.dumpObjects(referredEntities, sb);
            sb.append("}");
            sb.append("}");

            return sb;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AtlasEntityExtInfo that = (AtlasEntityExtInfo) o;
            return Objects.equals(referredEntities, that.referredEntities);
        }

        @Override
        public int hashCode() {
            return Objects.hash(referredEntities);
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }
    }

    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasEntityWithExtInfo extends AtlasEntityExtInfo {
        private static final long serialVersionUID = 1L;

        private AtlasEntity entity;

        public AtlasEntityWithExtInfo() {
            this(null, null);
        }

        public AtlasEntityWithExtInfo(AtlasEntity entity) {
            this(entity, null);
        }

        public AtlasEntityWithExtInfo(AtlasEntity entity, AtlasEntityExtInfo extInfo) {
            super(extInfo);

            this.entity = entity;
        }

        public AtlasEntity getEntity() { return entity; }

        public void setEntity(AtlasEntity entity) { this.entity = entity; }

        @JsonIgnore
        @Override
        public AtlasEntity getEntity(String guid) {
            AtlasEntity ret = super.getEntity(guid);

            if (ret == null && entity != null) {
                if (StringUtils.equals(guid, entity.getGuid())) {
                    ret = entity;
                }
            }

            return ret;
        }

        @JsonIgnore
        @Override
        public void compact() {
            super.compact();

            // remove 'entity' from referredEntities
            if (entity != null) {
                removeEntity(entity.getGuid());
            }
        }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            if (sb == null) {
                sb = new StringBuilder();
            }

            sb.append("AtlasEntityWithExtInfo{");
            sb.append("entity=").append(entity).append(",");
            super.toString(sb);
            sb.append("}");

            return sb;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AtlasEntityWithExtInfo that = (AtlasEntityWithExtInfo) o;
            return Objects.equals(entity, that.entity);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), entity);
        }
    }

    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasEntitiesWithExtInfo extends AtlasEntityExtInfo {
        private static final long serialVersionUID = 1L;

        private List<AtlasEntity> entities;

        public AtlasEntitiesWithExtInfo() {
            this(null, null);
        }

        public AtlasEntitiesWithExtInfo(AtlasEntity entity) { this(Arrays.asList(entity), null);
        }

        public AtlasEntitiesWithExtInfo(List<AtlasEntity> entities) {
            this(entities, null);
        }

        public AtlasEntitiesWithExtInfo(AtlasEntityWithExtInfo entity) {
            this(Arrays.asList(entity.getEntity()), entity);
        }

        public AtlasEntitiesWithExtInfo(List<AtlasEntity> entities, AtlasEntityExtInfo extInfo) {
            super(extInfo);

            this.entities = entities;
        }

        public List<AtlasEntity> getEntities() { return entities; }

        public void setEntities(List<AtlasEntity> entities) { this.entities = entities; }

        @JsonIgnore
        @Override
        public AtlasEntity getEntity(String guid) {
            AtlasEntity ret = super.getEntity(guid);

            if (ret == null && CollectionUtils.isNotEmpty(entities)) {
                for (AtlasEntity entity : entities) {
                    if (StringUtils.equals(guid, entity.getGuid())) {
                        ret = entity;

                        break;
                    }
                }
            }

            return ret;
        }

        public void addEntity(AtlasEntity entity) {
            List<AtlasEntity> entities = this.entities;

            if (entities == null) {
                entities = new ArrayList<>();

                this.entities = entities;
            }

            entities.add(entity);
        }

        public void removeEntity(AtlasEntity entity) {
            List<AtlasEntity> entities = this.entities;

            if (entity != null && entities != null) {
                entities.remove(entity);
            }
        }

        @Override
        public void compact() {
            super.compact();

            // remove 'entities' from referredEntities
            if (CollectionUtils.isNotEmpty(entities)) {
                for (AtlasEntity entity : entities) {
                    removeReferredEntity(entity.getGuid());
                }
            }
        }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            if (sb == null) {
                sb = new StringBuilder();
            }

            sb.append("AtlasEntitiesWithExtInfo{");
            sb.append("entities=[");
            AtlasBaseTypeDef.dumpObjects(entities, sb);
            sb.append("],");
            super.toString(sb);
            sb.append("}");

            return sb;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AtlasEntitiesWithExtInfo that = (AtlasEntitiesWithExtInfo) o;
            return Objects.equals(entities, that.entities);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), entities);
        }
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
}

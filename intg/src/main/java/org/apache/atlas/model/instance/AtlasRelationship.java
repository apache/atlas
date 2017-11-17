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


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.atlas.model.typedef.AtlasRelationshipDef;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * Atlas relationship instance.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasRelationship extends AtlasStruct implements Serializable {
    private static final long serialVersionUID = 1L;

    private String        guid       = null;
    private AtlasObjectId end1       = null;
    private AtlasObjectId end2       = null;
    private String        label      = null;
    private Status        status     = Status.ACTIVE;
    private String        createdBy  = null;
    private String        updatedBy  = null;
    private Date          createTime = null;
    private Date          updateTime = null;
    private Long          version    = 0L;

    public enum Status { ACTIVE, DELETED }

    @JsonIgnore
    private static AtomicLong s_nextId = new AtomicLong(System.nanoTime());

    public AtlasRelationship() {
        super();

        init();
    }

    public AtlasRelationship(String typeName) {
        this(typeName, null);
    }

    public AtlasRelationship(String typeName, Map<String, Object> attributes) {
        super(typeName, attributes);

        init();
    }

    public AtlasRelationship(String typeName, AtlasObjectId end1, AtlasObjectId end2) {
        super(typeName);

        init(nextInternalId(), end1, end2, null, null, null, null, null, null, 0L);
    }

    public AtlasRelationship(String typeName, AtlasObjectId end1, AtlasObjectId end2, Map<String, Object> attributes) {
        super(typeName, attributes);

        init(nextInternalId(), end1, end2, null, null, null, null, null, null, 0L);
    }

    public AtlasRelationship(String typeName, String attrName, Object attrValue) {
        super(typeName, attrName, attrValue);

        init();
    }

    public AtlasRelationship(AtlasRelationshipDef relationshipDef) {
        this(relationshipDef != null ? relationshipDef.getName() : null);
    }

    public AtlasRelationship(AtlasRelationship other) {
        super(other);

        if (other != null) {
            init(other.guid, other.end1, other.end2, other.label, other.status, other.createdBy, other.updatedBy,
                 other.createTime, other.updateTime, other.version);
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

    public AtlasObjectId getEnd1() { return end1; }

    public void setEnd1(AtlasObjectId end1) { this.end1 = end1; }

    public AtlasObjectId getEnd2() { return end2; }

    public void setEnd2(AtlasObjectId end2) { this.end2 = end2; }

    public String getLabel() { return label; }

    public void setLabel(String label) { this.label = label; }

    private static String nextInternalId() {
        return "-" + Long.toString(s_nextId.getAndIncrement());
    }

    private void init() {
        init(nextInternalId(), null, null, null, null, null, null, null, null, 0L);
    }

    private void init(String guid, AtlasObjectId end1, AtlasObjectId end2, String label,
                      Status status, String createdBy, String updatedBy,
                      Date createTime, Date updateTime, Long version) {
        setGuid(guid);
        setEnd1(end1);
        setEnd2(end2);
        setLabel(label);
        setStatus(status);
        setCreatedBy(createdBy);
        setUpdatedBy(updatedBy);
        setCreateTime(createTime);
        setUpdateTime(updateTime);
        setVersion(version);
    }

    @Override
    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasRelationship{");
        super.toString(sb);
        sb.append("guid='").append(guid).append('\'');
        sb.append(", end1=").append(end1);
        sb.append(", end2=").append(end2);
        sb.append(", label='").append(label).append('\'');
        sb.append(", status=").append(status);
        sb.append(", createdBy='").append(createdBy).append('\'');
        sb.append(", updatedBy='").append(updatedBy).append('\'');
        dumpDateField(", createTime=", createTime, sb);
        dumpDateField(", updateTime=", updateTime, sb);
        sb.append(", version=").append(version);
        sb.append('}');

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }

        AtlasRelationship that = (AtlasRelationship) o;
        return Objects.equals(guid, that.guid)             &&
               Objects.equals(end1, that.end1)             &&
               Objects.equals(end2, that.end2)             &&
               Objects.equals(label, that.label)           &&
               status == that.status                       &&
               Objects.equals(createdBy, that.createdBy)   &&
               Objects.equals(updatedBy, that.updatedBy)   &&
               Objects.equals(createTime, that.createTime) &&
               Objects.equals(updateTime, that.updateTime) &&
               Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), guid, end1, end2, label, status, createdBy,
                            updatedBy, createTime, updateTime, version);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }
}
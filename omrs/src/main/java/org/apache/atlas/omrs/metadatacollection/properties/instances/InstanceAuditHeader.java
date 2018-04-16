/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.omrs.metadatacollection.properties.instances;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Date;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * InstanceAuditHeader manages the attributes that are common to classifications and "proper" instances, ie
 * as entities and relationships.  We need to be able to audit when these fundamental elements change and
 * by whom.  Thus they share this header.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public abstract class InstanceAuditHeader extends InstanceElementHeader
{
    /*
     * Summary information about this element's type
     */
    protected InstanceType              type = null;

    /*
     * Standard header information for a classification, entity and relationship.
     */
    protected String                    createdBy       = null;
    protected String                    updatedBy       = null;
    protected Date                      createTime      = null;
    protected Date                      updateTime      = null;
    protected long                      version         = 0L;

    protected InstanceStatus            currentStatus   = InstanceStatus.UNKNOWN;

    /*
     * Used only if the status is DELETED.  It defines the status to use if the instance is restored.
     */
    protected InstanceStatus            statusOnDelete  = InstanceStatus.UNKNOWN;


    /**
     * Default Constructor sets the instance to nulls.
     */
    public InstanceAuditHeader()
    {
        super();
    }


    /**
     * Copy/clone constructor set the value to those supplied in the template.
     *
     * @param template - Instance header
     */
    public InstanceAuditHeader(InstanceAuditHeader template)
    {
        super(template);

        if (template != null)
        {
            this.type = template.getType();
            this.createdBy = template.getCreatedBy();
            this.updatedBy = template.getUpdatedBy();
            this.createTime = template.getCreateTime();
            this.updateTime = template.getUpdateTime();
            this.version = template.getVersion();
            this.currentStatus = template.getStatus();
            this.statusOnDelete = template.getStatusOnDelete();
        }
    }


    /**
     * Return the type of this instance.  This identifies the type definition (TypeDef) that determines its properties.
     *
     * @return InstanceType object
     */
    public InstanceType getType()
    {
        if (type == null)
        {
            return type;
        }
        else
        {
            return new InstanceType(type);
        }
    }


    /**
     * Set up the type of this instance.  This identifies the type definition (TypeDef) that determines its properties.
     *
     * @param type - InstanceType object
     */
    public void setType(InstanceType type)
    {
        this.type = type;
    }


    /**
     * Return the status of this instance (UNKNOWN, PROPOSED, DRAFT, ACTIVE, DELETED).
     *
     * @return InstanceStatus
     */
    public InstanceStatus getStatus() { return currentStatus; }


    /**
     * Set up the status of this instance (UNKNOWN, PROPOSED, DRAFT, ACTIVE, DELETED).
     *
     * @param newStatus - InstanceStatus
     */
    public void setStatus(InstanceStatus newStatus) { this.currentStatus = newStatus; }


    /**
     * Return the name of the user that created this instance.
     *
     * @return String user name
     */
    public String getCreatedBy() { return createdBy; }


    /**
     * Set up the name of the user that created this instance.
     *
     * @param createdBy String user name
     */
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }


    /**
     * Return the name of the user that last updated this instance.
     *
     * @return String user name
     */
    public String getUpdatedBy() { return updatedBy; }


    /**
     * Set up the name of the user that last updated this instance.
     *
     * @param updatedBy - String user name
     */
    public void setUpdatedBy(String updatedBy) { this.updatedBy = updatedBy; }


    /**
     * Return the date/time that this instance was created.
     *
     * @return Date creation time
     */
    public Date getCreateTime()
    {
        if (createTime == null)
        {
            return createTime;
        }
        else
        {
            return new Date(createTime.getTime());
        }
    }


    /**
     * Set up the time that this instance was created.
     *
     * @param createTime Date of creation
     */
    public void setCreateTime(Date createTime) { this.createTime = createTime; }


    /**
     * Return what was the late time this instance was updated.
     *
     * @return Date - last update time
     */
    public Date getUpdateTime()
    {
        if (updateTime == null)
        {
            return updateTime;
        }
        else
        {
            return new Date(updateTime.getTime());
        }
    }


    /**
     * Set up the last update time for this instance.
     *
     * @param updateTime - Date - last update time
     */
    public void setUpdateTime(Date updateTime) { this.updateTime = updateTime; }


    /**
     * Return the version number for this instance.
     *
     * @return Long version number
     */
    public long getVersion() { return version; }


    /**
     * Set up the version number for this instance.
     *
     * @param version - Long version number
     */
    public void setVersion(long version) { this.version = version; }


    /**
     * Return the status to use when a deleted instance is restored.  UNKNOWN is used whenever the instance is
     * not in DELETED status.
     *
     * @return InstanceStatus
     */
    public InstanceStatus getStatusOnDelete() { return statusOnDelete; }


    /**
     * Set up the status to use when a deleted instance is restored.  UNKNOWN is used whenever the instance is
     * not in DELETED status.
     *
     * @param statusOnDelete - InstanceStatus Enum
     */
    public void setStatusOnDelete(InstanceStatus statusOnDelete) { this.statusOnDelete = statusOnDelete; }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "InstanceHeader{" +
                "type=" + type +
                ", status=" + currentStatus +
                ", createdBy='" + createdBy + '\'' +
                ", updatedBy='" + updatedBy + '\'' +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                ", version=" + version +
                ", statusOnDelete=" + statusOnDelete +
                '}';
    }
}

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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * InstanceHeader manages the attributes that are common to entities and relationship instances.  This includes
 * information abut its type, provenance and change history.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public abstract class InstanceHeader extends InstanceAuditHeader
{
    /*
     * Provenance information defining where the instance came from and whether this is a master or reference copy.
     */
    private InstanceProvenanceType    instanceProvenanceType = InstanceProvenanceType.UNKNOWN;
    private String                    metadataCollectionId   = null;

    /*
     * Entities and relationships have unique identifiers.
     */
    private String                    guid            = null;

    /*
     * Some metadata repositories offer a direct URL to access the instance.
     */
    private String                    instanceURL            = null;

    /**
     * Default Constructor sets the instance to nulls.
     */
    public InstanceHeader()
    {
        super();
    }


    /**
     * Copy/clone constructor set the value to those supplied in the template.
     *
     * @param template - Instance header
     */
    public InstanceHeader(InstanceHeader    template)
    {
        super(template);

        if (template != null)
        {
            this.metadataCollectionId = template.getMetadataCollectionId();
            this.instanceProvenanceType = template.getInstanceProvenanceType();
            this.guid = template.getGUID();
            this.instanceURL = template.getInstanceURL();
        }
    }


    /**
     * Return the type of the provenance for this instance (UNKNOWN, LOCAL_COHORT, EXPORT_ARCHIVE, CONTENT_PACK,
     * DEREGISTERED_REPOSITORY, CONFIGURATION).
     *
     * @return InstanceProvenanceType enum
     */
    public InstanceProvenanceType getInstanceProvenanceType() { return instanceProvenanceType; }


    /**
     * Set up the type of the provenance for this instance (UNKNOWN, LOCAL_COHORT, EXPORT_ARCHIVE, CONTENT_PACK,
     * DEREGISTERED_REPOSITORY, CONFIGURATION).
     *
     * @param instanceProvenanceType - InstanceProvenanceType enum
     */
    public void setInstanceProvenanceType(InstanceProvenanceType instanceProvenanceType)
    {
        this.instanceProvenanceType = instanceProvenanceType;
    }


    /**
     * Return the unique identifier for the metadata collection that is the home for this instance.
     * If the metadataCollectionId is null it means this instance belongs to the local metadata collection.
     *
     * @return metadataCollectionId - String unique identifier for the repository
     */
    public String getMetadataCollectionId() { return metadataCollectionId; }


    /**
     * Set up the unique identifier for the home metadata collection for this instance.
     * If the metadataCollectionId is null it means this instance belongs to the local metadata collection.
     *
     * @param metadataCollectionId - String unique identifier for the repository
     */
    public void setMetadataCollectionId(String metadataCollectionId) { this.metadataCollectionId = metadataCollectionId; }


    /**
     * Return the URL for this instance (or null if the metadata repository does not support instance URLs).
     *
     * @return String URL
     */
    public String getInstanceURL()
    {
        return instanceURL;
    }


    /**
     * Set up the URL for this instance (or null if the metadata repository does not support instance URLs).
     *
     * @param instanceURL - String URL
     */
    public void setInstanceURL(String instanceURL)
    {
        this.instanceURL = instanceURL;
    }


    /**
     * Return the unique identifier for this instance.
     *
     * @return guid - String unique identifier
     */
    public String getGUID() { return guid; }


    /**
     * Set up the unique identifier for this instance.
     *
     * @param guid - String unique identifier
     */
    public void setGUID(String guid) { this.guid = guid; }


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
                ", instanceProvenanceType=" + instanceProvenanceType +
                ", metadataCollectionId='" + metadataCollectionId + '\'' +
                ", instanceURL='" + instanceURL + '\'' +
                ", currentStatus=" + currentStatus +
                ", guid='" + guid + '\'' +
                ", createdBy='" + createdBy + '\'' +
                ", updatedBy='" + updatedBy + '\'' +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                ", version=" + version +
                ", statusOnDelete=" + statusOnDelete +
                ", GUID='" + getGUID() + '\'' +
                '}';
    }
}

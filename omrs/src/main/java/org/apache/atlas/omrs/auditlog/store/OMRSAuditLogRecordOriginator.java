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
package org.apache.atlas.omrs.auditlog.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * OMRSAuditLogRecordOriginator describes the server that originated an audit log record.  This is useful if
 * an organization is aggregating messages from different servers together.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class OMRSAuditLogRecordOriginator
{
    private String                   metadataCollectionId = null;
    private String                   serverName           = null;
    private String                   serverType           = null;
    private String                   organizationName     = null;

    /**
     * Default constructor used by parsing engines and other consumers.
     */
    public OMRSAuditLogRecordOriginator()
    {
    }


    /**
     * Returns the unique identifier (guid) of the originating repository's metadata collection.
     *
     * @return String guid
     */
    public String getMetadataCollectionId()
    {
        return metadataCollectionId;
    }


    /**
     * Sets up the unique identifier (guid) of the originating repository.
     *
     * @param metadataCollectionId - String guid
     */
    public void setMetadataCollectionId(String metadataCollectionId)
    {
        this.metadataCollectionId = metadataCollectionId;
    }


    /**
     * Return the display name for the server that is used in events, messages and UIs to
     * make it easier for people to understand the origin of metadata.
     *
     * @return String server name
     */
    public String getServerName()
    {
        return serverName;
    }


    /**
     * Set up the display name for the server that is used in events, messages and UIs to
     * make it easier for people to understand the origin of metadata.
     *
     * @param serverName - String server name
     */
    public void setServerName(String serverName)
    {
        this.serverName = serverName;
    }


    /**
     * Return the descriptive string describing the type of the server.  This might be the
     * name of the product, or similar identifier.
     *
     * @return String server type
     */
    public String getServerType()
    {
        return serverType;
    }


    /**
     * Set up the descriptive string describing the type of the server.  This might be the
     * name of the product, or similar identifier.
     *
     * @param serverType - String server type
     */
    public void setServerType(String serverType)
    {
        this.serverType = serverType;
    }


    /**
     * Return the name of the organization that runs/owns the server.
     *
     * @return String organization name
     */
    public String getOrganizationName()
    {
        return organizationName;
    }


    /**
     * Set up the name of the organization that runs/owns the server.
     *
     * @param organizationName - String organization name
     */
    public void setOrganizationName(String organizationName)
    {
        this.organizationName = organizationName;
    }


    @Override
    public String toString()
    {
        return  "Originator { " +
                "metadataCollectionId : " + this.metadataCollectionId + ", " +
                "serverName : " + this.serverName + ", " +
                "serverType : " + this.serverType + ", " +
                "organizationName : " + this.organizationName + " }";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        OMRSAuditLogRecordOriginator that = (OMRSAuditLogRecordOriginator) o;

        if (metadataCollectionId != null ? !metadataCollectionId.equals(that.metadataCollectionId) : that.metadataCollectionId != null)
        {
            return false;
        }
        if (serverName != null ? !serverName.equals(that.serverName) : that.serverName != null)
        {
            return false;
        }
        if (serverType != null ? !serverType.equals(that.serverType) : that.serverType != null)
        {
            return false;
        }
        return organizationName != null ? organizationName.equals(that.organizationName) : that.organizationName == null;
    }

    @Override
    public int hashCode()
    {
        int result = metadataCollectionId != null ? metadataCollectionId.hashCode() : 0;
        result = 31 * result + (serverName != null ? serverName.hashCode() : 0);
        result = 31 * result + (serverType != null ? serverType.hashCode() : 0);
        result = 31 * result + (organizationName != null ? organizationName.hashCode() : 0);
        return result;
    }
}

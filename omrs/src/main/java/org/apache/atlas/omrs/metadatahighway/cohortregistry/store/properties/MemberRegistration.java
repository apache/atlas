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
package org.apache.atlas.omrs.metadatahighway.cohortregistry.store.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.ocf.properties.beans.Connection;

import java.io.Serializable;
import java.util.Date;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * MemberRegistration is a POJO for storing the information about a metadata repository that is a member
 * of the open metadata repository cohort. This information is saved to disk by the
 * OMRSCohortRegistryStore.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class MemberRegistration implements Serializable
{
    private static final long serialVersionUID = 1L;

    /*
     * Information about a metadata repository that is a member of the metadata repository cluster
     */
    private String     metadataCollectionId = null;
    private String     serverName           = null;
    private String     serverType           = null;
    private String     organizationName     = null;
    private Date       registrationTime     = null;
    private Connection repositoryConnection = null;


    /**
     * Default constructor - initialize registration information to null.
     */
    public MemberRegistration()
    {
        /*
         * Nothing to do
         */
    }


    /**
     * Copy/clone constructor - copy registration information from the template.
     *
     * @param template - MemberRegistration properties to copy
     */
    public MemberRegistration(MemberRegistration template)
    {
        if (template != null)
        {
            metadataCollectionId  = template.getMetadataCollectionId();
            serverName            = template.getServerName();
            serverType            = template.getServerType();
            organizationName      = template.getOrganizationName();
            registrationTime      = template.getRegistrationTime();
            repositoryConnection  = template.getRepositoryConnection();

        }
    }


    /**
     * Return the unique identifier of the repository's metadata collection id.
     *
     * @return String metadata collection id
     */
    public String getMetadataCollectionId() { return metadataCollectionId; }


    /**
     * Set up the unique identifier of the repository's metadata collection id.
     *
     * @param metadataCollectionId - String guid
     */
    public void setMetadataCollectionId(String metadataCollectionId) { this.metadataCollectionId = metadataCollectionId; }


    /**
     * Return the display name for the server.  It is not guaranteed to be unique - just confusing for
     * administrators if it is different.  The display name can change over time with no loss of data integrity.
     *
     * @return String display name
     */
    public String getServerName()
    {
        return serverName;
    }


    /**
     * Set up the display name for the server.  It is not guaranteed to be unique - just confusing for
     * administrators if it is different.  The display name can change over time with no loss of data integrity.
     *
     * @param serverName - String display name
     */
    public void setServerName(String serverName)
    {
        this.serverName = serverName;
    }


    /**
     * Return the type of server.
     *
     * @return String server type
     */
    public String getServerType()
    {
        return serverType;
    }


    /**
     * Set up the type of server.
     *
     * @param serverType - String server type
     */
    public void setServerType(String serverType)
    {
        this.serverType = serverType;
    }


    /**
     * Return the name of the organization.
     *
     * @return String name of the organization
     */
    public String getOrganizationName()
    {
        return organizationName;
    }


    /**
     * Set up the name of the organization.
     *
     * @param organizationName - String name of the organization
     */
    public void setOrganizationName(String organizationName)
    {
        this.organizationName = organizationName;
    }


    /**
     * Return the time that this repository registered with the cluster. (Or null if it has not yet registered.)
     *
     * @return Date object representing the registration time stamp
     */
    public Date getRegistrationTime()
    {
        return registrationTime;
    }


    /**
     * Set up the time that this repository registered with the cluster. (Or null if it has not yet registered.)
     *
     * @param registrationTime - Date object representing the registration time stamp
     */
    public void setRegistrationTime(Date registrationTime) { this.registrationTime = registrationTime; }


    /**
     * Return the connection information for a connector that enables remote calls to the repository server.
     *
     * @return Connection object containing the properties of the connection
     */
    public Connection getRepositoryConnection()
    {
        if (repositoryConnection == null)
        {
            return repositoryConnection;
        }
        else
        {
            return new Connection(repositoryConnection);
        }
    }


    /**
     * Set up the connection information for a connector that enables remote calls to the repository server.
     *
     * @param repositoryConnection - Connection object containing the properties of the connection
     */
    public void setRepositoryConnection(Connection repositoryConnection)
    {
        this.repositoryConnection = repositoryConnection;
    }
}

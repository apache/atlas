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
package org.apache.atlas.omrs.admin.properties;

import org.apache.atlas.ocf.properties.Connection;

/**
 * EnterpriseAccessConfig describes the properties that control the enterprise access services that the
 * OMRS provides to the Open Metadata Access Services (OMASs).
 * <ul>
 *     <li>
 *         enterpriseMetadataCollectionName - name of the combined metadata collection covered by the connected open
 *                                        metadata repositories.  Used for messages.
 *     </li>
 *     <li>
 *         enterpriseMetadataCollectionId - unique identifier for the combined metadata collection covered by the
 *                                      connected open metadata repositories.
 *     </li>
 *     <li>
 *         enterpriseOMRSTopicConnection - connection for the enterprise OMRS Topic connector.
 *     </li>
 * </ul>
 */
public class EnterpriseAccessConfig
{
    private String                           enterpriseMetadataCollectionName   = null;
    private String                           enterpriseMetadataCollectionId     = null;
    private Connection                       enterpriseOMRSTopicConnection      = null;
    private OpenMetadataEventProtocolVersion enterpriseOMRSTopicProtocolVersion = null;


    /**
     * Default Constructor does nothing.
     */
    public EnterpriseAccessConfig()
    {
    }


    /**
     * Constructor to set up all configuration values.
     *
     * @param enterpriseMetadataCollectionName - name of the combined metadata collection covered by the connected open
     *                                        metadata repositories.  Used for messages.
     * @param enterpriseMetadataCollectionId - unique identifier for the combined metadata collection covered by the
     *                                      connected open metadata repositories.
     * @param enterpriseOMRSTopicConnection - connection for the OMRS Topic connector.
     * @param enterpriseOMRSTopicProtocolVersion - protocol versionName enum
     */
    public EnterpriseAccessConfig(String                           enterpriseMetadataCollectionName,
                                  String                           enterpriseMetadataCollectionId,
                                  Connection                       enterpriseOMRSTopicConnection,
                                  OpenMetadataEventProtocolVersion enterpriseOMRSTopicProtocolVersion)
    {
        this.enterpriseMetadataCollectionName = enterpriseMetadataCollectionName;
        this.enterpriseMetadataCollectionId = enterpriseMetadataCollectionId;
        this.enterpriseOMRSTopicConnection = enterpriseOMRSTopicConnection;
        this.enterpriseOMRSTopicProtocolVersion = enterpriseOMRSTopicProtocolVersion;
    }


    /**
     * Return the name of the combined metadata collection covered by the connected open
     * metadata repositories.  Used for messages.
     *
     * @return String name
     */
    public String getEnterpriseMetadataCollectionName()
    {
        return enterpriseMetadataCollectionName;
    }


    /**
     * Set up the name of the combined metadata collection covered by the connected open
     * metadata repositories.  Used for messages.
     *
     * @param enterpriseMetadataCollectionName - String name
     */
    public void setEnterpriseMetadataCollectionName(String enterpriseMetadataCollectionName)
    {
        this.enterpriseMetadataCollectionName = enterpriseMetadataCollectionName;
    }


    /**
     * Return the unique identifier for the combined metadata collection covered by the
     * connected open metadata repositories.
     *
     * @return Unique identifier (guid)
     */
    public String getEnterpriseMetadataCollectionId()
    {
        return enterpriseMetadataCollectionId;
    }


    /**
     * Set up the unique identifier for the combined metadata collection covered by the
     * connected open metadata repositories.
     *
     * @param enterpriseMetadataCollectionId - Unique identifier (guid)
     */
    public void setEnterpriseMetadataCollectionId(String enterpriseMetadataCollectionId)
    {
        this.enterpriseMetadataCollectionId = enterpriseMetadataCollectionId;
    }


    /**
     * Return the connection for the Enterprise OMRS Topic connector.
     *
     * @return Connection object
     */
    public Connection getEnterpriseOMRSTopicConnection()
    {
        return enterpriseOMRSTopicConnection;
    }


    /**
     * Set up the connection for the Enterprise OMRS Topic connector.
     *
     * @param enterpriseOMRSTopicConnection - Connection object
     */
    public void setEnterpriseOMRSTopicConnection(Connection enterpriseOMRSTopicConnection)
    {
        this.enterpriseOMRSTopicConnection = enterpriseOMRSTopicConnection;
    }


    /**
     * Return the protocol versionName to use on the EnterpriseOMRSTopicConnector.
     *
     * @return protocol versionName enum
     */
    public OpenMetadataEventProtocolVersion getEnterpriseOMRSTopicProtocolVersion()
    {
        return enterpriseOMRSTopicProtocolVersion;
    }


    /**
     * Set up the protocol versionName to use on the EnterpriseOMRSTopicConnector.
     *
     * @param enterpriseOMRSTopicProtocolVersion - protocol versionName enum
     */
    public void setEnterpriseOMRSTopicProtocolVersion(OpenMetadataEventProtocolVersion enterpriseOMRSTopicProtocolVersion)
    {
        this.enterpriseOMRSTopicProtocolVersion = enterpriseOMRSTopicProtocolVersion;
    }
}

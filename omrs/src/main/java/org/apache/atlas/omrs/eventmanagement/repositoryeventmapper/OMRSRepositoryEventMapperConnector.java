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
package org.apache.atlas.omrs.eventmanagement.repositoryeventmapper;

import org.apache.atlas.ocf.ConnectorBase;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.omrs.eventmanagement.OMRSRepositoryEventProcessor;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;

/**
 * OMRSRepositoryEventMapperBase provides a base class for implementors of OMRSRepositoryEventMapper.
 */
public abstract class OMRSRepositoryEventMapperConnector extends ConnectorBase implements OMRSRepositoryEventMapper
{
    protected OMRSRepositoryEventProcessor repositoryEventProcessor  = null;
    protected String                       repositoryEventMapperName = null;
    protected OMRSRepositoryConnector      repositoryConnector       = null;
    protected OMRSRepositoryHelper         repositoryHelper          = null;
    protected OMRSRepositoryValidator      repositoryValidator       = null;
    protected String                       localMetadataCollectionId = null;
    protected String                       localServerName           = null;
    protected String                       localServerType           = null;
    protected String                       localOrganizationName     = null;

    /**
     * Default constructor for OCF ConnectorBase.
     */
    public OMRSRepositoryEventMapperConnector()
    {
        super();
    }


    /**
     * Pass additional information to the connector needed to process events.
     *
     * @param repositoryEventMapperName - repository event mapper name used for the source of the OMRS events.
     * @param repositoryConnector - ths is the connector to the local repository that the event mapper is processing
     *                            events from.  The repository connector is used to retrieve additional information
     *                            necessary to fill out the OMRS Events.
     */
    public void initialize(String                      repositoryEventMapperName,
                           OMRSRepositoryConnector     repositoryConnector)
    {
        this.repositoryEventMapperName = repositoryEventMapperName;
        this.repositoryConnector = repositoryConnector;
    }


    /**
     * Set up a repository helper object for the repository connector to use.
     *
     * @param repositoryHelper - helper object for building TypeDefs and metadata instances.
     */
    public void setRepositoryHelper(OMRSRepositoryHelper repositoryHelper)
    {
        this.repositoryHelper = repositoryHelper;
    }


    /**
     * Set up a repository validator for the repository connector to use.
     *
     * @param repositoryValidator - validator object to check the validity of TypeDefs and metadata instances.
     */
    public void setRepositoryValidator(OMRSRepositoryValidator repositoryValidator)
    {
        this.repositoryValidator = repositoryValidator;
    }


    /**
     * Set up the name of the server where the metadata collection resides.
     *
     * @param serverName - String name
     */
    public void  setServerName(String      serverName)
    {
        this.localServerName = serverName;
    }


    /**
     * Set up the descriptive string describing the type of the server.  This might be the
     * name of the product, or similar identifier.
     *
     * @param serverType - String server type
     */
    public void setServerType(String serverType)
    {
        this.localServerType = serverType;
    }


    /**
     * Set up the name of the organization that runs/owns the server.
     *
     * @param organizationName - String organization name
     */
    public void setOrganizationName(String organizationName)
    {
        this.localOrganizationName = organizationName;
    }


    /**
     * Set up the unique Id for this metadata collection.
     *
     * @param metadataCollectionId - String unique Id
     */
    public void setMetadataCollectionId(String         metadataCollectionId)
    {
        this.localMetadataCollectionId = metadataCollectionId;
    }


    /**
     * Set up the repository event listener for this connector to use.  The connector should pass
     * each type or instance metadata change reported by its metadata repository's metadata on to the
     * repository event listener.
     *
     * @param repositoryEventProcessor - listener responsible for distributing notifications of local
     *                                changes to metadata types and instances to the rest of the
     *                                open metadata repository cluster.
     */
    public void setRepositoryEventProcessor(OMRSRepositoryEventProcessor repositoryEventProcessor)
    {
        this.repositoryEventProcessor = repositoryEventProcessor;
    }


    /**
     * Indicates that the connector is completely configured and can begin processing.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public void start() throws ConnectorCheckedException
    {
        super.start();
    }


    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public  void disconnect() throws ConnectorCheckedException
    {
        super.disconnect();
    }
}

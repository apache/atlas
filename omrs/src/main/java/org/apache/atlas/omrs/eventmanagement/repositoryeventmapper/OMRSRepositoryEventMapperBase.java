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
import org.apache.atlas.omrs.eventmanagement.OMRSRepositoryEventProcessor;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;

/**
 * OMRSRepositoryEventMapperBase provides a base class for implementors of OMRSRepositoryEventMapper.
 */
public abstract class OMRSRepositoryEventMapperBase extends ConnectorBase implements OMRSRepositoryEventMapper
{
    protected OMRSRepositoryEventProcessor repositoryEventProcessor  = null;
    protected String                       repositoryEventMapperName = null;
    protected OMRSRepositoryConnector      repositoryConnector       = null;
    protected OMRSRepositoryHelper         repositoryHelper          = null;
    protected String                       localMetadataCollectionId = null;
    protected String                       localServerName           = null;
    protected String                       localServerType           = null;
    protected String                       localOrganizationName     = null;

    /**
     * Default constructor for OCF ConnectorBase.
     */
    public OMRSRepositoryEventMapperBase()
    {
        super();
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
     * Pass additional information to the connector needed to process events.
     *
     * @param repositoryEventMapperName - repository event mapper name used for the source of the OMRS events.
     * @param repositoryConnector - ths is the connector to the local repository that the event mapper is processing
     *                            events from.  The repository connector is used to retrieve additional information
     *                            necessary to fill out the OMRS Events.
     * @param repositoryHelper - the TypeDef helper is used to create metadata instances that are
     * @param localMetadataCollectionId - unique identifier for the local repository's metadata collection.
     * @param localServerName - name of the local server.
     * @param localServerType - type of local repository/server.
     * @param localOrganizationName - name of the organization that owns the local metadata repository.
     */
    public void initialize(String                      repositoryEventMapperName,
                           OMRSRepositoryConnector     repositoryConnector,
                           OMRSRepositoryHelper        repositoryHelper,
                           String                      localMetadataCollectionId,
                           String                      localServerName,
                           String                      localServerType,
                           String                      localOrganizationName)
    {
        this.repositoryEventMapperName = repositoryEventMapperName;
        this.repositoryConnector = repositoryConnector;
        this.repositoryHelper = repositoryHelper;
        this.localMetadataCollectionId = localMetadataCollectionId;
        this.localServerName = localServerName;
        this.localServerType = localServerType;
        this.localOrganizationName = localOrganizationName;
    }
}

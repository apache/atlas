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

import org.apache.atlas.omrs.eventmanagement.OMRSRepositoryEventProcessor;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;

/**
 * OMRSRepositoryEventMapper is the interface to a connector that is converting events received from
 * a non-native local metadata repository into OMRS compliant repository events.  It is used when the Open Metadata
 * and Governance Server is being used as a RepositoryProxy, or if the local metadata repository has
 * additional APIs that mean metadata can be changed without going through the OMRS Repository Connectors.
 */
public interface OMRSRepositoryEventMapper
{
    /**
     * Pass additional information to the connector needed to process events.
     *
     * @param repositoryEventMapperName - repository event mapper name used for the source of the OMRS events.
     * @param repositoryConnector - this is the connector to the local repository that the event mapper is processing
     *                            events from.  The repository connector is used to retrieve additional information
     *                            necessary to fill out the OMRS Events.
     * @param repositoryHelper - provides helper methods for building TypeDefs and metadata instances such as
     *                         entities and relationships.
     * @param localMetadataCollectionId - unique identifier for the local repository's metadata collection.
     * @param localServerName - name of the local server.
     * @param localServerType - type of local repository/server.
     * @param localOrganizationName - name of the organization that owns the local metadata repository.
     */
    void initialize(String                      repositoryEventMapperName,
                    OMRSRepositoryConnector     repositoryConnector,
                    OMRSRepositoryHelper        repositoryHelper,
                    String                      localMetadataCollectionId,
                    String                      localServerName,
                    String                      localServerType,
                    String                      localOrganizationName);


    /**
     * Set up the repository event processor for this connector to use.  The connector should pass
     * each typeDef or instance metadata change reported by its metadata repository's metadata on to the
     * repository event processor.
     *
     * @param repositoryEventProcessor - listener responsible for distributing notifications of local
     *                                changes to metadata types and instances to the rest of the
     *                                open metadata repository cohort.
     */
    void setRepositoryEventProcessor(OMRSRepositoryEventProcessor repositoryEventProcessor);
}

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
package org.apache.atlas.omrs.enterprise.connectormanager;

import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;

/**
 * OMRSConnectConsumer provides the interfaces for a connector consumer.  This is a component that needs to
 * maintain a current list of connectors to all of the remote repositories in the open metadata repository cohorts that
 * the local server is a member of.
 */
public interface OMRSConnectorConsumer
{
    /**
     * Pass the connector for the local repository to the connector consumer.
     *
     * @param metadataCollectionId - Unique identifier for the metadata collection
     * @param localConnector - OMRSRepositoryConnector object for the local repository.
     */
    void setLocalConnector(String                  metadataCollectionId,
                           OMRSRepositoryConnector localConnector);


    /**
     * Pass the connector to one of the remote repositories in the metadata repository cohort.
     *
     * @param metadataCollectionId - Unique identifier for the metadata collection
     * @param remoteConnector - OMRSRepositoryConnector object providing access to the remote repository.
     */
    void addRemoteConnector(String                  metadataCollectionId,
                            OMRSRepositoryConnector remoteConnector);


    /**
     * Pass the metadata collection id for a repository that has just left the metadata repository cohort.
     *
     * @param metadataCollectionId - identifier of the metadata collection that is no longer available.
     */
    void removeRemoteConnector(String  metadataCollectionId);


    /**
     * Call disconnect on all registered connectors and stop calling them.  The OMRS is about to shutdown.
     */
    void disconnectAllConnectors();
}

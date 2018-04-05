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

import org.apache.atlas.ocf.ffdc.ConnectionCheckedException;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.properties.Connection;

/**
 * OMRSConnectionConsumer provides the interfaces for a connection consumer.  This is a component that needs to
 * maintain a current list of connections to all of the repositories in the open metadata repository cohort.
 */
public interface OMRSConnectionConsumer
{
    /**
     * Pass details of the connection for one of the remote repositories registered in a connected
     * open metadata repository cohort.
     *
     * @param cohortName - name of the cohort adding the remote connection.
     * @param remoteServerName - name of the remote server for this connection.
     * @param remoteServerType - type of the remote server.
     * @param owningOrganizationName - name of the organization the owns the remote server.
     * @param metadataCollectionId - Unique identifier for the metadata collection
     * @param remoteConnection - Connection object providing properties necessary to create an
     *                         OMRSRepositoryConnector for the remote repository.
     * @throws ConnectionCheckedException - there are invalid properties in the Connection
     * @throws ConnectorCheckedException - there is a problem initializing the Connector
     */
    void addRemoteConnection(String         cohortName,
                             String         remoteServerName,
                             String         remoteServerType,
                             String         owningOrganizationName,
                             String         metadataCollectionId,
                             Connection     remoteConnection) throws ConnectionCheckedException, ConnectorCheckedException;


    /**
     * Pass details of the connection for the repository that has left one of the open metadata repository cohorts.
     *
     * @param cohortName - name of the cohort removing the remote connection.
     * @param metadataCollectionId - Unique identifier for the metadata collection.
     */
    void removeRemoteConnection(String         cohortName,
                                String         metadataCollectionId);


    /**
     * Remove all of the remote connections for the requested open metadata repository cohort.
     *
     * @param cohortName - name of the cohort
     */
    void removeCohort(String   cohortName);
}

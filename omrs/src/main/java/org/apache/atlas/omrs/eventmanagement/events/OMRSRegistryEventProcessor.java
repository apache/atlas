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
package org.apache.atlas.omrs.eventmanagement.events;


import org.apache.atlas.ocf.properties.beans.Connection;

import java.util.Date;

/**
 * OMRSRegistryEventProcessor is an interface implemented by a component that is able to process
 * registry events for an Open Metadata Repository's membership of an Open Metadata Repository Cohort.
 */
public interface OMRSRegistryEventProcessor
{
    /**
     * Introduces a new server/repository to the metadata repository cohort.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection that is registering with the cohort.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param registrationTimestamp - the time that the server/repository issued the registration request.
     * @param remoteConnection - the Connection properties for the connector used to call the registering server.
     * @return flag indicating if the event was sent or not.
     */
    boolean processRegistrationEvent(String                    sourceName,
                                     String                    originatorMetadataCollectionId,
                                     String                    originatorServerName,
                                     String                    originatorServerType,
                                     String                    originatorOrganizationName,
                                     Date                      registrationTimestamp,
                                     Connection                remoteConnection);


    /**
     * Requests that the other servers in the cohort send re-registration events.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @return flag indicating if the event was sent or not.
     */
    boolean processRegistrationRefreshRequest(String                    sourceName,
                                              String                    originatorServerName,
                                              String                    originatorServerType,
                                              String                    originatorOrganizationName);


    /**
     * Refreshes the other servers in the cohort with the originating server's registration.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection that is registering with the cohort.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param registrationTimestamp - the time that the server/repository first registered with the cohort.
     * @param remoteConnection - the Connection properties for the connector used to call the registering server.
     * @return flag indicating if the event was sent or not.
     */
    boolean processReRegistrationEvent(String                    sourceName,
                                       String                    originatorMetadataCollectionId,
                                       String                    originatorServerName,
                                       String                    originatorServerType,
                                       String                    originatorOrganizationName,
                                       Date                      registrationTimestamp,
                                       Connection                remoteConnection);


    /**
     * A server/repository is being removed from the metadata repository cohort.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - metadata collectionId of originator.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @return flag indicating if the event was sent or not.
     */
    boolean processUnRegistrationEvent(String                    sourceName,
                                       String                    originatorMetadataCollectionId,
                                       String                    originatorServerName,
                                       String                    originatorServerType,
                                       String                    originatorOrganizationName);


    /**
     * There is more than one member of the open metadata repository cohort that is using the same metadata
     * collection Id.  This means that their metadata instances can be updated in more than one server and their
     * is a potential for data integrity issues.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - metadata collectionId of originator.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param conflictingMetadataCollectionId - unique identifier for the metadata collection that is registering with the cohort.
     * @param errorMessage - details of the conflict
     */
    void processConflictingCollectionIdEvent(String  sourceName,
                                             String  originatorMetadataCollectionId,
                                             String  originatorServerName,
                                             String  originatorServerType,
                                             String  originatorOrganizationName,
                                             String  conflictingMetadataCollectionId,
                                             String  errorMessage);


    /**
     * A connection to one of the members of the open metadata repository cohort is not usable by one of the members.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - metadata collectionId of originator.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param targetMetadataCollectionId - Id for the repository with the bad remote connection.
     * @param remoteConnection - the Connection properties for the connector used to call the registering server.
     * @param errorMessage - details of the error that occurs when the connection is used.
     */
    void processBadConnectionEvent(String     sourceName,
                                   String     originatorMetadataCollectionId,
                                   String     originatorServerName,
                                   String     originatorServerType,
                                   String     originatorOrganizationName,
                                   String     targetMetadataCollectionId,
                                   Connection remoteConnection,
                                   String     errorMessage);
}

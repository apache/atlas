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

/**
 * OMRSInstanceEventType defines the different types of instance events in the open metadata repository services
 * protocol.
 */
public enum OMRSInstanceEventType
{
    UNKNOWN_INSTANCE_EVENT          (0,  "UnknownInstanceEvent",
                                         "An event that is not recognized by the local server."),
    NEW_ENTITY_EVENT                (1,  "NewEntityEvent",
                                         "A new entity has been created."),
    UPDATED_ENTITY_EVENT            (2,  "UpdatedEntityEvent",
                                         "An existing entity has been updated."),
    UNDONE_ENTITY_EVENT             (3,  "UndoneEntityEvent",
                                         "An update to an entity has been undone."),
    CLASSIFIED_ENTITY_EVENT         (4,  "ClassifiedEntityEvent",
                                         "A new classification has been added to an entity."),
    DECLASSIFIED_ENTITY_EVENT       (5,  "DeclassifiedEntityEvent",
                                         "A classification has been removed from an entity."),
    RECLASSIFIED_ENTITY_EVENT       (6,  "ReclassifiedEntityEvent",
                                         "An existing classification has been changed on an entity."),
    DELETED_ENTITY_EVENT            (7,  "DeletedEntityEvent",
                                         "An existing entity has been deleted.  This is a soft delete. " +
                                          "This means it is still in the repository " +
                                          "but it is no longer returned on queries."),
    PURGED_ENTITY_EVENT             (8,  "PurgedEntityEvent",
                                         "A deleted entity has been permanently removed from the repository. " +
                                          "This request can not be undone."),
    RESTORED_ENTITY_EVENT           (9,  "RestoredEntityEvent",
                                         "A deleted entity has been restored to the state it was before it was deleted."),
    RE_IDENTIFIED_ENTITY_EVENT      (10, "ReIdentifiedEntityEvent",
                                         "The guid of an existing entity has been changed to a new value."),
    RETYPED_ENTITY_EVENT            (11, "ReTypedEntityEvent",
                                         "An existing entity has had its type changed."),
    RE_HOMED_ENTITY_EVENT           (12, "ReHomedEntityEvent",
                                         "An existing entity has changed home repository."),
    REFRESH_ENTITY_REQUEST          (13, "RefreshEntityRequestEvent",
                                         "The local repository is requesting that an entity from another repository's " +
                                          "metadata collection is " +
                                          "refreshed so the local repository can create a reference copy."),
    REFRESHED_ENTITY_EVENT          (14, "RefreshedEntityEvent",
                                         "A remote repository in the cohort has sent entity details in response " +
                                          "to a refresh request."),
    NEW_RELATIONSHIP_EVENT          (15, "NewRelationshipEvent",
                                         "A new relationship has been created."),
    UPDATED_RELATIONSHIP_EVENT      (16, "UpdateRelationshipEvent",
                                         "An existing relationship has been updated."),
    UNDONE_RELATIONSHIP_EVENT       (17, "UndoneRelationshipEvent",
                                         "An earlier change to a relationship has been undone."),
    DELETED_RELATIONSHIP_EVENT      (18, "DeletedRelationshipEvent",
                                         "An existing relationship has been deleted.  This is a soft delete. " +
                                          "This means it is still in the repository " +
                                          "but it is no longer returned on queries."),
    PURGED_RELATIONSHIP_EVENT       (19, "PurgedRelationshipEvent",
                                         "A deleted relationship has been permanently removed from the repository. " +
                                          "This request can not be undone."),
    RESTORED_RELATIONSHIP_EVENT     (20, "RestoredRelationshipEvent",
                                         "A deleted relationship has been restored to the state it was before it was deleted."),
    RE_IDENTIFIED_RELATIONSHIP_EVENT(21, "ReIdentifiedRelationshipEvent",
                                         "The guid of an existing relationship has changed."),
    RETYPED_RELATIONSHIP_EVENT      (22, "ReTypedRelationshipEvent",
                                         "An existing relationship has had its type changed."),
    RE_HOMED_RELATIONSHIP_EVENT     (23, "ReHomedRelationshipEvent",
                                         "An existing relationship has changed home repository."),
    REFRESH_RELATIONSHIP_REQUEST    (24, "RefreshRelationshipRequestEvent",
                                         "A repository has requested the home repository of a relationship send " +
                                                "details of hte relationship so " +
                                                "the local repository can create a reference copy of the instance."),
    REFRESHED_RELATIONSHIP_EVENT    (25, "RefreshedRelationshipEvent",
                                         "The local repository is refreshing the information about a relationship for the " +
                                          "other repositories in the cohort."),
    INSTANCE_ERROR_EVENT            (99, "InstanceErrorEvent",
                                         "An error has been detected in the exchange of instances between members of the cohort.")
    ;


    private  int      eventTypeCode;
    private  String   eventTypeName;
    private  String   eventTypeDescription;


    /**
     * Default Constructor - sets up the specific values for this instance of the enum.
     *
     * @param eventTypeCode - int identifier used for indexing based on the enum.
     * @param eventTypeName - string name used for messages that include the enum.
     * @param eventTypeDescription - default description for the enum value - used when natural resource
     *                                     bundle is not available.
     */
    OMRSInstanceEventType(int eventTypeCode, String eventTypeName, String eventTypeDescription)
    {
        this.eventTypeCode = eventTypeCode;
        this.eventTypeName = eventTypeName;
        this.eventTypeDescription = eventTypeDescription;
    }


    /**
     * Return the int identifier used for indexing based on the enum.
     *
     * @return int identifier code
     */
    public int getInstanceEventTypeCode()
    {
        return eventTypeCode;
    }


    /**
     * Return the string name used for messages that include the enum.
     *
     * @return String name
     */
    public String getInstanceEventTypeName()
    {
        return eventTypeName;
    }


    /**
     * Return the default description for the enum value - used when natural resource
     * bundle is not available.
     *
     * @return String default description
     */
    public String getInstanceEventTypeDescription()
    {
        return eventTypeDescription;
    }
}

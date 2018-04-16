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
 * OMRSTypeDefEventType defines the different types of TypeDef events in the open metadata repository services
 * protocol:
 * <ul>
 *     <li>
 *         UNKNOWN_TYPEDEF_EVENT - the event is not recognized by this local server, probably because it is back-level
 *         from other servers in the cluster.  It is logged in the audit log and then ignored.  The metadata exchange
 *         protocol should evolve so that new message types can be ignored by back-level servers without damage
 *         to the cluster's integrity.
 *     </li>
 *     <li>
 *         NEW_TYPEDEF - A new TypeDef has been defined.
 *     </li>
 *     <li>
 *         UPDATED_TYPEDEF - An existing TypeDef has been updated.
 *     </li>
 *     <li>
 *         DELETED_TYPEDEF_EVENT - An existing TypeDef has been deleted.
 *     </li>
 *     <li>
 *         RE_IDENTIFIED_TYPEDEF_EVENT - the guid has been changed for a TypeDef.
 *     </li>
 * </ul>
 */
public enum OMRSTypeDefEventType
{
    UNKNOWN_TYPEDEF_EVENT                 (0,  "UnknownTypeDefEvent",          "A TypeDef event that is not recognized by the local server."),
    NEW_TYPEDEF_EVENT                     (1,  "NewTypeDef",                   "A new TypeDef has been defined."),
    NEW_ATTRIBUTE_TYPEDEF_EVENT           (2,  "NewAttributeTypeDef",          "A new AttributeTypeDef has been defined."),
    UPDATED_TYPEDEF_EVENT                 (3,  "UpdatedTypeDef",               "An existing TypeDef has been updated."),
    DELETED_TYPEDEF_EVENT                 (4,  "DeletedTypeDef",               "An existing TypeDef has been deleted."),
    DELETED_ATTRIBUTE_TYPEDEF_EVENT       (5,  "DeletedAttributeTypeDef",      "An existing AttributeTypeDef has been deleted."),
    RE_IDENTIFIED_TYPEDEF_EVENT           (6,  "ReIdentifiedTypeDef",          "An existing TypeDef has changed either it guid or its name."),
    RE_IDENTIFIED_ATTRIBUTE_TYPEDEF_EVENT (7,  "ReIdentifiedAttributeTypeDef", "An existing AttributeTypeDef has changed either it guid or its name."),
    TYPEDEF_ERROR_EVENT                   (99, "InstanceErrorEvent",
                                               "An error has been detected in the exchange of TypeDefs between members of the cohort.");


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
    OMRSTypeDefEventType(int eventTypeCode, String eventTypeName, String eventTypeDescription)
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
    public int getTypeDefEventTypeCode()
    {
        return eventTypeCode;
    }


    /**
     * Return the string name used for messages that include the enum.
     *
     * @return String name
     */
    public String getTypeDefEventTypeName()
    {
        return eventTypeName;
    }


    /**
     * Return the default description for the enum value - used when natural resource
     * bundle is not available.
     *
     * @return String default description
     */
    public String getTypeDefEventTypeDescription()
    {
        return eventTypeDescription;
    }
}

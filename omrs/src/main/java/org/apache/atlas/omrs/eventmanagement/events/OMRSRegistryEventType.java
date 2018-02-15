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
 * OMRSRegistryEventType defines the different types of registry events in the open metadata repository services
 * protocol.  There are 4 basic values.
 * <ul>
 *     <li>
 *         UNKNOWN_REGISTRY_EVENT - the event is not recognized by this local server, probably because it is back-level
 *         from other servers in the cohort.  It is logged in the audit log and then ignored.  The registration
 *         protocol should evolve so that new message types can be ignored by back-level servers without damage
 *         to the cohort's integrity.
 *     </li>
 *     <li>
 *         REGISTRATION_EVENT - this event is a server's first contact with the cohort.  It publicises the id of the
 *         local repository, the connection information needed to connect remotely to this local server's
 *         repository (localServerRemoteConnection) plus a list of typedef names and GUIDs.  Other servers in the
 *         cohort will respond either with a RE_REGISTRATION_EVENT if all is ok or REGISTRATION_ERROR if there
 *         is a problem with any information in the REGISTRATION_EVENT.
 *     </li>
 *     <li>
 *         REFRESH_REGISTRATION_EVENT - this event is used after a local server has experienced configuration issues
 *         and needs to receive the registration information from other members of the cohort again.
 *     </li>
 *     <li>
 *         RE_REGISTRATION_EVENT - this event is used by a previously registered server to broadcast its current
 *         configuration.  It is used in response to a REGISTRATION_EVENT and a REFRESH_REGISTRATION_EVENT to
 *         let the other servers know about its configuration, or whenever its configuration may have changed
 *         (such as after a server restart or administrator action).
 *     </li>
 *     <li>
 *         UN_REGISTRATION_EVENT - this event is used when a server is being removed from the cohort.  The other
 *         servers can choose whether to remove its replicated metadata from their repository, or mark it as
 *         deregistered repository's metadata.
 *     </li>
 *     <li>
 *         REGISTRATION_ERROR_EVENT - ths event is used to report anomalies in the registration information being
 *         passed between the repositories and the actions taken.  Each of these errors should be investigated
 *         since they should not occur.
 *     </li>
 * </ul>
 */
public enum OMRSRegistryEventType
{
    UNKNOWN_REGISTRY_EVENT       (0, "UnknownRegistryEvent",
                                     "An event that is not recognized by the local server."),
    REGISTRATION_EVENT           (1, "RegistrationEvent",
                                     "Introduces a new server/repository to the metadata repository cohort."),
    REFRESH_REGISTRATION_REQUEST (2, "RefreshRegistrationRequest",
                                     "Requests that the other servers in the cohort send re-registration events."),
    RE_REGISTRATION_EVENT        (3, "ReRegistrationEvent",
                                     "Refreshes the other servers in the cohort with the local server's configuration."),
    UN_REGISTRATION_EVENT        (4, "UnRegistrationEvent",
                                     "A server/repository is being removed from the metadata repository cohort."),
    REGISTRATION_ERROR_EVENT     (99, "RegistrationErrorEvent",
                                      "An anomaly has been detected in the information received from a member in the metadata repository cohort.");


    private  int      registryEventTypeCode;
    private  String   registryEventTypeName;
    private  String   registryEventTypeDescription;


    /**
     * Default Constructor - sets up the specific values for this instance of the enum.
     *
     * @param registryEventTypeCode - int identifier used for indexing based on the enum.
     * @param registryEventTypeName - string name used for messages that include the enum.
     * @param registryEventTypeDescription - default description for the enum value - used when natural resource
     *                                     bundle is not available.
     */
    OMRSRegistryEventType(int registryEventTypeCode, String registryEventTypeName, String registryEventTypeDescription)
    {
        this.registryEventTypeCode = registryEventTypeCode;
        this.registryEventTypeName = registryEventTypeName;
        this.registryEventTypeDescription = registryEventTypeDescription;
    }


    /**
     * Return the int identifier used for indexing based on the enum.
     *
     * @return int identifier code
     */
    public int getRegistryEventTypeCode()
    {
        return registryEventTypeCode;
    }


    /**
     * Return the string name used for messages that include the enum.
     *
     * @return String name
     */
    public String getRegistryEventTypeName()
    {
        return registryEventTypeName;
    }


    /**
     * Return the default description for the enum value - used when natural resource
     * bundle is not available.
     *
     * @return String default description
     */
    public String getRegistryEventTypeDescription()
    {
        return registryEventTypeDescription;
    }
}

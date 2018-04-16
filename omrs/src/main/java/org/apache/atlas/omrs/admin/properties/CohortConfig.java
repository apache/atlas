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


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * CohortConfig provides the configuration properties used to connect to an open metadata repository cohort.
 * <ul>
 *     <li>
 *         cohortName is a descriptive name for the cohort that is used primarily for messages and diagnostics.
 *         It is also used to create a default name for the cohort's OMRS Topic and the cohortRegistry's store
 *         if these names are not explicitly defined.
 *     </li>
 *     <li>
 *         cohortRegistryConnection is the connection properties necessary to create the connector to the
 *         cohort registry store.  This is the store where the cohort registry keeps information about its
 *         local metadata collection Id and details of other repositories in the cohort.
 *
 *         The default value is to use a local file called "cohort.registry" that is stored in the server's
 *         home directory.
 *     </li>
 *     <li>
 *         cohortOMRSTopicConnection is the connection properties necessary to create the connector to the OMRS Topic.
 *         This is used to send/receive events between members of the open metadata repository cohort.
 *     </li>
 *     <li>
 *         cohortOMRSTopicProtocolVersion defines the version of the event payload to use when communicating with other
 *         members of the cohort through the OMRS Topic.
 *     </li>
 *     <li>
 *         eventsToProcessRule defines how incoming events on the OMRS Topic should be processed.
 *     </li>
 *     <li>
 *         selectedTypesToProcess - list of TypeDefs used if the eventsToProcess rule (above) says
 *         "SELECTED_TYPES" - otherwise it is set to null.
 *     </li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class CohortConfig
{
    private String                           cohortName                     = null;
    private Connection                       cohortRegistryConnection       = null;
    private Connection                       cohortOMRSTopicConnection      = null;
    private OpenMetadataEventProtocolVersion cohortOMRSTopicProtocolVersion = null;
    private OpenMetadataExchangeRule         eventsToProcessRule            = null;
    private ArrayList<TypeDefSummary>        selectedTypesToProcess         = null;



    /**
     * Default constructor does nothing.
     */
    public CohortConfig()
    {
    }


    /**
     * Constructor to populate all config values.
     *
     * @param cohortName - name of the cohort
     * @param cohortRegistryConnection - connection to the cohort registry store
     * @param cohortOMRSTopicConnection - connection to the OMRS Topic
     * @param eventsToProcessRule - rule indicating whether metadata events should be sent to the federated OMRS Topic.
     * @param selectedTypesToProcess - if the rule says "SELECTED_TYPES" then this is the list of types - otherwise
     *                                it is set to null.
     */
    public CohortConfig(String                      cohortName,
                        Connection                  cohortRegistryConnection,
                        Connection                  cohortOMRSTopicConnection,
                        OpenMetadataExchangeRule    eventsToProcessRule,
                        ArrayList<TypeDefSummary>   selectedTypesToProcess)
    {
        this.cohortName = cohortName;
        this.cohortRegistryConnection = cohortRegistryConnection;
        this.cohortOMRSTopicConnection = cohortOMRSTopicConnection;
        this.eventsToProcessRule = eventsToProcessRule;
        this.selectedTypesToProcess = selectedTypesToProcess;
    }


    /**
     * Return the name of the cohort.
     *
     * @return String name
     */
    public String getCohortName()
    {
        return cohortName;
    }


    /**
     * Set up the name of the cohort.
     *
     * @param cohortName String
     */
    public void setCohortName(String cohortName)
    {
        this.cohortName = cohortName;
    }


    /**
     * Set up the connection to the cohort registry store.
     *
     * @return Connection object
     */
    public Connection getCohortRegistryConnection()
    {
        return cohortRegistryConnection;
    }


    /**
     * Set up the connection for the cohort registry store.
     *
     * @param cohortRegistryConnection - Connection object
     */
    public void setCohortRegistryConnection(Connection cohortRegistryConnection)
    {
        this.cohortRegistryConnection = cohortRegistryConnection;
    }


    /**
     * Return the connection to the cohort's OMRS Topic.
     *
     * @return Connection object
     */
    public Connection getCohortOMRSTopicConnection()
    {
        return cohortOMRSTopicConnection;
    }

    /**
     * Set up the connection to the cohort's OMRS Topic.
     *
     * @param cohortOMRSTopicConnection - Connection object
     */
    public void setCohortOMRSTopicConnection(Connection cohortOMRSTopicConnection)
    {
        this.cohortOMRSTopicConnection = cohortOMRSTopicConnection;
    }


    /**
     * Return the protocol version to use when exchanging events amongst the cohort members.
     *
     * @return protocol version enum
     */
    public OpenMetadataEventProtocolVersion getCohortOMRSTopicProtocolVersion()
    {
        return cohortOMRSTopicProtocolVersion;
    }


    /**
     * Set up the protocol version to use when exchanging events amongst the cohort members.
     *
     * @param cohortOMRSTopicProtocolVersion - protocol version enum
     */
    public void setCohortOMRSTopicProtocolVersion(OpenMetadataEventProtocolVersion cohortOMRSTopicProtocolVersion)
    {
        this.cohortOMRSTopicProtocolVersion = cohortOMRSTopicProtocolVersion;
    }


    /**
     * Return the rule indicating whether incoming metadata events from a cohort should be processed.
     *
     * @return OpenMetadataExchangeRule - NONE, JUST_TYPEDEFS, SELECTED_TYPES and ALL.
     */
    public OpenMetadataExchangeRule getEventsToProcessRule()
    {
        return eventsToProcessRule;
    }


    /**
     * Set up the rule indicating whether incoming metadata events from a cohort should be processed.
     *
     * @param eventsToProcessRule - OpenMetadataExchangeRule - NONE, JUST_TYPEDEFS, SELECTED_TYPES and ALL.
     */
    public void setEventsToProcessRule(OpenMetadataExchangeRule eventsToProcessRule)
    {
        this.eventsToProcessRule = eventsToProcessRule;
    }


    /**
     * Return the list of TypeDefs used if the eventsToProcess rule (above) says "SELECTED_TYPES" - otherwise
     * it is set to null.
     *
     * @return list of TypeDefs that determine which metadata instances to process
     */
    public List<TypeDefSummary> getSelectedTypesToProcess()
    {
        if (selectedTypesToProcess == null)
        {
            return null;
        }
        else
        {
            return selectedTypesToProcess;
        }
    }


    /**
     * Set up the list of TypeDefs used if the EventsToProcess rule (above) says "SELECTED_TYPES" - otherwise
     * it is set to null.
     *
     * @param selectedTypesToProcess - list of TypeDefs that determine which metadata instances to process
     */
    public void setSelectedTypesToProcess(List<TypeDefSummary> selectedTypesToProcess)
    {
        if (selectedTypesToProcess == null)
        {
            this.selectedTypesToProcess = null;
        }
        else
        {
            this.selectedTypesToProcess = new ArrayList<>(selectedTypesToProcess);
        }
    }
}

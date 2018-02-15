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
package org.apache.atlas.omrs.auditlog;

import org.apache.atlas.omrs.admin.OMRSConfigurationFactory;
import org.apache.atlas.omrs.admin.OMRSOperationalServices;
import org.apache.atlas.omrs.archivemanager.OMRSArchiveManager;
import org.apache.atlas.omrs.enterprise.connectormanager.OMRSEnterpriseConnectorManager;
import org.apache.atlas.omrs.enterprise.repositoryconnector.EnterpriseOMRSRepositoryConnector;
import org.apache.atlas.omrs.eventmanagement.OMRSRepositoryEventManager;
import org.apache.atlas.omrs.localrepository.repositoryconnector.LocalOMRSInstanceEventProcessor;
import org.apache.atlas.omrs.localrepository.repositoryconnector.LocalOMRSRepositoryConnector;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryContentManager;
import org.apache.atlas.omrs.metadatahighway.OMRSMetadataHighwayManager;
import org.apache.atlas.omrs.metadatahighway.cohortregistry.OMRSCohortRegistry;
import org.apache.atlas.omrs.metadatahighway.cohortregistry.store.OMRSCohortRegistryStore;
import org.apache.atlas.omrs.eventmanagement.OMRSEventListener;
import org.apache.atlas.omrs.eventmanagement.OMRSEventPublisher;
import org.apache.atlas.omrs.metadatahighway.OMRSCohortManager;
import org.apache.atlas.omrs.rest.repositoryconnector.OMRSRESTRepositoryConnector;
import org.apache.atlas.omrs.rest.server.OMRSRepositoryRESTServices;
import org.apache.atlas.omrs.topicconnectors.OMRSTopicConnector;


/**
 * OMRSAuditingComponent provides identifying and background information about the components writing log records
 * to the OMRS Audit log.  This is to help someone reading the OMRS Audit Log understand the records.
 */
public enum OMRSAuditingComponent
{
    UNKNOWN (0,
             "<Unknown>", "Uninitialized component name", null, null),

    AUDIT_LOG (1,
             "Audit Log",
             "Reads and writes records to the Open Metadata Repository Services (OMRS) audit log.",
             OMRSAuditLog.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Audit+Log"),

    CONFIGURATION_FACTORY (2,
             "Configuration Factory",
             "Generates default values for the Open Metadata Repository Services (OMRS) configuration.",
             OMRSConfigurationFactory.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Configuration+Factory"),

    OPERATIONAL_SERVICES (3,
             "Operational Services",
             "Supports the administration services for the Open Metadata Repository Services (OMRS).",
             OMRSOperationalServices.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Operational+Services"),

    ARCHIVE_MANAGER (4,
             "Archive Manager",
             "Manages the loading of Open Metadata Archives into an open metadata repository.",
             OMRSArchiveManager.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Archive+Manager"),

    ENTERPRISE_CONNECTOR_MANAGER (5,
             "Enterprise Connector Manager",
             "Manages the list of open metadata repositories that the Enterprise OMRS Repository Connector " +
                                          "should call to retrieve an enterprise view of the metadata collections " +
                                          "supported by these repositories",
             OMRSEnterpriseConnectorManager.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Enterprise+Connector+Manager"),

    ENTERPRISE_REPOSITORY_CONNECTOR (6,
             "Enterprise Repository Connector",
             "Supports enterprise access to the list of open metadata repositories registered " +
                                             "with the OMRS Enterprise Connector Manager.",
             EnterpriseOMRSRepositoryConnector.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/Enterprise+OMRS+Repository+Connector"),

    LOCAL_REPOSITORY_CONNECTOR (7,
             "Local Repository Connector",
             "Supports access to metadata stored in the local server's repository and ensures " +
                                        "repository events are generated when metadata changes in the local repository",
             LocalOMRSRepositoryConnector.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/Local+OMRS+Repository+Connector"),

    TYPEDEF_MANAGER (8,
             "Local TypeDef Manager",
             "Supports an in-memory cache for open metadata type definitions (TypeDefs) used for " +
                             "verifying TypeDefs in use in other open metadata repositories and for " +
                             "constructing new metadata instances.",
             OMRSRepositoryContentManager.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/Local+OMRS+TypeDef+Manager"),

    INSTANCE_EVENT_PROCESSOR (8,
             "Local Inbound Instance Event Processor",
             "Supports the loading of reference metadata into the local repository that has come from other members of the local server's cohorts and open metadata archives.",
             LocalOMRSInstanceEventProcessor.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/Local+OMRS+Instance+Event+Processor"),

    REPOSITORY_EVENT_MANAGER (9,
             "Repository Event Manager",
             "Distribute repository events (TypeDefs, Entity and Instance events) between internal OMRS components within a server.",
             OMRSRepositoryEventManager.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Repository+Event+Manager"),

    REST_SERVICES (10,
             "Repository REST Services",
             "Provides the server-side support the the OMRS Repository Services REST API.",
             OMRSRepositoryRESTServices.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Repository+REST+Services"),

    REST_REPOSITORY_CONNECTOR (11,
             "REST Repository Connector",
             "Supports an OMRS Repository Connector for calling the OMRS Repository REST API in a remote " +
                                       "open metadata repository.",
             OMRSRESTRepositoryConnector.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+REST+Repository+Connector"),

    METADATA_HIGHWAY_MANAGER (12,
             "Metadata Highway Manager",
             "Manages the initialization and shutdown of the components that connector to each of the cohorts that the local server is a member of.",
             OMRSMetadataHighwayManager.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Metadata+Highway+Manager"),

    COHORT_MANAGER  (13,
             "Cohort Manager",
             "Manages the initialization and shutdown of the server's connectivity to a cohort.",
             OMRSCohortManager.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Cohort+Manager"),

    COHORT_REGISTRY(14,
             "Cohort Registry",
             "Manages the registration requests send and received from this local repository.",
             OMRSCohortRegistry.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Cohort+Registry"),

    REGISTRY_STORE  (15,
             "Registry Store",
             "Stores information about the repositories registered in the open metadata repository cohort.",
             OMRSCohortRegistryStore.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Cohort+Registry+Store"),

    EVENT_PUBLISHER (16,
             "Event Publisher",
             "Manages the publishing of events that this repository sends to the OMRS topic.",
             OMRSEventPublisher.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Event+Publisher"),

    EVENT_LISTENER  (17,
             "Event Listener",
             "Manages the receipt of incoming OMRS events.",
              OMRSEventListener.class.getName(),
              "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Event+Listener"),

    OMRS_TOPIC_CONNECTOR(18,
             "OMRS Topic Connector",
             "Provides access to the OMRS Topic that is used to exchange events between members of a cohort, " +
                                 "or to notify Open Metadata Access Services (OMASs) of changes to " +
                                 "metadata in the enterprise.",
             OMRSTopicConnector.class.getName(),
             "https://cwiki.apache.org/confluence/display/ATLAS/OMRS+Topic+Connector")
    ;


    private  int      componentId;
    private  String   componentName;
    private  String   componentDescription;
    private  String   componentJavaClass;
    private  String   componentWikiURL;


    /**
     * Set up the values of the enum.
     *
     * @param componentId - code number for the component.
     * @param componentName - name of the component used in the audit log record.
     * @param componentDescription - short description of the component.
     * @param componentJavaClass - name of java class for the component - if logic errors need to be investigated.
     * @param componentWikiURL - URL link to the description of the component.
     */
    OMRSAuditingComponent(int    componentId,
                          String componentName,
                          String componentDescription,
                          String componentJavaClass,
                          String componentWikiURL)
    {
        this.componentId = componentId;
        this.componentName = componentName;
        this.componentDescription = componentDescription;
        this.componentJavaClass = componentJavaClass;
        this.componentWikiURL = componentWikiURL;
    }


    /**
     * Return the numerical code for this enum.
     *
     * @return int componentId
     */
    public int getComponentId()
    {
        return componentId;
    }


    /**
     * Return the name of the component.  This is the name used in the audit log records.
     *
     * @return String component name
     */
    public String getComponentName()
    {
        return componentName;
    }


    /**
     * Return the short description of the component. This is an English description.  Natural language support for
     * these values can be added to UIs using a resource bundle indexed with the component Id.  This value is
     * provided as a default if the resource bundle is not available.
     *
     * @return String description
     */
    public String getComponentDescription()
    {
        return componentDescription;
    }


    /**
     * Name of the java class supporting this component.  This value is provided for debug and not normally make
     * available on end user UIs for security reasons.
     *
     * @return String fully-qualified java class name
     */
    public String getComponentJavaClass()
    {
        return componentJavaClass;
    }


    /**
     * URL link to the wiki page that describes this component.  This provides more information to the log reader
     * on the operation of the component.
     *
     * @return String URL
     */
    public String getComponentWikiURL()
    {
        return componentWikiURL;
    }
}

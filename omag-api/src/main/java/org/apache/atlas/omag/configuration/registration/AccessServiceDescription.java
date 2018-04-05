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

package org.apache.atlas.omag.configuration.registration;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * AccessServiceDescription provides a list of registered OMAS services.
 */
public enum AccessServiceDescription implements Serializable
{
    ASSET_CATALOG_OMAS               (0,   "AssetCatalog", "Search and understand your assets",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Asset+Catalog+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    ASSET_CONSUMER_OMAS              (1,   "AssetConsumer", "Access assets through connectors",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Asset+Consumer+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    ASSET_OWNER_OMAS                 (2,   "AssetOwner", "Manage an asset",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Asset+Owner+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    COMMUNITY_PROFILE_OMAS           (3,   "CommunityProfile", "Define personal profile and collaborate",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Community+Profile+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    CONNECTED_ASSET_OMAS             (4,   "ConnectedAsset", "Understand an asset",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Connected+Asset+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    DATA_PLATFORM_OMAS               (5,   "DataPlatform", "Capture changes in the types of data stored in a data platform",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Data+Platform+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    DATA_SCIENCE_OMAS                (6,   "DataScience", "Create and manage data science definitions and models",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Data+Science+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    DEVOPS_OMAS                      (7,   "DevOps", "Manage a DevOps pipeline",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/DevOps+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    GOVERNANCE_ENGINE_OMAS           (8,   "GovernanceEngine", "Set up an operational governance engine",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Governance+Engine+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    GOVERNANCE_PROGRAM_OMAS          (9,   "GovernanceProgram", "Manage the governance program",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Governance+Program+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    INFORMATION_INFRASTRUCTURE_OMAS  (10,  "InformationInfrastructure", "Describe and plan IT infrastructure",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Information+Infrastructure+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    INFORMATION_LANDSCAPE_OMAS       (11,  "InformationLandscape", "Design the information landscape",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Information+Landscape+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    INFORMATION_PROCESS_OMAS         (12,  "InformationProcess", "Manage process definitions and lineage tracking",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Information+Process+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    INFORMATION_PROTECTION_OMAS      (13,  "InformationProtection", "Manage information protection definitions and compliance",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Information+Protection+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    INFORMATION_VIEW_OMAS            (14,  "InformationView", "Support information virtualization and data set definitions",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Information+View+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    METADATA_DISCOVERY_OMAS          (15,  "MetadataDiscovery", "Support automated metadata discovery",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Metadata+Discovery+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    PRIVACY_OFFICE_OMAS             (16,   "PrivacyOffice", "Manage privacy compliance",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Privacy+Office+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    PROJECT_MANAGEMENT_OMAS          (17,  "ProjectManagement", "Manage data projects",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Project+Management+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    SOFTWARE_DEVELOPMENT_OMAS        (18,  "SoftwareDevelopment", "Develop software with best practices",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Software+Development+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    STEWARDSHIP_ACTION_OMAS          (19,  "StewardshipAction", "Manage exceptions and actions from open governance",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Stewardship+Action+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null),
    SUBJECT_AREA_OMAS                (20,  "SubjectArea", "Document knowledge about a subject area",
                                           "https://cwiki.apache.org/confluence/display/ATLAS/Subject+Area+OMAS",
                                           AccessServiceOperationalStatus.NOT_IMPLEMENTED,
                                           null)
    ;

    private static final long     serialVersionUID    = 1L;
    private static final String   defaultTopicRoot    = "omag/omas/";
    private static final String   defaultInTopicLeaf  = "/inTopic";
    private static final String   defaultOutTopicLeaf = "/outTopic";

    private int                            accessServiceCode;
    private String                         accessServiceName;
    private String                         accessServiceDescription;
    private String                         accessServiceWiki;
    private AccessServiceOperationalStatus accessServiceOperationalStatus;
    private String                         accessServiceAdminClassName;


    /**
     * Return a list containing each of the access service descriptions defined in this enum class.
     *
     * @return ArrayList of enums
     */
    public static ArrayList<AccessServiceDescription> getAccessServiceDescriptionList()
    {
        ArrayList<AccessServiceDescription>  accessServiceDescriptionList = new ArrayList<>();

        accessServiceDescriptionList.add(AccessServiceDescription.ASSET_CATALOG_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.ASSET_CONSUMER_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.ASSET_OWNER_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.COMMUNITY_PROFILE_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.CONNECTED_ASSET_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.DATA_PLATFORM_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.DATA_SCIENCE_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.DEVOPS_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.GOVERNANCE_ENGINE_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.GOVERNANCE_PROGRAM_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.INFORMATION_INFRASTRUCTURE_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.INFORMATION_LANDSCAPE_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.INFORMATION_PROCESS_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.INFORMATION_PROTECTION_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.INFORMATION_VIEW_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.METADATA_DISCOVERY_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.PRIVACY_OFFICE_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.PROJECT_MANAGEMENT_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.SOFTWARE_DEVELOPMENT_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.STEWARDSHIP_ACTION_OMAS);
        accessServiceDescriptionList.add(AccessServiceDescription.SUBJECT_AREA_OMAS);

        return accessServiceDescriptionList;
    }


    /**
     * Default Constructor
     *
     * @param accessServiceCode - ordinal for this access service
     * @param accessServiceName - symbolic name for this access service
     * @param accessServiceDescription - short description for this access service
     * @param accessServiceWiki - wiki page for the access service for this access service
     * @param accessServiceOperationalStatus - default initial operational status for the access service
     */
    AccessServiceDescription(int                            accessServiceCode,
                             String                         accessServiceName,
                             String                         accessServiceDescription,
                             String                         accessServiceWiki,
                             AccessServiceOperationalStatus accessServiceOperationalStatus,
                             String                         accessServiceAdminClassName)
    {
        /*
         * Save the values supplied
         */
        this.accessServiceCode = accessServiceCode;
        this.accessServiceName = accessServiceName;
        this.accessServiceDescription = accessServiceDescription;
        this.accessServiceWiki = accessServiceWiki;
        this.accessServiceOperationalStatus = accessServiceOperationalStatus;
        this.accessServiceAdminClassName = accessServiceAdminClassName;
    }


    /**
     * Return the code for this enum instance
     *
     * @return int - type code
     */
    public int getAccessServiceCode()
    {
        return accessServiceCode;
    }


    /**
     * Return the default name for this enum instance.
     *
     * @return String - default name
     */
    public String getAccessServiceName()
    {
        return accessServiceName;
    }


    /**
     * Return the default description for the type for this enum instance.
     *
     * @return String - default description
     */
    public String getAccessServiceDescription()
    {
        return accessServiceDescription;
    }


    /**
     * Return the URL for the wiki page describing this access service.
     *
     * @return String URL name for the wiki page
     */
    public String getAccessServiceWiki()
    {
        return accessServiceWiki;
    }


    /**
     * Return the default initial operational status for the service.
     *
     * @return AccessServiceOperationalStatus enum
     */
    public AccessServiceOperationalStatus getAccessServiceOperationalStatus()
    {
        return accessServiceOperationalStatus;
    }


    /**
     * Return the InTopic name for the access service.
     *
     * @return String topic name
     */
    public String getAccessServiceInTopic()
    {
        return defaultTopicRoot + accessServiceName + defaultInTopicLeaf;
    }


    /**
     * Return the OutTopic name for the access service.
     *
     * @return String topic name
     */
    public String getAccessServiceOutTopic()
    {
        return defaultTopicRoot + accessServiceName + defaultOutTopicLeaf;
    }


    /**
     * Return the name of the java class that implements the admin class for the access service.
     *
     * @return Name of Java class implementing the AccessServiceAdmin interface.
     */
    public String getAccessServiceAdminClassName()
    {
        return accessServiceAdminClassName;
    }
}

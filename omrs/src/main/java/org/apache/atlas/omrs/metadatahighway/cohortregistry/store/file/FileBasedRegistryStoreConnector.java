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
package org.apache.atlas.omrs.metadatahighway.cohortregistry.store.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.ocf.properties.Endpoint;
import org.apache.atlas.omrs.auditlog.OMRSAuditCode;
import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.auditlog.OMRSAuditingComponent;
import org.apache.atlas.omrs.metadatahighway.cohortregistry.store.OMRSCohortRegistryStoreConnectorBase;
import org.apache.atlas.omrs.metadatahighway.cohortregistry.store.properties.CohortMembership;
import org.apache.atlas.omrs.metadatahighway.cohortregistry.store.properties.MemberRegistration;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * FileBasedRegistryStoreConnector uses JSON to store details of the membership of the open metadata repository
 * cohort on behalf of the OMRSCohortRegistry.
 */
public class FileBasedRegistryStoreConnector extends OMRSCohortRegistryStoreConnectorBase
{
    /*
     * This is the name of the cohort registry file that is used if there is no file name in the connection.
     */
    private static final String defaultFilename = "cohort.registry";

    /*
     * Variables used in writing to the file.
     */
    private String           registryStoreName       = defaultFilename;
    private CohortMembership registryStoreProperties = null;

    /*
     * Variables used for logging and debug.
     */
    private static final OMRSAuditLog auditLog = new OMRSAuditLog(OMRSAuditingComponent.REGISTRY_STORE);

    private static final Logger log = LoggerFactory.getLogger(FileBasedRegistryStoreConnector.class);

    /**
     * Default constructor
     */
    public FileBasedRegistryStoreConnector()
    {
        /*
         * Nothing to do
         */
    }


    /**
     * Initialize the connector.
     *
     * @param connectorInstanceId - unique id for the connector instance - useful for messages etc
     * @param connection - POJO for the configuration used to create the connector.
     */
    @Override
    public void initialize(String connectorInstanceId, Connection connection)
    {
        super.initialize(connectorInstanceId, connection);

        Endpoint endpoint = connection.getEndpoint();

        if (endpoint != null)
        {
            registryStoreName = endpoint.getAddress();

            if (registryStoreName == null)
            {
                registryStoreName = defaultFilename;
            }
        }
    }


    /**
     * Returns the index of the requested member in the members array list.  If the member is not found, the index
     * returned is the size of the array.
     *
     * @param metadataCollectionId - id of the member to find.
     * @param members - list of members
     * @return int index pointing to the location of the member (or the size of the array if the member is not found).
     */
    private int findRemoteRegistration(String   metadataCollectionId, List<MemberRegistration>   members)
    {
        int   indexOfNewMember = members.size();

        for (int i=0; i<members.size(); i++)
        {
            String memberId = members.get(i).getMetadataCollectionId();

            if (metadataCollectionId.equals(memberId))
            {
                if (log.isDebugEnabled())
                {
                    log.debug("Found existing registration for " + metadataCollectionId + " at position " + i);
                }
                return i;
            }
        }

        if (log.isDebugEnabled())
        {
            log.debug("New registration for " + metadataCollectionId + " - saving at position " + indexOfNewMember);
        }
        return indexOfNewMember;
    }


    /**
     * Save the local registration to the cohort registry store.  This provides details of the local repository's
     * registration with the metadata repository cohort.
     * Any previous local registration information is overwritten.
     *
     * @param localRegistration - details of the local repository's registration with the metadata cohort.
     */
    public void saveLocalRegistration(MemberRegistration localRegistration)
    {
        if (localRegistration != null)
        {
            if (registryStoreProperties == null)
            {
                registryStoreProperties = this.retrieveRegistryStoreProperties();
            }

            registryStoreProperties.setLocalRegistration(localRegistration);

            this.writeRegistryStoreProperties(registryStoreProperties);
        }
        else
        {
            String actionDescription = "Saving Local Registration to Registry Store";

            OMRSAuditCode auditCode = OMRSAuditCode.NULL_MEMBER_REGISTRATION;

            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(registryStoreName),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            if (log.isDebugEnabled())
            {
                log.debug("Null local registration passed to saveLocalRegistration :(");
            }
        }
    }


    /**
     * Retrieve details of the local registration from the cohort registry store.  A null may be returned if the
     * local registration information has not been saved (typically because this is a new server instance).
     *
     * @return MemberRegistration object containing details for the local repository's registration with the
     * metadata cohort (may be null if no registration has taken place).
     */
    public MemberRegistration retrieveLocalRegistration()
    {
        MemberRegistration  localRegistration = null;

        if (registryStoreProperties == null)
        {
            registryStoreProperties = this.retrieveRegistryStoreProperties();
        }

        localRegistration = registryStoreProperties.getLocalRegistration();

        if (log.isDebugEnabled())
        {
            if (localRegistration == null)
            {
                log.debug("Null local registration returned from retrieveLocalRegistration");
            }
            else
            {
                log.debug("Local Registration details: " +
                                  "metadataCollectionId: " + localRegistration.getMetadataCollectionId() +
                                  "; displayName: " + localRegistration.getServerName() +
                                  "; serverType: " + localRegistration.getServerType() +
                                  "; organizationName: " + localRegistration.getOrganizationName() +
                                  "; registrationTime " + localRegistration.getRegistrationTime());
            }
        }

        return localRegistration;
    }


    /**
     * Remove details of the local registration from the cohort registry store.  This is used when the local
     * repository unregisters from the open metadata repository cohort.
     *
     * There is a side-effect that all of the remote registrations are removed to since the local repository is
     * no longer a member of this cohort.
     */
    public void removeLocalRegistration()
    {
        if (log.isDebugEnabled())
        {
            log.debug("Emptying cohort registry store.");
        }

        this.writeRegistryStoreProperties(new CohortMembership());
    }


    /**
     * Save details of a remote registration.  This contains details of one of the other repositories in the
     * metadata repository cohort.
     *
     * @param remoteRegistration - details of a remote repository in the metadata repository cohort.
     */
    public void saveRemoteRegistration(MemberRegistration  remoteRegistration)
    {
        if (remoteRegistration != null)
        {
            /*
             * Retrieve the current properties from the file is necessary.
             */
            if (registryStoreProperties == null)
            {
                registryStoreProperties = this.retrieveRegistryStoreProperties();
            }

            /*
             * It is possible that the remote repository already has an entry in the cohort registry and if this is
             * the case, it will be overwritten.  Otherwise the new remote properties are added.
             */
            List<MemberRegistration> remotePropertiesList = registryStoreProperties.getRemoteRegistrations();

            int index = findRemoteRegistration(remoteRegistration.getMetadataCollectionId(), remotePropertiesList);

            if (index < remotePropertiesList.size())
            {
                remotePropertiesList.set(index, remoteRegistration);
            }
            else
            {
                remotePropertiesList.add(remoteRegistration);
            }
            registryStoreProperties.setRemoteRegistrations(remotePropertiesList);

            /*
             * Write out the new cohort registry content.
             */
            this.writeRegistryStoreProperties(registryStoreProperties);
        }
        else
        {
            String actionDescription = "Saving a Remote Registration to Cohort Registry Store";

            OMRSAuditCode auditCode = OMRSAuditCode.NULL_MEMBER_REGISTRATION;

            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(registryStoreName),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            if (log.isDebugEnabled())
            {
                log.debug("Null remote registration passed to saveRemoteRegistration :(");
            }
        }
    }


    /**
     * Return a list of all of the remote metadata repositories registered in the metadata repository cohort.
     *
     * @return Remote registrations iterator
     */
    public List<MemberRegistration> retrieveRemoteRegistrations()
    {
        List<MemberRegistration>    remoteRegistrations = null;

        /*
         * Ensure the current properties are retrieved from the registry.
         */
        if (registryStoreProperties == null)
        {
            registryStoreProperties = this.retrieveRegistryStoreProperties();
        }

        /*
         * Copy the remote member properties into a registration iterator for return.
         */
        List<MemberRegistration> remotePropertiesList = registryStoreProperties.getRemoteRegistrations();
        List<MemberRegistration> remoteRegistrationArray = new ArrayList<>();

        if (remotePropertiesList != null)
        {
            for (MemberRegistration remoteRegistration : remotePropertiesList)
            {
                MemberRegistration   member = new MemberRegistration(remoteRegistration);

                remoteRegistrationArray.add(member);
            }
        }

        if (remoteRegistrationArray.size() > 0)
        {
            remoteRegistrations = remoteRegistrationArray;
        }

        return remoteRegistrations;
    }


    /**
     * Return the registration information for a specific metadata repository, identified by its metadataCollectionId.
     * If the metadataCollectionId is not recognized then null is returned.
     *
     * @param metadataCollectionId - unique identifier for the repository
     * @return MemberRegistration object containing details of the remote metadata repository. (null if not found)
     */
    public MemberRegistration retrieveRemoteRegistration(String    metadataCollectionId)
    {
        MemberRegistration    remoteRegistration = null;

        if (metadataCollectionId != null)
        {
            /*
             * Ensure the current properties are retrieved from the registry.
             */
            if (registryStoreProperties == null)
            {
                registryStoreProperties = this.retrieveRegistryStoreProperties();
            }

            /*
             * Retrieve the list of remote registrations
             */
            List<MemberRegistration> remotePropertiesList = registryStoreProperties.getRemoteRegistrations();

            /*
             * Locate the required entry
             */
            int indexOfEntry = findRemoteRegistration(metadataCollectionId, remotePropertiesList);

            /*
             * If the entry is found create a registration object from it.
             */
            if (indexOfEntry < remotePropertiesList.size())
            {
                remoteRegistration = remotePropertiesList.get(indexOfEntry);
            }
        }
        else
        {
            String actionDescription = "Retrieving Remote Registration from Cohort Registry Store";

            OMRSAuditCode auditCode = OMRSAuditCode.NULL_MEMBER_REGISTRATION;

            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(registryStoreName),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            if (log.isDebugEnabled())
            {
                log.debug("Null metadataCollectionId passed to retrieveRemoteRegistration :(");
            }
        }

        return remoteRegistration;
    }


    /**
     * Remove details of the requested remote repository's registration from the store.
     *
     * @param metadataCollectionId - unique identifier for the repository
     */
    public void removeRemoteRegistration(String    metadataCollectionId)
    {
        if (metadataCollectionId != null)
        {
            /*
             * Ensure the current properties are retrieved from the registry.
             */
            if (registryStoreProperties == null)
            {
                registryStoreProperties = this.retrieveRegistryStoreProperties();
            }

            /*
             * Retrieve the list of remote registrations
             */
            List<MemberRegistration> remotePropertiesList = registryStoreProperties.getRemoteRegistrations();

            /*
             * Locate the required entry
             */
            int indexOfEntry = findRemoteRegistration(metadataCollectionId, remotePropertiesList);

            /*
             * If the entry is found create a registration object from it.
             */
            if (indexOfEntry < remotePropertiesList.size())
            {
                remotePropertiesList.remove(indexOfEntry);
                registryStoreProperties.setRemoteRegistrations(remotePropertiesList);
                writeRegistryStoreProperties(registryStoreProperties);
            }
            else
            {
                String actionDescription = "Removing Remote Registration from Cohort Registry Store";

                OMRSAuditCode auditCode = OMRSAuditCode.MISSING_MEMBER_REGISTRATION;

                auditLog.logRecord(actionDescription,
                                   auditCode.getLogMessageId(),
                                   auditCode.getSeverity(),
                                   auditCode.getFormattedLogMessage(metadataCollectionId, registryStoreName),
                                   null,
                                   auditCode.getSystemAction(),
                                   auditCode.getUserAction());

                if (log.isDebugEnabled())
                {
                    log.debug("MetadataCollectionId : " + metadataCollectionId + " passed to removeRemoteRegistration not found :(");
                }
            }
        }
        else
        {
            String actionDescription = "Removing Remote Registration from Cohort Registry Store";

            OMRSAuditCode auditCode = OMRSAuditCode.NULL_MEMBER_REGISTRATION;

            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(registryStoreName),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            if (log.isDebugEnabled())
            {
                log.debug("Null metadataCollectionId passed to removeRemoteRegistration :(");
            }
        }
    }


    /**
     * Remove the local and remote registrations from the cohort registry store since the local server has
     * unregistered from the cohort.
     */
    public void clearAllRegistrations()
    {
        writeRegistryStoreProperties(null);
    }


    /**
     * Close the config file
     */
    public void disconnect()
    {
        registryStoreProperties = null;

        if (log.isDebugEnabled())
        {
            log.debug("Closing Cohort Registry Store.");
        }
    }


    /**
     * Refresh the registry store properties with the current values in the file base registry store.
     *
     * @return CohortRegistryProperties object containing the currently stored properties.
     */
    private CohortMembership retrieveRegistryStoreProperties()
    {
        File               registryStoreFile = new File(registryStoreName);
        CohortMembership   newRegistryStoreProperties = null;

        try
        {
            if (log.isDebugEnabled())
            {
                log.debug("Retrieving cohort registry store properties");
            }

            String registryStoreFileContents = FileUtils.readFileToString(registryStoreFile, "UTF-8");

            ObjectMapper objectMapper = new ObjectMapper();
            newRegistryStoreProperties = objectMapper.readValue(registryStoreFileContents, CohortMembership.class);
        }
        catch (IOException   ioException)
        {
            /*
             * The registry file is not found, create a new one ...
             */
            String actionDescription = "Retrieving Cohort Registry Store Properties";

            OMRSAuditCode auditCode = OMRSAuditCode.CREATE_REGISTRY_FILE;

            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(registryStoreName),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            if (log.isDebugEnabled())
            {
                log.debug("New Cohort Registry Store", ioException);
            }

            newRegistryStoreProperties = new CohortMembership();
        }

        return newRegistryStoreProperties;
    }


    /**
     * Writes the supplied registry store properties to the registry store.
     *
     * @param newRegistryStoreProperties - contents of the registry store
     */
    private void writeRegistryStoreProperties(CohortMembership   newRegistryStoreProperties)
    {
        File    registryStoreFile = new File(registryStoreName);

        try
        {
            if (log.isDebugEnabled())
            {
                log.debug("Writing cohort registry store properties", newRegistryStoreProperties);
            }

            if (newRegistryStoreProperties == null)
            {
                registryStoreFile.delete();
            }
            else
            {
                ObjectMapper objectMapper = new ObjectMapper();

                String registryStoreFileContents = objectMapper.writeValueAsString(newRegistryStoreProperties);

                FileUtils.writeStringToFile(registryStoreFile, registryStoreFileContents, false);
            }
        }
        catch (IOException   ioException)
        {
            String actionDescription = "Writing Cohort Registry Store Properties";

            OMRSAuditCode auditCode = OMRSAuditCode.UNUSABLE_REGISTRY_FILE;

            auditLog.logException(actionDescription,
                                  auditCode.getLogMessageId(),
                                  auditCode.getSeverity(),
                                  auditCode.getFormattedLogMessage(registryStoreName),
                                  null,
                                  auditCode.getSystemAction(),
                                  auditCode.getUserAction(),
                                  ioException);

            if (log.isDebugEnabled())
            {
                log.debug("Unusable Cohort Registry Store :(", ioException);
            }
        }
    }


    /**
     * Flush all changes and close the registry store.
     */
    public void close()
    {
        this.disconnect();
    }
}

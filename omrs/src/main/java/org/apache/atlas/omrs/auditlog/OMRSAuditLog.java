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


import org.apache.atlas.omrs.auditlog.store.OMRSAuditLogRecord;
import org.apache.atlas.omrs.auditlog.store.OMRSAuditLogRecordOriginator;
import org.apache.atlas.omrs.auditlog.store.OMRSAuditLogReportingComponent;
import org.apache.atlas.omrs.auditlog.store.OMRSAuditLogStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * OMRSAuditLog is a class for managing the audit logging of activity for the OMRS components.  Each auditing component
 * will have their own instance of an OMRSAuditLog. OMRSAuditLog will ensure audit log records are written to
 * disk in the common OMRSAuditLog for this local server.
 *
 * There are different levels of log record to cover all of the activity of the OMRS.
 *
 * This audit log is critical to validate the behavior of the OMRS, particularly in the initial interaction of
 * a new metadata repository to the OMRS Cohort.
 */
public class OMRSAuditLog
{
    static private final OMRSAuditLogRecordOriginator   originator         = new OMRSAuditLogRecordOriginator();
    static private       ArrayList<OMRSAuditLogStore>   auditLogStores     = null;

    private static final Logger log = LoggerFactory.getLogger(OMRSAuditLog.class);

    private              OMRSAuditLogReportingComponent reportingComponent;   /* Initialized in the constructor */


    /**
     * Initialize the static values used in all log records.  These values help to pin-point the source of messages
     * when audit log records from many servers are consolidated into centralized operational tooling.
     *
     * @param localServerName - name of the local server
     * @param localServerType - type of the local server
     * @param localOrganizationName - name of the organization that owns the local server
     * @param auditLogStores - list of destinations for the audit log records
     */
    public static void  initialize(String                  localServerName,
                                   String                  localServerType,
                                   String                  localOrganizationName,
                                   List<OMRSAuditLogStore> auditLogStores)
    {
        OMRSAuditLog.originator.setServerName(localServerName);
        OMRSAuditLog.originator.setServerType(localServerType);
        OMRSAuditLog.originator.setOrganizationName(localOrganizationName);

        if (auditLogStores != null)
        {
            OMRSAuditLog.auditLogStores = new ArrayList<>(auditLogStores);
        }
    }


    /**
     * Set up the local metadata collection Id.  This is null if there is no local repository.
     *
     * @param localMetadataCollectionId - String unique identifier for the metadata collection
     */
    public static void setLocalMetadataCollectionId(String              localMetadataCollectionId)
    {
        OMRSAuditLog.originator.setMetadataCollectionId(localMetadataCollectionId);
    }


    /**
     * Typical constructor - Each component using the Audit log will create their own OMRSAuditLog instance and
     * will push log records to it.
     *
     * @param componentId - numerical identifier for the component.
     * @param componentName - display name for the component.
     * @param componentDescription - description of the component.
     * @param componentWikiURL - link to more information.
     */
    public OMRSAuditLog(int    componentId,
                        String componentName,
                        String componentDescription,
                        String componentWikiURL)
    {
        this.reportingComponent = new OMRSAuditLogReportingComponent(componentId,
                                                                     componentName,
                                                                     componentDescription,
                                                                     componentWikiURL);
    }


    /**
     * External constructor - used to create an audit log for a component outside of OMRS
     *
     * @param reportingComponent - information about the component that will use this instance of the audit log.
     */
    public OMRSAuditLog(OMRSAuditingComponent reportingComponent)
    {
        this.reportingComponent = new OMRSAuditLogReportingComponent(reportingComponent.getComponentId(),
                                                                     reportingComponent.getComponentName(),
                                                                     reportingComponent.getComponentDescription(),
                                                                     reportingComponent.getComponentWikiURL());
    }


    /**
     * Log an audit log record for an event, decision, error, or exception detected by the OMRS.
     *
     * @param actionDescription - description of the activity creating the audit log record
     * @param logMessageId - id for the audit log record
     * @param severity - is this an event, decision, error or exception?
     * @param logMessage - description of the audit log record including specific resources involved
     * @param additionalInformation - additional data to help resolve issues of verify behavior
     * @param systemAction - the related action taken by the OMRS.
     * @param userAction - details of any action that an administrator needs to take.
     */
    public void logRecord(String                      actionDescription,
                          String                      logMessageId,
                          OMRSAuditLogRecordSeverity  severity,
                          String                      logMessage,
                          String                      additionalInformation,
                          String                      systemAction,
                          String                      userAction)
    {
        if (severity != null)
        {
            if ((severity == OMRSAuditLogRecordSeverity.ERROR) || (severity == OMRSAuditLogRecordSeverity.EXCEPTION))
            {
                log.error(logMessageId + " " + logMessage, actionDescription, logMessageId, severity, logMessage, additionalInformation, systemAction, userAction);
            }
            else
            {
                log.info(logMessageId + " " + logMessage, actionDescription, logMessageId, severity, logMessage, additionalInformation, systemAction, userAction);
            }
        }
        else
        {
            severity = OMRSAuditLogRecordSeverity.UNKNOWN;
        }

        if (auditLogStores != null)
        {
            for (OMRSAuditLogStore  auditLogStore : auditLogStores)
            {
                if (auditLogStore != null)
                {
                    List<String> additionalInformationArray = null;

                    if (additionalInformation != null)
                    {
                        additionalInformationArray = new ArrayList<>();
                        additionalInformationArray.add(additionalInformation);
                    }

                    OMRSAuditLogRecord logRecord = new OMRSAuditLogRecord(originator,
                                                                          reportingComponent,
                                                                          severity.getSeverityName(),
                                                                          logMessageId,
                                                                          logMessage,
                                                                          additionalInformationArray,
                                                                          systemAction,
                                                                          userAction);
                    try
                    {
                        auditLogStore.storeLogRecord(logRecord);
                    }
                    catch (Throwable error)
                    {
                        log.error("Error writing audit log: ", logRecord, error);
                    }
                }
            }
        }
    }


    /**
     * Log details of an unexpected exception detected by the OMRS.  These exceptions typically mean that the local
     * server is not configured correctly, or there is a logic error in the code.  When exceptions are logged, it is
     * important that they are investigated and the cause corrected since the local repository is not able to operate
     * as a proper peer in the metadata repository cluster whilst these conditions persist.
     *
     * @param actionDescription - description of the activity in progress when the error occurred
     * @param logMessageId - id for the type of exception caught
     * @param severity - severity of the error
     * @param logMessage - description of the exception including specific resources involved
     * @param additionalInformation - additional data to help resolve issues of verify behavior
     * @param systemAction - the action taken by the OMRS in response to the error.
     * @param userAction - details of any action that an administrator needs to take.
     * @param caughtException - the original exception.
     */
    public void logException(String                      actionDescription,
                             String                      logMessageId,
                             OMRSAuditLogRecordSeverity  severity,
                             String                      logMessage,
                             String                      additionalInformation,
                             String                      systemAction,
                             String                      userAction,
                             Throwable                   caughtException)
    {
        if (caughtException != null)
        {
            this.logRecord(actionDescription,
                           logMessageId,
                           severity,
                           logMessage,
                           additionalInformation + caughtException.toString(),
                           systemAction,
                           userAction);
        }
    }
}

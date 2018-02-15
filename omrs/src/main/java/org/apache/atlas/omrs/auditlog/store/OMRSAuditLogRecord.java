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
package org.apache.atlas.omrs.auditlog.store;

import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

/**
 * OMRSAuditLogRecord provides a carrier for details about a single log record in the OMRS audit log.
 */
public class OMRSAuditLogRecord
{
    private String                         guid                  = null;
    private Date                           timeStamp             = new Date();
    private OMRSAuditLogRecordOriginator   originator            = null;
    private String                         severity              = null;
    private OMRSAuditLogReportingComponent reportingComponent    = null;
    private String                         messageId             = null;
    private String                         messageText           = null;
    private ArrayList<String>              additionalInformation = null;
    private String                         systemAction          = null;
    private String                         userAction            = null;


    /**
     * Audit log records are immutable so the only way to update the values is through the constructor.
     *
     * @param originator - details of the originating server
     * @param reportingComponent - details of the component making the audit log entry.
     * @param severity - OMRSAuditLogRecordSeverity enum that indicates the severity of log record.
     * @param messageId - id of the message in the audit log record.
     * @param messageText - description of the message for the audit log record.
     * @param additionalInformation - additional properties that help to describe the situation.
     * @param systemAction - action taken by the system.
     * @param userAction - followup action that should be taken by the target end user (typically the server
     *                   administrator).
     */
    public OMRSAuditLogRecord(OMRSAuditLogRecordOriginator   originator,
                              OMRSAuditLogReportingComponent reportingComponent,
                              String                         severity,
                              String                         messageId,
                              String                         messageText,
                              ArrayList<String>              additionalInformation,
                              String                         systemAction,
                              String                         userAction)
    {
        this.guid = UUID.randomUUID().toString();
        this.originator = originator;
        this.severity = severity;
        this.reportingComponent = reportingComponent;
        this.messageId = messageId;
        this.messageText = messageText;
        this.additionalInformation = additionalInformation;
        this.systemAction = systemAction;
        this.userAction = userAction;
    }

    /**
     * Return the unique Id of the audit log record
     *
     * @return String guid
     */
    public String getGUID()
    {
        return guid;
    }


    /**
     * Return the time stamp for when the audit log record was created.
     *
     * @return Date object
     */
    public Date getTimeStamp()
    {
        return timeStamp;
    }


    /**
     * Return details of the originator of the log record.
     *
     * @return OMRSAuditLogRecordOriginator object
     */
    public OMRSAuditLogRecordOriginator getOriginator()
    {
        return originator;
    }


    /**
     * Return the severity of the situation recorded in the log record.
     *
     * @return String severity
     */
    public String getSeverity()
    {
        return severity;
    }


    /**
     * Return the name of the component that reported the situation recorded in the log record.
     *
     * @return OMRSAuditLogReportingComponent object
     */
    public OMRSAuditLogReportingComponent getReportingComponent()
    {
        return reportingComponent;
    }


    /**
     * Return the identifier of the message within the log record.
     *
     * @return String message Id
     */
    public String getMessageId()
    {
        return messageId;
    }


    /**
     * Return the text of the message within the log record.
     *
     * @return String message text
     */
    public String getMessageText()
    {
        return messageText;
    }


    /**
     * Return any additional information in the audit log record.
     *
     * @return String additional information
     */
    public ArrayList<String> getAdditionalInformation()
    {
        return additionalInformation;
    }


    public String getSystemAction()
    {
        return systemAction;
    }

    public String getUserAction()
    {
        return userAction;
    }

    @Override
    public String toString()
    {
        String    originatorString = null;
        if (this.originator != null)
        {
            originatorString = this.originator.toString();
        }

        String    reportingComponentString = null;
        if (this.reportingComponent != null)
        {
            reportingComponentString = this.reportingComponent.toString();
        }

        String    additionalInformationString = null;
        if (this.additionalInformation != null)
        {
            boolean    notFirst = false;

            additionalInformationString = "[ ";

            for (String nugget : additionalInformation)
            {
                if (notFirst)
                {
                    additionalInformationString = additionalInformationString + ", ";
                    notFirst = true;
                }

                additionalInformationString = additionalInformationString + nugget;
            }

            additionalInformationString = additionalInformationString + " ]";
        }

        return  "AuditLogRecord { " +
                    "timestamp : " + timeStamp.toString() + ", " +
                    "guid : " + guid + ", " +
                    "originator : " + originatorString + ", " +
                    "severity : " + severity + ", " +
                    "reportingComponent : " + reportingComponentString + ", " +
                    "messageId : " + messageId + ", " +
                    "messageText : " + messageText + ", " +
                    "additionalInformation : " + additionalInformationString + ", " +
                    "systemAction : " + systemAction + ", " +
                    "userAction : " + userAction + " }";
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }

        OMRSAuditLogRecord that = (OMRSAuditLogRecord) object;

        return guid.equals(that.guid);
    }
}

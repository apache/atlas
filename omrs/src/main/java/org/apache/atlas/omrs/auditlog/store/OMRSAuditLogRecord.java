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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * OMRSAuditLogRecord provides a carrier for details about a single log record in the OMRS audit log.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class OMRSAuditLogRecord
{
    private String                         guid                  = null;
    private Date                           timeStamp             = new Date();
    private OMRSAuditLogRecordOriginator   originator            = null;
    private String                         severity              = null;
    private OMRSAuditLogReportingComponent reportingComponent    = null;
    private String                         messageId             = null;
    private String                         messageText           = null;
    private List<String>                   additionalInformation = null;
    private String                         systemAction          = null;
    private String                         userAction            = null;


    /**
     * Default constructor
     */
    public OMRSAuditLogRecord()
    {
    }


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
                              List<String>                   additionalInformation,
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
     * Return the unique Id of the audit log record.
     *
     * @return String guid
     */
    public String getGUID()
    {
        return guid;
    }


    /**
     * Set up the unique Id of the audit log record.
     *
     * @param guid - String guid
     */
    public void setGUID(String guid)
    {
        this.guid = guid;
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
     * Set up the time stamp for when the audit log record was created.
     *
     * @param timeStamp Date object
     */
    public void setTimeStamp(Date timeStamp)
    {
        this.timeStamp = timeStamp;
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
     * Set up details of the originator of the log record.
     *
     * @param originator
     */
    public void setOriginator(OMRSAuditLogRecordOriginator originator)
    {
        this.originator = originator;
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
     * Set up the severity of the situation recorded in the log record.
     *
     * @param severity - String severity
     */
    public void setSeverity(String severity)
    {
        this.severity = severity;
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
     * Set up the name of the component that reported the situation recorded in the log record.
     *
     * @param reportingComponent - OMRSAuditLogReportingComponent object
     */
    public void setReportingComponent(OMRSAuditLogReportingComponent reportingComponent)
    {
        this.reportingComponent = reportingComponent;
    }


    /**
     * Return the identifier of the message within the log record.
     *
     * @return String message id
     */
    public String getMessageId()
    {
        return messageId;
    }


    /**
     * Set up  the identifier of the message within the log record.
     *
     * @param messageId - String message id
     */
    public void setMessageId(String messageId)
    {
        this.messageId = messageId;
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
     * Set up the text of the message within the log record.
     *
     * @param messageText - String message text
     */
    public void setMessageText(String messageText)
    {
        this.messageText = messageText;
    }


    /**
     * Return any additional information in the audit log record.
     *
     * @return List of String additional information
     */
    public List<String> getAdditionalInformation()
    {
        return additionalInformation;
    }


    /**
     * Set up any additional information in the audit log record.
     *
     * @param additionalInformation - List of String additional information
     */
    public void setAdditionalInformation(List<String> additionalInformation)
    {
        this.additionalInformation = additionalInformation;
    }


    /**
     * Return the description of the actions taken by the local server as a result of the reported situation.
     *
     * @return string description
     */
    public String getSystemAction()
    {
        return systemAction;
    }


    /**
     * Set up the description of the actions taken by the local server as a result of the reported situation.
     *
     * @param systemAction - a description of the actions taken by the system as a result of the error.
     */
    public void setSystemAction(String systemAction)
    {
        this.systemAction = systemAction;
    }


    /**
     * Return details of the actions (if any) that a user can take in response to the reported situation.
     *
     * @return String instructions
     */
    public String getUserAction()
    {
        return userAction;
    }


    /**
     * Set up details of the actions (if any) that a user can take in response to the reported situation.
     *
     * @param userAction - String instructions
     */
    public void setUserAction(String userAction)
    {
        this.userAction = userAction;
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

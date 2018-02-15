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

/**
 * OMRSAuditLogRecordSeverity defines the different levels of severity for log records stored in the OMRSAuditLogRecord.
 * <ul>
 *     <li>
 *         UNKNOWN - Uninitialized Severity - if this is seen then there is a logic error in the audit log processing.
 *     </li>
 *     <li>
 *         INFO - An activity occurred as part of the normal operation of the open metadata repository.
 *     </li>
 *     <li>
 *         EVENT - An OMRS Event was sent to or received from members of an open metadata repository cohort.
 *     </li>
 *     <li>
 *         DECISION - A decision has been made related to the interaction of the local metadata repository and the
 *         rest of the open metadata repository cohort.
 *     </li>
 *     <li>
 *         ACTION - A situation has been detected that requires administrator intervention.
 *     </li>
 *     <li>
 *         ERROR - An unexpected error occurred, possibly caused by an incompatibility between the local metadata repository
 *         and one of the remote repositories.  The local repository may restrict some of the metadata interchange
 *         functions as a result.
 *     </li>
 *     <li>
 *         EXCEPTION - Unexpected exception occurred.  This means that the local repository needs some administration
 *         attention to correct configuration or fix a logic error because it is not operating as a proper peer in the
 *         metadata repository cluster.
 *     </li>
 * </ul>
 */
public enum OMRSAuditLogRecordSeverity
{
    UNKNOWN   (0, "<Unknown>",   "Uninitialized Severity."),
    INFO      (1, "Information", "The server is providing information about its normal operation."),
    EVENT     (2, "Event",       "An OMRSEvent was exchanged amongst members of the metadata repository cohort."),
    DECISION  (3, "Decision",    "A decision has been made related to the interaction of the local metadata repository and the rest of the cohort."),
    ACTION    (4, "Action",      "Action is required by the administrator.  " +
                                 "At a minimum, the situation needs to be investigated and if necessary, corrective action taken."),
    ERROR     (5, "Error",       "An error occurred, possibly caused by an incompatibility between the local metadata repository \n" +
                                 "and one of the remote repositories.  " +
                                 "The local repository may restrict some of the metadata interchange \n" +
                                 "functions as a result."),
    EXCEPTION (6, "Exception",   "Unexpected exception occurred.  This means that the local repository needs some administration\n" +
                                 " attention to correct configuration or fix a logic error because it is not operating as a proper peer in the\n" +
                                 " metadata repository cohort.");


    private  int    severityCode;
    private  String severityName;
    private  String severityDescription;


    /**
     * Typical constructor sets up the selected enum value.
     *
     * @param severityCode - numeric of this enum.
     * @param severityName - name of enum.
     * @param severityDescription - default description of enum..
     */
    OMRSAuditLogRecordSeverity(int      severityCode,
                               String   severityName,
                               String   severityDescription)
    {
        this.severityCode = severityCode;
        this.severityName = severityName;
        this.severityDescription = severityDescription;
    }

    /**
     * Return the code for this enum.
     *
     * @return int numeric for this enum
     */
    public int getSeverityCode()
    {
        return severityCode;
    }


    /**
     * Return the name of this enum.
     *
     * @return String name
     */
    public String getSeverityName()
    {
        return severityName;
    }


    /**
     * Return the default description of this enum.  This description is in English.  Natural language translations can be
     * created using a Resource Bundle indexed by the severity code.  This description is a fall back when the resource
     * bundle is not available.
     *
     * @return String default description
     */
    public String getSeverityDescription()
    {
        return severityDescription;
    }
}

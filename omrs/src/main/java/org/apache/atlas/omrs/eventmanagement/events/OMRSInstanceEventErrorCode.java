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

import org.apache.atlas.omrs.eventmanagement.events.OMRSEventErrorCode;

/**
 * OMRSInstanceEventErrorCode defines the list of error codes that are used to record errors in the metadata
 * instance replication process that is used by the repository connectors within the open metadata repository cluster.
 * <ul>
 *     <li>
 *         NOT_IN_USE - There has been no error detected and so the error code is not in use.
 *     </li>
 * </ul>
 */
public enum OMRSInstanceEventErrorCode
{
    NOT_IN_USE                (0, "No Error",
                                  "There has been no error detected and so the error code is not in use.",
                               null),
    CONFLICTING_INSTANCES     (1, "Conflicting Instances",
                               "There are two metadata instances that have the same unique identifier (guid) but" +
                                           " have different types.",
                               OMRSEventErrorCode.CONFLICTING_INSTANCES),
    CONFLICTING_TYPE         (2, "Conflicting Type Version",
                               "An instance can not be processed because there is a mismatch in the type definition (TypeDef) version.",
                               OMRSEventErrorCode.CONFLICTING_TYPE),
    UNKNOWN_ERROR_CODE        (99, "Unknown Error Code",
                               "Unrecognized error code from incoming event.",
                               null);


    private int                errorCodeId;
    private String             errorCodeName;
    private String             errorCodeDescription;
    private OMRSEventErrorCode errorCodeEncoding;


    /**
     * Default constructor sets up the values for this enum instance.
     *
     * @param errorCodeId - int identifier for the enum, used for indexing arrays etc with the enum.
     * @param errorCodeName - String name for the enum, used for message content.
     * @param errorCodeDescription - String default description for the enum, used when there is not natural
     *                             language resource bundle available.
     * @param errorCodeEncoding - code value to use in OMRSEvents
     */
    OMRSInstanceEventErrorCode(int                errorCodeId,
                               String             errorCodeName,
                               String             errorCodeDescription,
                               OMRSEventErrorCode errorCodeEncoding)
    {
        this.errorCodeId = errorCodeId;
        this.errorCodeName = errorCodeName;
        this.errorCodeDescription = errorCodeDescription;
        this.errorCodeEncoding = errorCodeEncoding;
    }


    /**
     * Return the identifier for the enum, used for indexing arrays etc with the enum.
     *
     * @return int identifier
     */
    public int getErrorCodeId()
    {
        return errorCodeId;
    }


    /**
     * Return the name for the enum, used for message content.
     *
     * @return String name
     */
    public String getErrorCodeName()
    {
        return errorCodeName;
    }


    /**
     * Return the default description for the enum, used when there is not natural
     * language resource bundle available.
     *
     * @return String default description
     */
    public String getErrorCodeDescription()
    {
        return errorCodeDescription;
    }


    /**
     * Return the encoding to use in OMRSEvents.
     *
     * @return String OMRSEvent encoding for this errorCode
     */
    public OMRSEventErrorCode getErrorCodeEncoding()
    {
        return errorCodeEncoding;
    }
}
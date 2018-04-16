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
package org.apache.atlas.omag.ffdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Arrays;

/**
 * The OMAGErrorCode is used to define first failure data capture (FFDC) for errors that occur within the OMAG Server
 * It is used in conjunction with OMAG Exceptions, both Checked and Runtime (unchecked).
 *
 * The 5 fields in the enum are:
 * <ul>
 *     <li>HTTP Error Code - for translating between REST and JAVA - Typically the numbers used are:</li>
 *     <li><ul>
 *         <li>500 - internal error</li>
 *         <li>501 - not implemented </li>
 *         <li>503 - Service not available</li>
 *         <li>400 - invalid parameters</li>
 *         <li>401 - unauthorized</li>
 *         <li>404 - not found</li>
 *         <li>405 - method not allowed</li>
 *         <li>409 - data conflict errors - eg item already defined</li>
 *     </ul></li>
 *     <li>Error Message Id - to uniquely identify the message</li>
 *     <li>Error Message Text - includes placeholder to allow additional values to be captured</li>
 *     <li>SystemAction - describes the result of the error</li>
 *     <li>UserAction - describes how a user should correct the error</li>
 * </ul>
 */
public enum OMAGErrorCode
{
    NULL_LOCAL_SERVER_NAME(400, "OMAG-ADMIN-400-001 ",
            "OMAG server has been called with a null local server name",
            "The system is unable to configure the local server.",
            "The local server name is supplied by the caller to the OMAG server. This call needs to be corrected before the server can operate correctly."),

    BAD_LOCAL_SERVER_NAME(400, "OMAG-ADMIN-400-002 ",
            "OMAG server has been called with server name {0} and in previous calls, the server name was {1}",
            "The system is unable to configure the local server.",
            "The local server name is supplied by the caller to the OMAG server. This call needs to be corrected before the server can operate correctly."),

    NULL_USER_NAME(400, "OMAG-ADMIN-400-003 ",
            "OMAG server {0} has been called with a null user name (userId)",
            "The system is unable to configure the local server.",
            "The user name name is supplied by the caller to the OMAG server. This call needs to be corrected before the server can operate correctly."),

    NULL_SERVICE_MODE(400, "OMAG-ADMIN-400-004 ",
            "OMAG server {0} has been configured with a null service mode",
            "The system is unable to configure the local server.",
            "The service mode is supplied by the caller to the OMAG server. This call needs to be corrected before the server can operate correctly."),

    NULL_LOCAL_REPOSITORY_MODE(400, "OMAG-ADMIN-400-005 ",
            "OMAG server {0} has been configured with a null local repository service mode",
            "The system is unable to configure the local server.",
            "The service mode is supplied by the caller to the OMAG server. This call needs to be corrected before the server can operate correctly."),

    NULL_COHORT_NAME(400, "OMAG-ADMIN-400-006 ",
            "OMAG server {0} has been configured with a null cohort name",
            "The system is unable to configure the local server.",
            "The service mode is supplied by the caller to the OMAG server. This call needs to be corrected before the server can operate correctly."),

    LOCAL_REPOSITORY_MODE_NOT_SET(400, "OMAG-ADMIN-400-007 ",
            "The local repository mode has not been set for OMAG server {0}",
            "The local repository mode must be enabled before the event mapper connection or repository proxy connection is set.  The system is unable to configure the local server.",
            "The local repository mode is supplied by the caller to the OMAG server. This call to enable the local repository needs to be made before the call to set the event mapper connection or repository proxy connection."),

    NULL_SERVER_CONFIG(400, "OMAG-ADMIN-400-008 ",
            "The OMAG server {0} has been passed null configuration.",
            "The system is unable to initialize the local server instance.",
            "Retry the request with server configuration."),

    NULL_REPOSITORY_CONFIG(400, "OMAG-ADMIN-400-009 ",
            "The OMAG server {0} has been passed null open metadata repository services configuration",
            "The system is unable to initialize the local server instance.",
            "Set up the open metadata repository services configuration and then retry the request with server configuration."),

    NULL_ACCESS_SERVICE_ADMIN_CLASS(400, "OMAG-ADMIN-400-010 ",
            "The OMAG server {0} has been passed a null admin class name for access service {1}",
            "The system is unable to initialize this access service.",
            "if the access service should be initialized then set up the appropriate admin class name and restart the server instance."),

    BAD_ACCESS_SERVICE_ADMIN_CLASS(400, "OMAG-ADMIN-400-011 ",
            "The OMAG server {0} has been passed an invalid admin class name {1} for access service {2}",
            "The system is unable to initialize this access service.",
            "If the access service should be initialized then set up the appropriate admin class name and restart the server instance."),

    BAD_CONFIG_FILE(400, "OMAG-ADMIN-400-012 ",
            "The OMAG server {0} is not able to open its configuration file {1} due to the following error: {2}",
            "The system is unable to initialize the server.",
            "Review the error message to determine the cause of the problem."),

    NULL_CONFIG_FILE(400, "OMAG-ADMIN-400-012 ",
            "The OMAG server is not able to save its configuration file",
            "The system is unable to maintain the configuration for the server.",
            "Review any related error messages to determine the cause of the problem."),

    BAD_MAX_PAGE_SIZE(400, "OMAG-ADMIN-400-013 ",
            "The OMAG server {0} has been passed an invalid maximum page size of {1}",
            "The system has ignored this value.",
            "The maximum page size must be a number greater than zero.  Retry the request with a valid value."),

    ENTERPRISE_TOPIC_START_FAILED(400, "OMAG-ADMIN-400-014 ",
            "The OMAG server {0} is unable to start the enterprise OMRS topic connector, error message was {1}",
            "The open metadata access services will not be able to receive events from the connected repositories.",
            "Review the error messages and once the source of the problem is resolved, restart the server and retry the request.")
            ;

    private int    httpErrorCode;
    private String errorMessageId;
    private String errorMessage;
    private String systemAction;
    private String userAction;

    private static final Logger log = LoggerFactory.getLogger(OMAGErrorCode.class);


    /**
     * The constructor for OMRSErrorCode expects to be passed one of the enumeration rows defined in
     * OMRSErrorCode above.   For example:
     *
     *     OMRSErrorCode   errorCode = OMRSErrorCode.SERVER_NOT_AVAILABLE;
     *
     * This will expand out to the 5 parameters shown below.
     *
     * @param newHTTPErrorCode - error code to use over REST calls
     * @param newErrorMessageId - unique Id for the message
     * @param newErrorMessage - text for the message
     * @param newSystemAction - description of the action taken by the system when the error condition happened
     * @param newUserAction - instructions for resolving the error
     */
    OMAGErrorCode(int  newHTTPErrorCode, String newErrorMessageId, String newErrorMessage, String newSystemAction, String newUserAction)
    {
        this.httpErrorCode = newHTTPErrorCode;
        this.errorMessageId = newErrorMessageId;
        this.errorMessage = newErrorMessage;
        this.systemAction = newSystemAction;
        this.userAction = newUserAction;
    }


    public int getHTTPErrorCode()
    {
        return httpErrorCode;
    }


    /**
     * Returns the unique identifier for the error message.
     *
     * @return errorMessageId
     */
    public String getErrorMessageId()
    {
        return errorMessageId;
    }


    /**
     * Returns the error message with placeholders for specific details.
     *
     * @return errorMessage (unformatted)
     */
    public String getUnformattedErrorMessage()
    {
        return errorMessage;
    }


    /**
     * Returns the error message with the placeholders filled out with the supplied parameters.
     *
     * @param params - strings that plug into the placeholders in the errorMessage
     * @return errorMessage (formatted with supplied parameters)
     */
    public String getFormattedErrorMessage(String... params)
    {
        if (log.isDebugEnabled())
        {
            log.debug(String.format("<== OMAGErrorCode.getMessage(%s)", Arrays.toString(params)));
        }

        MessageFormat mf = new MessageFormat(errorMessage);
        String result = mf.format(params);

        if (log.isDebugEnabled())
        {
            log.debug(String.format("==> OMAGErrorCode.getMessage(%s): %s", Arrays.toString(params), result));
        }

        return result;
    }


    /**
     * Returns a description of the action taken by the system when the condition that caused this exception was
     * detected.
     *
     * @return systemAction
     */
    public String getSystemAction()
    {
        return systemAction;
    }


    /**
     * Returns instructions of how to resolve the issue reported in this exception.
     *
     * @return userAction
     */
    public String getUserAction()
    {
        return userAction;
    }
}

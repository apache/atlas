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
package org.apache.atlas.omas.connectedasset.ffdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Arrays;

/**
 * The ConnectedAssetErrorCode is used to define first failure data capture (FFDC) for errors that occur when
 * working with
 * OCF Connectors.
 * It is used in conjunction with all Connected Asset OMAS Exceptions, both Checked and Runtime (unchecked).
 *
 * The 5 fields in the enum are:
 * <ul>
 *     <li>HTTP Error Code - for translating between REST and JAVA - Typically the numbers used are:</li>
 *     <li><ul>
 *         <li>500 - internal error</li>
 *         <li>400 - invalid parameters</li>
 *         <li>404 - not found</li>
 *         <li>409 - data conflict errors - eg item already defined</li>
 *     </ul></li>
 *     <li>Error Message Id - to uniquely identify the message</li>
 *     <li>Error Message Text - includes placeholder to allow additional values to be captured</li>
 *     <li>SystemAction - describes the result of the error</li>
 *     <li>UserAction - describes how a user should correct the error</li>
 * </ul>
 */
public enum ConnectedAssetErrorCode
{
    SERVER_URL_NOT_SPECIFIED(400, "OMAS-CONNECTEDASSET-400-001",
            "The OMAS Server URL is null",
            "The system is unable to connect to the OMAS Server to populate the ConnectedAssetProperties object.",
            "Retry the request when the OMAS Service is available."),
    SERVER_URL_MALFORMED(400, "OMAS-CONNECTEDASSET-400-002",
            "The OMAS Server URL {0} is not in a recognized format",
            "The system is unable to connect to the OMAS Server to populate the ConnectedAssetProperties object.",
            "Retry the request when the OMAS Service is available."),
    NULL_CONNECTION(400, "OMAS-CONNECTEDASSET-400-003",
            "The connection passed to OMASConnectedAssetProperties for connector {0} is null.",
            "The system is unable to populate the ConnectedAssetProperties object because it needs the connection to identify the asset.",
            "Look for other error messages to identify what caused this error.  When the issue is resolved, retry the request."),
    NULL_CONNECTION_ID(400, "OMAS-CONNECTEDASSET-400-004",
            "The connection \'{0}\' passed to OMASConnectedAssetProperties has a null GUID inside it.",
            "The system is unable to populate the ConnectedAssetProperties object because it needs the connection to identify the asset.",
            "Look for other error messages to identify what caused this error.  When the issue is resolved, retry the request."),
    NULL_RELATED_ASSET(400, "OMAS-CONNECTEDASSET-400-005",
            "The related asset is null.",
            "The system is unable to populate the RelatedAssetProperties object because it needs the URL to identify the asset.",
            "Look for other error messages to identify what caused this error.  When the issue is resolved, retry the request."),
    NULL_PROPERTY_NAME(400, "OMAS-CONNECTEDASSET-400-006 ",
            "Null property name passed to operation {0} of type {1}",
            "A request to set an additional property failed because the property name passed was null",
            "Recode the call to the property object with a valid property name and retry."),
    NULL_CLASSIFICATION_NAME(400, "OMAS-CONNECTEDASSET-400-007 ",
            "No classification name for entity {0} of type {1}",
            "A classification with a null name is assigned to an entity.   This value should come from a metadata repository, and always be filled in.",
            "Look for other error messages to identify the source of the problem.  Identify the metadata repository where the asset came from.  Correct the cause of the error and then retry."),
    NULL_TAG_NAME(400, "OMAS-CONNECTEDASSET-400-008 ",
            "Null tag name passed to operation {0} of type {1}",
            "A request to set the name of an informal tag property failed because the name passed was null",
            "Recode the call to the property object with a valid name and retry."),
    SERVER_NOT_AVAILABLE(404, "OMAS-CONNECTEDASSET-404-001",
            "The OMAS Server is not available",
            "The system is unable to populate the ConnectedAssetProperties object.",
            "Retry the request when the OMAS Service is available.")
    ;

    private int    httpErrorCode;
    private String errorMessageId;
    private String errorMessage;
    private String systemAction;
    private String userAction;

    private static final Logger log = LoggerFactory.getLogger(ConnectedAssetErrorCode.class);


    /**
     * The constructor for ConnectedAssetErrorCode expects to be passed one of the enumeration rows defined in
     * ConnectedAssetErrorCode above.   For example:
     *
     *     ConnectedAssetErrorCode   errorCode = ConnectedAssetErrorCode.SERVER_NOT_AVAILABLE;
     *
     * This will expand out to the 5 parameters shown below.
     *
     * @param newHTTPErrorCode - error code to use over REST calls
     * @param newErrorMessageId - unique Id for the message
     * @param newErrorMessage - text for the message
     * @param newSystemAction - description of the action taken by the system when the error condition happened
     * @param newUserAction - instructions for resolving the error
     */
    ConnectedAssetErrorCode(int  newHTTPErrorCode, String newErrorMessageId, String newErrorMessage, String newSystemAction, String newUserAction)
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
            log.debug(String.format("<== OCFErrorCode.getMessage(%s)", Arrays.toString(params)));
        }

        MessageFormat mf = new MessageFormat(errorMessage);
        String result = mf.format(params);

        if (log.isDebugEnabled())
        {
            log.debug(String.format("==> OCFErrorCode.getMessage(%s): %s", Arrays.toString(params), result));
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

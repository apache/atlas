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
package org.apache.atlas.ocf.ffdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Arrays;

/**
 * The OCF error code is used to define first failure data capture (FFDC) for errors that occur when working with
 * OCF Connectors.  It is used in conjunction with all OCF Exceptions, both Checked and Runtime (unchecked).
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
public enum OCFErrorCode
{
    NULL_CONNECTION(400, "OCF-CONNECTION-400-001 ",
            "Null connection object passed on request for new connector instance",
            "The system is unable to create the requested connector instance without the connection information that describes which type of connector is required.",
            "Recode call to system to include a correctly formatted connection object and retry the request."),
    UNNAMED_CONNECTION(400, "OCF-CONNECTION-400-002 ",
            "Unnamed connection object passed to requested action {0}",
            "The system is unable to perform the requested action without a connection name.",
            "Update connection configuration to include a value for at least one of the following name properties: qualifiedName, displayName, guid. Then retry the request."),
    NULL_CONNECTOR_TYPE(400, "OCF-CONNECTION-400-003 ",
            "Null connectorType property passed in connection {0}",
            "The system is unable to create the requested connector instance without information on the type of connection required.",
            "Update the connection configuration to include a valid connectorType definition.  Then retry the request."),
    NULL_CONNECTOR_PROVIDER(400, "OCF-CONNECTION-400-004 ",
            "Null Connector Provider passed in connection {0}",
            "The system is unable to create the requested connector instance without information on the type of connection required.",
            "Update the connection configuration to include a valid Java class name for the connector provider in the connectorProviderClassName property of the connection's connectorType. Then retry the request."),
    UNKNOWN_CONNECTOR_PROVIDER(400, "OCF-CONNECTION-400-005 ",
            "Unknown Connector Provider class {0} passed in connection {1}",
            "The system is unable to create the requested connector instance because the Connector Provider's class is not known to the JVM.  This may be because the Connector Provider's jar is not installed in the local JVM or the wrong Java class name has been configured in the connection. ",
            "Verify that the Connector Provider and Connector jar files are properly configured in the process.  Update the connection configuration to include a valid Java class name for the connector provider in the connectorProviderClassName property of the connection's connectorType. Then retry the request."),
    NOT_CONNECTOR_PROVIDER(400, "OCF-CONNECTION-400-006 ",
            "Class {0} passed in connection {1} is not a Connector Provider",
            "The system is unable to create the requested connector instance because the Connector Provider's class does not implement org.apache.atlas.ocf.ConnectorProvider. ",
            "Update the connection configuration to include a valid Java class name for the connector provider in the connectorProviderClassName property of the connection's connectorType. Then retry the request."),
    INCOMPLETE_CONNECTOR_PROVIDER(400, "OCF-CONNECTION-400-007 ",
            "Unable to load Connector Provider class {0} passed in connection {1}",
            "The system is unable to create the requested connector instance because the Connector Provider's class is failing to load in the JVM.  This has resulted in an exception in the class loader.",
            "Verify that the Connector Provider and Connector jar files are properly configured in the process.  Update the connection configuration to include a valid Java class name for the connector provider in the connectorProviderClassName property of the connection's connectorType. Then retry the request."),
    INVALID_CONNECTOR_PROVIDER(400, "OCF-CONNECTION-400-008 ",
            "Invalid Connector Provider class {0} passed in connection {1}",
            "The system is unable to create the requested connector instance because the Connector Provider's class is failing to initialize in the JVM.  This has resulted in an exception in the class loader.",
            "Verify that the Connector Provider and Connector jar files are properly configured in the process.  Update the connection configuration to include a valid Java class name for the connector provider in the connectorProviderClassName property of the connection's connectorType. Then retry the request."),
    NULL_ENDPOINT_IN_CONNECTION(400, "OCF-CONNECTION-400-009 ",
            "Null endpoint detected in connection {0}",
            "The system is unable to initialize the requested connector because the endpoint information in the connection is missing.",
            "Add the endpoint information into the connection object and retry the request."),
    MALFORMED_ENDPOINT(400, "OCF-CONNECTION-400-010 ",
            "The endpoint attribute {0} in connection {1} is set to \"{2}\" which is invalid",
            "The system is unable to initialize the requested connector because the endpoint information in the connection is not formatted correctly for this type of connection.",
            "Correct the endpoint information into the connection object and retry the request."),
    NULL_PROPERTY_NAME(400, "OCF-PROPERTIES-400-011 ",
            "Null property name passed to entity {0} of type {1}",
            "A request to set an additional property failed because the property name passed was null",
            "Recode the call to the property object with a valid property name and retry."),
    INVALID_PROPERTY_NAMES(400, "OCF-PROPERTIES-400-012 ",
            "Non-string property names stored in entity {0} of type {1}",
            "A request to retrieve additional properties failed because the properties have become corrupted.",
            "Debug the calls to the properties object."),
    NULL_SECURED_PROPERTY_NAME(400, "OCF-CONNECTION-400-013 ",
            "Null securedProperty name passed to connection {0})",
            "A request to set a secured property failed because the property name passed was null",
            "Recode the call to the connection object with a valid property name and retry."),
    NO_MORE_ELEMENTS(400, "OCF-PROPERTIES-400-014 ",
            "No more elements in {0} iterator for entity {1} of type {2}",
            "A caller stepping through an iterator has requested more elements when there are none left.",
            "Recode the caller to use the hasNext() method to check for more elements before calling next() and then retry."),
    NO_ITERATOR(400, "OCF-PROPERTIES-400-015 ",
            "No type-specific iterator for {0} paging iterator for entity {1} of type {2}",
            "A caller requesting a paging iterator has not supplied a type-specific iterator in the constructor.",
            "Recode the caller to use the hasNext() method to check for more elements before calling next() and then retry."),
    NULL_CLASSIFICATION_NAME(400, "OCF-PROPERTIES-400-016 ",
            "No classification name for entity {0} of type {1}",
            "A classification with a null name is assigned to an entity.   This value should come from a metadata repository, and always be filled in.",
            "Look for other error messages to identify the source of the problem.  Identify the metadata repository where the asset came from.  Correct the cause of the error and then retry."),
    NULL_TAG_NAME(400, "OCF-PROPERTIES-400-017 ",
            "No tag name for entity {0} of type {1}",
            "A tag with a null name is assigned to an entity.   This value should come from a metadata repository, and always be filled in.",
            "Look for other error messages to identify the source of the problem.  Identify the metadata repository where the asset came from.  Correct the cause of the error and then retry."),
    UNKNOWN_ENDPOINT(404, "OCF-CONNECTOR-404-001 ",
            "Endpoint {0} in connection {1} for connector instance {2} is either unknown or unavailable",
            "The requested action is not able to complete because the remote endpoint where the assets are located is not responding.  It may be unavailable or unknown.",
            "Verify that the endpoint information is correct and the server that supports it is operational, then retry the request."),
    PROPERTIES_NOT_AVAILABLE(404, "OCF-PROPERTIES-404-002 ",
            "Exception with error message \"{0}\" was returned to object {1} resulted from a request for connected asset properties",
            "The requested action is not able to complete which may mean that the server is not able to return all of the properties associated with the asset.",
                     "Verify that the endpoint information is correct and the server that supports it is operational, then retry the request."),
    CAUGHT_EXCEPTION(500, "OCF-CONNECTION-500-001 ",
            "OCF method detected an unexpected exception",
            "The system detected an error during connector processing.",
            "The root cause of the error is captured in previous reported messages."),
    CAUGHT_EXCEPTION_WITHMSG(500, "OCF-CONNECTION-500-002 ",
            "OCF method {0} detected an unexpected exception, message was {1}",
            "The system detected an error during connector processing.",
            "The root cause of the error is captured in previous reported messages."),
    NOT_IMPLEMENTED(500, "OCF-CONNECTION-500-003 ",
            "OCF method {0} not yet implemented",
            "The system is not able to process a request because a feature is not yet implemented.",
            "Contact your support organization for help in discovering a workaround, fix or upgrade to the system."),
    UNKNOWN_ERROR(500, "OCF-CONNECTION-500-004 ",
            "Connection error detected",
            "The system detected an error during connection processing.",
            "The root cause of the error is captured in previous reported messages."),
    INTERNAL_ERROR(500, "OCF-CONNECTION-500-005 ",
            "Internal error in OCF method {0}",
            "The system detected an error during connection processing.",
            "The root cause of the error is captured in previous reported messages."),
    NULL_CONNECTOR_CLASS(500, "OCF-CONNECTOR-500-006 ",
            "The class name for the connector is not set up",
            "The system is unable to create the requested connector instance without the name of the Java class for the connector.",
            "Update the implementation of the connector provider to ensure the connector's java class is intitialized correctly"),
    UNKNOWN_CONNECTOR(500,"OCF-CONNECTOR-500-007 ",
            "Unknown Connector Java class {0}",
            "The system is unable to create the requested connector instance because the Connector's class is not known to the JVM.  This may be because the Connector Provider's jar is not installed in the local JVM or the wrong Java class name has been configured in the connection. ",
            "Verify that the Connector Provider and Connector jar files are properly configured in the process.  Update the connection configuration to include a valid Java class name for the connector provider in the connectorProviderClassName property of the connection's connectorType. Then retry the request."),
    NOT_CONNECTOR(500,"OCF-CONNECTOR-500-008 ",
            "Java class {0} is not a Connector",
            "The system is unable to create the requested connector instance because the Connector's class does not implement org.apache.atlas.ocf.Connector. ",
            "Update the connection configuration to include a valid Java class name for the connector provider in the connectorProviderClassName property of the connection's connectorType. Then retry the request."),
    INCOMPLETE_CONNECTOR(500,"OCF-CONNECTOR-500-009 ",
            "Unable to load Connector Java class {0}",
            "The system is unable to create the requested connector instance because the Connector's class is failing to load in the JVM.  This has resulted in an exception in the class loader.",
            "Verify that the Connector Provider and Connector jar files are properly configured in the process. Then retry the request."),
    INVALID_CONNECTOR(500, "OCF-CONNECTION-500-010 ",
            "Invalid Connector class {0}",
            "The system is unable to create the requested connector instance because the Connector's class is failing to initialize in the JVM.  This has resulted in an exception in the class loader.",
            "Verify that the Connector Provider and Connector jar files are properly configured in the process.  Then retry the request."),
    NULL_CONNECTOR(500, "OCF-CONNECTION-500-011 ",
            "Connector Provider {0} returned a null connector instance for connection {1}",
            "The system detected an error during connector processing and was unable to create a connector.",
            "The root cause of the error is captured in previous reported messages.");

    private int    httpErrorCode;
    private String errorMessageId;
    private String errorMessage;
    private String systemAction;
    private String userAction;

    private static final Logger log = LoggerFactory.getLogger(OCFErrorCode.class);


    /**
     * The constructor for OCFErrorCode expects to be passed one of the enumeration rows defined in
     * OCFErrorCode above.   For example:
     *
     *     OCFErrorCode   errorCode = OCFErrorCode.UNKNOWN_ENDPOINT;
     *
     * This will expand out to the 5 parameters shown below.
     *
     * @param httpErrorCode - error code to use over REST calls
     * @param errorMessageId - unique Id for the message
     * @param errorMessage - text for the message
     * @param systemAction - description of the action taken by the system when the error condition happened
     * @param userAction - instructions for resolving the error
     */
    OCFErrorCode(int  httpErrorCode, String errorMessageId, String errorMessage, String systemAction, String userAction)
    {
        this.httpErrorCode = httpErrorCode;
        this.errorMessageId = errorMessageId;
        this.errorMessage = errorMessage;
        this.systemAction = systemAction;
        this.userAction = userAction;
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
        MessageFormat mf = new MessageFormat(errorMessage);
        String result = mf.format(params);

        if (log.isDebugEnabled())
        {
            log.debug(String.format("OCFErrorCode.getMessage(%s): %s", Arrays.toString(params), result));
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
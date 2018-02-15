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
package org.apache.atlas.omrs.ffdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Arrays;

/**
 * The OMRSErrorCode is used to define first failure data capture (FFDC) for errors that occur within the OMRS
 * It is used in conjunction with all OMRS Exceptions, both Checked and Runtime (unchecked).
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
public enum OMRSErrorCode
{
    ENTITY_NOT_KNOWN(400, "OMRS-REPOSITORY-400-001",
            "The entity identified with guid \"{0}\" is not known to the open metadata repository {1}.",
            "The system is unable to retrieve the properties for the requested entity because the supplied guid is not recognized.",
            "The guid is supplied by the caller to the OMRS.  It may have a logic problem that has corrupted the guid, or the entity has been deleted since the guid was retrieved."),
    RELATIONSHIP_NOT_KNOWN(400, "OMRS-REPOSITORY-400-002",
            "The relationship identified with guid \"{0}\" is not known to the open metadata repository {1}.",
            "The system is unable to retrieve the properties for the requested relationship because the supplied guid is not recognized.",
            "The guid is supplied by the caller to the OMRS.  It may have a logic problem that has corrupted the guid, or the relationship has been deleted since the guid was retrieved."),
    TYPEDEF_NOT_KNOWN(400, "OMRS-REPOSITORY-400-003",
            "The typedef \"{0}\" is not known to the metadata repository.",
            "The system is unable to retrieve the properties for the requested typedef because the supplied identifier is not recognized.",
            "The identifier is supplied by the caller.  It may have a logic problem that has corrupted the identifier, or the typedef has been deleted since the identifier was retrieved."),
    TYPEDEF_IN_USE(400, "OMRS-REPOSITORY-400-004",
            "Unable to delete the typedef identified with guid \"{0}\" since  it is still in use in the metadata repository.",
            "The system is unable to delete the typedef because there are still instances in the metadata repository that are using it.",
            "Remove the existing instances from the open metadata repositories and try the delete again."),
    UNKNOWN_CLASSIFICATION(400, "OMRS-REPOSITORY-400-005",
            "Classification \"{0}\" is not a recognized classification type",
            "The system is unable to create a new classification for an entity because the local repository does not recognize the type.",
            "Create a ClassificationDef for the classification and retry the request."),
    INVALID_CLASSIFICATION_FOR_ENTITY(400, "OMRS-REPOSITORY-400-006",
            "Unable to assign a classification of type  \"{0}\" to an entity of type \"{1}\" because this classification is not valid for this type of entity.",
            "The system is unable to classify an entity because the ClassificationDef for the classification does not list this entity type, or one of its super-types.",
            "Update the ClassificationDef to include the entity's type and rerun the request."),
    NO_TYPEDEF_NAME(400, "OMRS-REPOSITORY-400-007",
            "A null TypeDef name has been passed on a metadata repository request.",
            "The system is unable to perform the request because the TypeDef name is needed.",
            "Correct the caller's code and retry the request."),
    NO_TYPEDEF_CATEGORY(400, "OMRS-REPOSITORY-400-008",
            "A null TypeDef category has been passed on a metadata repository request.",
            "The system is unable to perform the request because the TypeDef category is needed.",
            "Correct the caller's code and retry the request."),
    NO_MATCH_CRITERIA(400, "OMRS-REPOSITORY-400-009",
            "A null list of match criteria properties has been passed on a metadata repository request.",
            "The system is unable to perform the request because the match criteria is needed.",
            "Correct the caller's code and retry the request."),
    NO_EXTERNAL_ID(400, "OMRS-REPOSITORY-400-010",
            "Null values for all of the parameters describing an external id for a standard has been passed on a metadata repository request.",
            "The system is unable to perform the request because at least one of the values are needed.",
            "Correct the caller's code and retry the request."),
    NO_SEARCH_CRITERIA(400, "OMRS-REPOSITORY-400-011",
            "A null search criteria has been passed on a metadata repository request.",
            "The system is unable to perform the request because the search criteria is needed.",
            "Correct the caller's code and retry the request."),
    NO_GUID(400, "OMRS-REPOSITORY-400-012",
            "A null unique identifier (guid) has been passed on a metadata repository request.",
            "The system is unable to perform the request because the TypeDef name is needed.",
            "Correct the caller's code and retry the request."),
    NO_TYPEDEF(400, "OMRS-REPOSITORY-400-013",
            "A null TypeDef has been passed on a metadata repository request.",
            "The system is unable to perform the request because the TypeDef is needed.",
            "Correct the caller's code and retry the request."),
    INVALID_TYPEDEF(400, "OMRS-REPOSITORY-400-013",
            "An invalid TypeDef has been passed on a metadata repository request.",
            "The system is unable to perform the request because the TypeDef is needed.",
            "Correct the caller's code and retry the request."),
    NO_MORE_ELEMENTS(400, "OMRS-PROPERTIES-400-001",
            "No more elements in {0} iterator",
            "A caller stepping through an iterator has requested more elements when there are none left.",
            "Recode the caller to use the hasNext() method to check for more elements before calling next() and then retry."),
    NULL_CLASSIFICATION_NAME(400, "OMRS-PROPERTIES-400-002",
            "No name provided for entity classification",
            "A classification with a null name is assigned to an entity.   This value should come from a metadata repository, and always be filled in.",
            "Look for other error messages to identify the source of the problem.  Identify the metadata repository where the asset came from.  Correct the cause of the error and then retry."),
    NULL_PROPERTY_NAME(400, "OMRS-PROPERTIES-400-003",
            "Null property name passed to properties object",
            "A request to set an additional property failed because the property name passed was null",
            "Recode the call to the property object with a valid property name and retry."),
    ARRAY_OUT_OF_BOUNDS(400, "OMRS-PROPERTIES-400-004",
            "{0} is unable to add a new element to location {1} of an array of size {2} value",
            "There is an error in the update of an ArrayPropertyValue.",
            "Recode the call to the property object with a valid element location and retry."),
    BAD_ATTRIBUTE_TYPE(400, "OMRS-PROPERTIES-400-005",
            "AttributeDefs may only be of primitive, collection or enum type. {0} of category {1} is not allowed.",
            "There is an error in the creation of an AttributeDefType.",
            "Recode the call to the AttributeDefType object with a valid type."),
    REPOSITORY_URL_MALFORMED(400, "OMRS-CONNECTOR-400-001",
            "The Open Metadata Repository Server URL {0} is not in a recognized format",
            "The system is unable to connect to the open metadata repository to retrieve metadata properties.",
            "Retry the request when the connection configuration for this repository is corrected."),
    NULL_CONNECTION(400, "OMRS-CONNECTOR-400-003",
            "The connection passed to OMASConnectedAssetProperties for connector {0} is null.",
            "The system is unable to populate the ConnectedAssetProperties object because it needs the connection to identify the asset.",
            "Look for other error messages to identify what caused this error.  When the issue is resolved, retry the request."),
    NULL_OMRS_CONNECTION(400, "OMRS-CONNECTOR-400-004",
            "The connection passed to the EnterpriseOMRSRepositoryConnector is null.",
            "The system is unable to populate the EnterpriseOMRSRepositoryConnector object because it needs the connection to identify the repository.",
            "Look for other error messages to identify what caused this error.  When the issue is resolved, retry the request."),
    INVALID_OMRS_CONNECTION(400, "OMRS-CONNECTOR-400-005",
            "The connection {0} passed to the EnterpriseOMRSRepositoryConnector is invalid.",
            "The system is unable to populate the EnterpriseOMRSRepositoryConnector object because it needs the connection to identify the repository.",
            "Look for other error messages to identify what caused this error.  When the issue is resolved, retry the request."),
    NULL_TOPIC_CONNECTOR(400, "OMRS-TOPIC-CONNECTOR-400-001",
            "Unable to send or receive events for source {0} because the connector to the OMRS Topic failed to initialize",
            "The local server will not connect to the cohort.",
            "The connection to the connector is configured in the server configuration.  " +
                                 "Review previous error messages to determine the precise error in the " +
                                 "start up configuration. " +
                                 "Correct the configuration and reconnect the server to the cohort. "),
    NULL_REGISTRY_STORE(400, "OMRS-COHORT-REGISTRY-404-001",
            "The Open Metadata Repository Cohort Registry Store for cohort {0} is not available.",
            "The system is unable to process registration requests from the open metadata repository cohort.",
            "Correct the configuration for the registry store connection in the server configuration. " +
            "Retry the request when the registry store configuration is correct."),
    INVALID_LOCAL_METADATA_COLLECTION_ID(400, "OMRS-COHORT-REGISTRY-400-002",
            "The Open Metadata Repository Cohort {0} is not available to server {1} because the local " +
                    "metadata collection id has been changed from {2} to {3} since this server registered with the cohort.",
            "The system is unable to connect with other members of the cohort while this incompatibility exists.",
            "If there is no reason for the change of local metadata collection id (this is the normal case) " +
                    "change the local metadata collection id back to its original valid in the server configuration. " +
                    "If the local metadata collection Id must be changed (due to a conflict for example) " +
                    "then shutdown the server, restart it with no local repository configured and shut it down " +
                    "normally once the server has successfully unregistered with the cohort. " +
                    "Then re-establish the local repository configuration." +
            "Restart the server once the configuration is correct."),
    NULL_AUDIT_LOG_STORE(400, "OMRS-AUDIT-LOG-400-001",
            "The Audit Log Store for server {0} is not available.",
            "The system is unable to process any open metadata registry services (OMRS) requests because " +
                                 "the audit log for this server is unavailable.",
            "Correct the configuration for the audit log store connection in the server configuration. " +
                                "Retry the request when the audit log store configuration is correct."),
    NULL_ARCHIVE_STORE(400, "OMRS-ARCHIVE-MANAGER-400-001",
            "An open metadata archive configured for server {0} is not accessible.",
             "The system is unable to process the contents of this open metadata archive.  " +
                               "Other services may fail if they were dependent on this open metadata archive.",
             "Correct the configuration for the open metadata archive connection in the server configuration. " +
                                 "Retry the request when the open metadata archive configuration is correct."),
    NULL_EVENT_MAPPER(400, "OMRS-LOCAL-REPOSITORY-400-001",
             "The repository event mapper configured for the local repository for server {0} is not accessible.",
             "The system is unable to create the repository event mapper which means that events from the " +
                              "local repository will not be captured and processed.  " +
                              "Other services may fail if they were dependent on this event notification.",
             "Correct the configuration for the repository event mapper connection in the server configuration. " +
                               "Retry the request when the repository event mapper configuration is correct."),
    DUPLICATE_COHORT_NAME(400, "OMRS-METADATA-HIGHWAY-404-001",
            "There are more than one cohort configurations with the same name of {0}.",
            "The system is unable to connect to more than one cohort with the same name.",
            "Correct the configuration for the cohorts in the server configuration. " +
                                 "Retry the request when the cohort configuration is correct."),
    CONFLICTING_ENTERPRISE_TYPEDEFS(400, "OMRS-ENTERPRISE-REPOSITORY-400-001",
            "Conflicting TypeDefs have been detected.",
            "The system is unable to create a reliable list of TypeDefs for the enterprise.",
            "Details of the conflicts and the steps necessary to repair the situation can be found in the audit log. " +
                                  "Retry the request when the cohort configuration is correct."),
    NO_TYPEDEFS_DEFINED(400, "OMRS-ENTERPRISE-REPOSITORY-400-002",
            "No TypeDefs have been defined in any of the connected repositories.",
            "The system is unable to create a list of TypeDefs for the enterprise.",
            "Look for errors in the set up of the repositories in the audit log and verify that TypeDefs are configured. " +
                                            "Retry the request when the cohort configuration is correct."),
    REPOSITORY_NOT_AVAILABLE(404, "OMRS-REPOSITORY-CONNECTOR-404-001",
            "The Open Metadata Repository Server is not available",
            "The system is unable to retrieve any metadata properties from this repository.",
            "Retry the request when the repository server is available."),
    COHORT_NOT_CONNECTED(404, "OMRS-REPOSITORY-CONNECTOR-404-002",
            "The Open Metadata Repository Servers in the cohort are not available",
            "The system is unable to retrieve any metadata properties from this repository.",
            "Retry the request when the repository server is available."),
    INVALID_COHORT_CONFIG(404, "OMRS-REPOSITORY-CONNECTOR-404-003",
            "The Open Metadata Repository Servers in the cohort are not configured correctly",
            "The root cause of this error is recorded in previous exceptions.",
            "Review the other error messages to determine the source of the error.  When these are resolved, retry the request."),
    LOCAL_REPOSITORY_CONFIGURATION_ERROR(404, "OMRS-REPOSITORY-CONNECTOR-404-004",
            "The connection to the local Open Metadata Repository Server is not configured correctly",
            "The root cause of this error is recorded in previous exceptions.",
            "Review the other error messages to determine the source of the error.  When these are resolved, retry the request."),
    METADATA_HIGHWAY_NOT_AVAILABLE(404, "OMRS-METADATA-HIGHWAY-404-001",
            "The local server's metadata highway communication components are failing to initialize",
            "The root cause of this error is recorded in previous exceptions.",
            "Review the other error messages to determine the source of the error.  When these are resolved, retry the request."),
    COHORT_DISCONNECT_FAILED(404, "OMRS-METADATA-HIGHWAY-404-002",
            "The local server is unable to disconnect from an open metadata repository cohort {0}",
            "The root cause of this error is recorded in previous exceptions.",
            "Review the other error messages to determine the source of the error.  When these are resolved, retry the request."),
    TOPIC_CONNECTOR_NOT_AVAILABLE(404, "OMRS-TOPIC-CONNECTOR-404-001",
            "The OMRS Topic Connector is not available.",
            "The system is not able to process events sent between repositories in the open metadata cohort.",
            "Correct the configuration for the OMRS Topic Connector (in the OMRS Configuration). Retry the request when the topic connector configuration is correct."),
    ENTERPRISE_NOT_SUPPORTED(405, "OMRS-ENTERPRISE-REPOSITORY-CONNECTOR-405-001",
            "The requested method {0} is not supported by the EnterpriseOMRSRepositoryConnector",
            "The system is not able to process the requested method because it is not supported by the " +
                                     "Open Metadata Repository Services (OMRS) Enterprise Repository Services.",
            "Correct the application that called this method."),
    INVALID_PRIMITIVE_CLASS_NAME(500, "OMRS-METADATA-COLLECTION-500-001",
            "The Java class \'{0}\' for PrimitiveDefCategory {1} is not known.",
            "There is an internal error in Java class PrimitiveDefCategory as it has been set up with an invalid class.",
            "Raise a Jira to get this fixed."),
    INVALID_PRIMITIVE_VALUE(500, "OMRS-METADATA-COLLECTION-500-002",
            "The primitive value should be store in Java class \'{0}\' since it is of PrimitiveDefCategory {1}.",
            "There is an internal error in the creation of a PrimitiveTypeValue.",
            "Raise a Jira to get this fixed."),
    NULL_HOME_METADATA_COLLECTION_ID(500, "OMRS-METADATA-COLLECTION-500-003",
            "Null home metadata collection identifier found in metadata instance {0} from open metadata repository {1}",
            "A request to retrieve a metadata instance (entity or relationship) has encountered a homeless metadata instance.",
            "Locate the open metadata repository that supplied the instance and correct the logic in its OMRSRepositoryConnector."),
    NULL_CONFIG(500, "OMRS-OPERATIONAL-SERVICES-500-001",
            "No configuration has been passed to the Open Metadata Repository Services (OMRS) on initialization.",
            "here is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NULL_METADATA_COLLECTION_ID(500, "OMRS-METADATA-COLLECTION-500-004",
            "The local repository services have been initialized with a null metadata collection identifier.",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NULL_COHORT_NAME(500, "OMRS-COHORT-MANAGER-500-001",
            "OMRSCohortManager has been initialized with a null cohort name",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NULL_LOCAL_METADATA_COLLECTION(500, "OMRS-LOCAL-REPOSITORY-500-001",
            "The local repository services have been initialized with a null real metadata collection.",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NULL_ENTERPRISE_METADATA_COLLECTION(500, "OMRS-ENTERPRISE-REPOSITORY-500-001",
            "The enterprise repository services has detected a repository connector with a null metadata collection.",
            "There is an internal error in the Open Metadata Repository Services (OMRS) operation.",
            "Raise a Jira to get this fixed."),
    BAD_TYPEDEF(500, "OMRS-CONTENT-MANAGER-500-001",
            "The repository content manager has detected an invalid TypeDef from {0}.",
            "There is an internal error in the Open Metadata Repository Services (OMRS) operation.",
            "Raise a Jira to get this fixed."),
    BAD_TYPEDEF_ATTRIBUTE_NAME(500, "OMRS-CONTENT-MANAGER-500-002",
            "The repository content manager has detected an invalid attribute name in a TypeDef from {0}.",
            "There is an internal error in the Open Metadata Repository Services (OMRS) operation.",
            "Raise a Jira to get this fixed."),
    NULL_TYPEDEF_ATTRIBUTE(500, "OMRS-CONTENT-MANAGER-500-003",
            "The repository content manager has detected a null attribute in a TypeDef from {0}.",
            "There is an internal error in the Open Metadata Repository Services (OMRS) operation.",
            "Raise a Jira to get this fixed."),
    BAD_CATEGORY_FOR_TYPEDEF_ATTRIBUTE(500, "OMRS-CONTENT-MANAGER-500-004",
            "Source {0} has requested type {1} with an incompatible category of {2} from repository content manager.",
            "There is an error in the Open Metadata Repository Services (OMRS) operation - probably in the source component.",
            "Raise a Jira to get this fixed."),
    ARCHIVE_UNAVAILABLE(503, "OMRS-OPEN-METADATA-TYPES-500-001",
            "The enterprise repository services are disconnected from the open metadata repositories.",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    METHOD_NOT_IMPLEMENTED(501, "OMRS-METADATA-COLLECTION-501-001",
            "OMRSMetadataCollection method {0} for OMRS Connector {1} to repository type {2} is not implemented.",
            "A method in MetadataCollectionBase was called which means that the connector's OMRSMetadataCollection " +
                                   "(a subclass of MetadataCollectionBase) does not have a complete implementation.",
            "Raise a Jira to get this fixed."),
    NO_REPOSITORIES(503, "OMRS-ENTERPRISE-REPOSITORY-503-001",
            "There are no open metadata repositories available for this server.",
            "The configuration for the server is set up so there is no local repository and no remote repositories " +
                            "connected through the open metadata repository cohorts.  " +
                            "This may because of one or more configuration errors.",
            "Retry the request once the configuration is changed."),
    ENTERPRISE_DISCONNECTED(503, "OMRS-ENTERPRISE-REPOSITORY-503-002",
            "The enterprise repository services are disconnected from the open metadata repositories.",
            "The server has shutdown (or failed to set up) access to the open metadata repositories.",
            "Retry the request once the open metadata repositories are connected."),
    NULL_COHORT_METADATA_COLLECTION(503, "OMRS-ENTERPRISE-REPOSITORY-500-001",
            "The enterprise repository services has detected a repository connector from cohort {0} for metadata collection identifier {1} that has a null metadata collection API object.",
            "There is an internal error in the OMRS Repository Connector implementation.",
            "Raise a Jira to get this fixed.")
    ;

    private int    httpErrorCode;
    private String errorMessageId;
    private String errorMessage;
    private String systemAction;
    private String userAction;

    private static final Logger log = LoggerFactory.getLogger(OMRSErrorCode.class);


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
    OMRSErrorCode(int  newHTTPErrorCode, String newErrorMessageId, String newErrorMessage, String newSystemAction, String newUserAction)
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
            log.debug(String.format("<== OMRSErrorCode.getMessage(%s)", Arrays.toString(params)));
        }

        MessageFormat mf = new MessageFormat(errorMessage);
        String result = mf.format(params);

        if (log.isDebugEnabled())
        {
            log.debug(String.format("==> OMRSErrorCode.getMessage(%s): %s", Arrays.toString(params), result));
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

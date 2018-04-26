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
    TYPEDEF_IN_USE(400, "OMRS-REPOSITORY-400-001 ",
            "Unable to delete the TypeDef \"{0}\" (guid = {1}) since it is still in use in the open metadata repository {2}",
            "The system is unable to delete the TypeDef because there are still instances in the metadata repository that are using it.",
            "Remove the existing instances from the open metadata repositories and try the delete again."),
    ATTRIBUTE_TYPEDEF_IN_USE(400, "OMRS-REPOSITORY-400-002 ",
            "Unable to delete the AttributeTypeDef \"{0}\" (guid = {1}) since it is still in use in the open metadata repository {2}",
            "The system is unable to delete the AttributeTypeDef because there are still instances in the metadata repository that are using it.",
            "Remove the existing instances from the open metadata repositories and try the delete again."),
    TYPEDEF_ALREADY_DEFINED(400, "OMRS-REPOSITORY-400-003 ",
            "Unable to add the TypeDef \"{0}\" (guid = {1}) since it is already defined in the open metadata repository {2}",
            "The system is unable to add the TypeDef to its repository because it is already defined.",
            "Validate that the existing definition is as required.  It is possible to patch the TypeDef, or delete it and re-define it."),
    ATTRIBUTE_TYPEDEF_ALREADY_DEFINED(400, "OMRS-REPOSITORY-400-004 ",
            "Unable to add the AttributeTypeDef \"{0}\" (guid = {1}) since it is already defined in the open metadata repository {2}",
            "The system is unable to delete the AttributeTypeDef because there are still instances in the metadata repository that are using it.",
            "Validate that the existing definition is as required.  It is possible to patch the TypeDef, or delete it and re-define it."),
    UNKNOWN_CLASSIFICATION(400, "OMRS-REPOSITORY-400-005 ",
            "Classification \"{0}\" is not a recognized classification type by open metadata repository {1}",
            "The system is unable to create a new classification for an entity because the open metadata repository does not recognize the classification type.",
            "Create a ClassificationDef for the classification and retry the request."),
    INVALID_CLASSIFICATION_FOR_ENTITY(400, "OMRS-REPOSITORY-400-006 ",
            "Open metadata repository {0} is unable to assign a classification of type \"{0}\" to an entity of type \"{1}\" because the classification type is not valid for this type of entity",
            "The system is unable to classify an entity because the ClassificationDef for the classification does not list this entity type, or one of its super-types.",
            "Update the ClassificationDef to include the entity's type and rerun the request. Alternatively use a different classification."),
    NO_TYPEDEF_NAME(400, "OMRS-REPOSITORY-400-007 ",
            "A null TypeDef name has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the TypeDef name is needed.",
            "Correct the caller's code and retry the request."),
    NO_ATTRIBUTE_TYPEDEF_NAME(400, "OMRS-REPOSITORY-400-008 ",
            "A null AttributeTypeDef name has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the AttributeTypeDef name is needed.",
            "Correct the caller's code and retry the request."),
    NO_TYPEDEF_CATEGORY(400, "OMRS-REPOSITORY-400-009 ",
            "A null TypeDef category has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the TypeDef category is needed.",
            "Correct the caller's code and retry the request."),
    NO_ATTRIBUTE_TYPEDEF_CATEGORY(400, "OMRS-REPOSITORY-400-010 ",
            "A null AttributeTypeDef category has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the AttributeTypeDef category is needed.",
            "Correct the caller's code and retry the request."),
    NO_MATCH_CRITERIA(400, "OMRS-REPOSITORY-400-011 ",
            "A null list of match criteria properties has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the match criteria is needed.",
            "Correct the caller's code and retry the request."),
    NO_EXTERNAL_ID(400, "OMRS-REPOSITORY-400-012 ",
            "Null values for all of the parameters describing an external id for a standard has been passed on a {0} request to open metadata repository {1}",
            "The system is unable to perform the request because at least one of the values are needed.",
            "Correct the caller's code and retry the request."),
    NO_SEARCH_CRITERIA(400, "OMRS-REPOSITORY-400-013 ",
            "A null search criteria has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the search criteria is needed.",
            "Correct the caller's code and retry the request."),
    NO_GUID(400, "OMRS-REPOSITORY-400-014 ",
            "A null unique identifier (guid) has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the unique identifier is needed.",
            "Correct the caller's code and retry the request."),
    NO_TYPEDEF(400, "OMRS-REPOSITORY-400-015 ",
            "A null TypeDef has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the TypeDef is needed.",
            "Correct the caller's code and retry the request."),
    NO_ATTRIBUTE_TYPEDEF(400, "OMRS-REPOSITORY-400-016 ",
            "A null AttributeTypeDef has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the AttributeTypeDef is needed.",
            "Correct the caller's code and retry the request."),
    INVALID_TYPEDEF(400, "OMRS-REPOSITORY-400-017 ",
            "An invalid TypeDef {0} (guid={1}) has been passed as the {2} parameter on a {3} request to open metadata repository {4}. Full TypeDef is {5}",
            "The system is unable to perform the request because the TypeDef is needed.",
            "Correct the caller's code and retry the request."),
    INVALID_ATTRIBUTE_TYPEDEF(400, "OMRS-REPOSITORY-400-018 ",
            "An invalid AttributeTypeDef {0} (guid={1}) has been passed as the {2} parameter on a {3} request to open metadata repository {4}. Full AttributeTypeDef is {5}",
            "The system is unable to perform the request because the AttributeTypeDef is needed.",
            "Correct the caller's code and retry the request."),
    NULL_TYPEDEF(400, "OMRS-REPOSITORY-400-019 ",
            "A null TypeDef has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the TypeDef is needed to perform the operation.",
            "Correct the caller's code and retry the request."),
    NULL_ATTRIBUTE_TYPEDEF(400, "OMRS-REPOSITORY-400-020 ",
            "A null AttributeTypeDef has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the AttributeTypeDef is needed to perform the operation.",
            "Correct the caller's code and retry the request."),
    NULL_TYPEDEF_GALLERY(400, "OMRS-REPOSITORY-400-021 ",
            "A null TypeDefGalleryResponse object has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the TypeDefGalleryResponse should contain the information to process.",
            "Correct the caller's code and retry the request."),
    NULL_TYPEDEF_IDENTIFIER(400, "OMRS-REPOSITORY-400-022 ",
            "A null unique identifier (guid) for a TypeDef object has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the identifier for the TypeDef is needed to proceed.",
            "Correct the caller's code and retry the request."),
    NULL_TYPEDEF_NAME(400, "OMRS-REPOSITORY-400-023 ",
            "A null unique name for a TypeDef object has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the identifier for the TypeDef is needed to proceed.",
            "Correct the caller's code and retry the request."),
    NULL_ATTRIBUTE_TYPEDEF_IDENTIFIER(400, "OMRS-REPOSITORY-400-024 ",
            "A null unique identifier (guid) for a AttributeTypeDef object has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the identifier for the AttributeTypeDef is needed to proceed.",
            "Correct the caller's code and retry the request."),
    NULL_METADATA_COLLECTION(400, "OMRS-REPOSITORY-400-025 ",
            "Open metadata repository for server {0} has not initialized correctly because it has a null metadata collection.",
            "The system is unable to process requests for this repository.",
            "Verify that the repository connector is correctly configured in the OMAG server."),
    NULL_CLASSIFICATION_NAME(400, "OMRS-REPOSITORY-400-026 ",
            "A null classification name has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to access the local metadata repository.",
            "The classification name is supplied by the caller to the API. This call needs to be corrected before the server can operate correctly."),
    NULL_USER_ID(400, "OMRS-REPOSITORY-400-027 ",
            "A null user name has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to access the local metadata repository.",
            "The user name is supplied by the caller to the API. This call needs to be corrected before the server can operate correctly."),
    BAD_PROPERTY_FOR_TYPE(400, "OMRS-REPOSITORY-400-028 ",
            "A property called {0} has been proposed for a new metadata instance of category {1} and type {2}; it is not supported for this type in open metadata repository {3}",
            "The system is unable to store the metadata instance in the metadata repository because the properties listed do not match the supplied type definition.",
            "Verify that the property name is spelt correctly and the correct type has been used. Correct the call to the metadata repository and retry."),
    NO_PROPERTIES_FOR_TYPE(400, "OMRS-REPOSITORY-400-029 ",
            "Properties have been proposed for a new metadata instance of category {0} and type {1}; properties not supported for this type in open metadata repository {2}",
            "The system is unable to store the metadata instance in the metadata repository because the properties listed do not match the supplied type definition.",
            "Verify that the property name is spelt correctly and the correct type has been used. Correct the call to the metadata repository and retry."),
    BAD_PROPERTY_TYPE(400, "OMRS-REPOSITORY-400-030 ",
            "A property called {0} of type {1} has been proposed for a new metadata instance of category {2} and type {3}; this property should be of type {4} in open metadata repository {5}",
            "The system is unable to store the metadata instance in the metadata repository because the properties listed do not match the supplied type definition.",
            "Verify that the property name is spelt correctly and the correct type has been used. Correct the call to the metadata repository and retry."),
    NULL_PROPERTY_NAME_FOR_INSTANCE(400, "OMRS-REPOSITORY-400-031 ",
            "A null property name has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to process the metadata instance.",
            "The property name is supplied by the caller to the API. This call needs to be corrected before the server can operate correctly."),
    NULL_PROPERTY_VALUE_FOR_INSTANCE(400, "OMRS-REPOSITORY-400-032 ",
            "A null property value has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to process the metadata instance.",
            "The property value is supplied by the caller to the API. This call needs to be corrected before the server can operate correctly."),
    NULL_PROPERTY_TYPE_FOR_INSTANCE(400, "OMRS-REPOSITORY-400-033 ",
            "A null property type has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to process the metadata instance.",
            "The property type is supplied by the caller to the API. This call needs to be corrected before the server can operate correctly."),
    BAD_TYPEDEF_ID_FOR_INSTANCE(400, "OMRS-REPOSITORY-400-034 ",
            "A invalid TypeDef unique identifier (guid) has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to perform the request because the unique identifier is needed.",
            "Correct the caller's code and retry the request."),
    BAD_INSTANCE_STATUS(400, "OMRS-REPOSITORY-400-035 ",
            "An instance status of {0} has been passed as the {1} parameter on a {2} request to open metadata repository {3} but this status is not valid for an instance of type {4}",
            "The system is unable to process the request.",
            "The instance status is supplied by the caller to the API. This call needs to be corrected before the server can complete this operation successfully."),
    NULL_INSTANCE_STATUS(400, "OMRS-REPOSITORY-400-036 ",
            "A null instance status has been passed as the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to process the metadata instance request.",
            "The instance status is supplied by the caller to the API. This call needs to be corrected before the server can operate correctly."),
    NO_NEW_PROPERTIES(400, "OMRS-REPOSITORY-400-037 ",
            "No properties have been passed on the {0} parameter on a {1} request to open metadata repository {2}",
            "The system is unable to process the metadata instance request.",
            "The instance status is supplied by the caller to the API. This call needs to be corrected before the server can operate correctly."),
    REPOSITORY_NOT_CRYSTAL_BALL(400, "OMRS-REPOSITORY-400-038 ",
            "A future time of {0} has been passed on the {0} parameter of a {1} request to open metadata repository {2}",
            "The system declines to process the request lest it gives away its secret powers.",
            "The asOfTime is supplied by the caller to the API. This call needs to be corrected before the server will operate correctly."),
    BAD_TYPEDEF_IDS_FOR_DELETE(400, "OMRS-REPOSITORY-400-039 ",
            "Incompatible TypeDef unique identifiers (name={0}, guid{1}) have been passed on a {2} request for instance {3} to open metadata repository {2}",
            "The system is unable to perform the request because the unique identifier is needed.",
            "Correct the caller's code and retry the request."),
    BAD_PROPERTY_FOR_INSTANCE(400, "OMRS-REPOSITORY-400-040 ",
            "An invalid property has been found in a metadata instance stored in repository {0} during the {1} operation",
            "The system is unable to perform the request because the unique identifier is needed.",
            "Correct the caller's code and retry the request."),
    NULL_REFERENCE_INSTANCE(400, "OMRS-REPOSITORY-400-041 ",
            "A null reference instance has been passed to repository {0} during the {1} in the {2} parameter",
            "The system is unable to perform the request because the instance is needed.",
            "The reference instance comes from another server.  Look for errors in the audit log and validate that the message passing " +
                                    "protocol levels are compatible. If nothing is obviously wrong with the set up, " +
                                    "raise a Jira or ask for help on the dev mailing list."),
    LOCAL_REFERENCE_INSTANCE(400, "OMRS-REPOSITORY-400-042 ",
            "A reference instance has been passed to repository {0} during the {1} in the {2} parameter which has the local repository as its home",
            "The system is unable to perform the request because the instance should come from another repository.",
            "The reference instance comes from another server.  Look for errors in the audit log and validate that the message passing " +
                                    "protocol levels are compatible. If nothing is obviously wrong with the set up, " +
                                    "raise a Jira or ask for help on the dev mailing list."),
    NULL_ENTITY_PROXY(400, "OMRS-REPOSITORY-400-043 ",
            "A null entity proxy has been passed to repository {0} as the {1} parameter of the {2} operation",
            "The system is unable to perform the request because the entity proxy is needed.",
            "Correct the caller's code and retry the request."),
    LOCAL_ENTITY_PROXY(400, "OMRS-REPOSITORY-400-044 ",
            "An entity proxy has been passed to repository {0} as the {1} parameter of the {2} operation which has the local repository as its home",
            "The system is unable to perform the request because the entity proxy should represent an entity that is homed in another repository.",
            "Correct the caller's code and retry the request."),
    INSTANCE_ALREADY_DELETED(400, "OMRS-REPOSITORY-400-045 ",
            "A {0} request has been made to repository {1} for an instance {2} that is already deleted",
            "The system is unable to perform the request because the instance is in the wrong state.",
            "Try a different request or a different instance."),
    INSTANCE_NOT_DELETED(400, "OMRS-REPOSITORY-400-046 ",
            "A {0} request has been made to repository {1} for an instance {2} that is not deleted",
            "The system is unable to perform the request because the instance is in the wrong state.",
            "Try a different request or a different instance."),
    INVALID_RELATIONSHIP_ENDS(400, "OMRS-REPOSITORY-400-047 ",
            "A {0} request has been made to repository {1} for a relationship that has one or more ends of the wrong or invalid type.  Relationship type is {2}; entity proxy for end 1 is {3} and entity proxy for end 2 is {4}",
            "The system is unable to perform the request because the instance has invalid values.",
            "Correct the caller's code and retry the request."),
    ENTITY_NOT_CLASSIFIED(400, "OMRS-REPOSITORY-400-048 ",
            "A {0} request has been made to repository {1} to remove a non existent classification {2} from entity {3}",
            "The system is unable to perform the request because the instance has invalid values.",
            "Correct the caller's code and retry the request."),
    NULL_TYPEDEF_PATCH(400, "OMRS-REPOSITORY-400-049 ",
            "A null TypeDef patch has been passed on the {0} operation of repository {1} ",
            "The system is unable to perform the request because it needs the patch to process the update.",
            "Correct the caller's code and retry the request."),
    NEGATIVE_PAGE_SIZE(400, "OMRS-REPOSITORY-400-050 ",
            "A negative pageSize of {0} has been passed on the {0} parameter of a {1} request to open metadata repository {2}",
            "The system is unable to process the request.",
            "The pageSize parameter is supplied by the caller to the API. This call needs to be corrected before the server will operate correctly."),
    ENTITY_PROXY_ONLY(400, "OMRS-REPOSITORY-400-051 ",
            "A request for entity {0} has been passed to repository {1} as the {2} parameter of the {3} operation but only an entity proxy has been found",
            "The system is unable to return all of the details of the entity.  It can only supply an entity summary.",
            "The fact that the system has a proxy means that the entity exists in one of the members of the connected cohorts.  " +
                              "The repository where it is located may be unavailable, or the entity has been deleted " +
                              "but the delete request has not propagated through to this repository."),
    INVALID_ENTITY_FROM_STORE(400, "OMRS-REPOSITORY-400-052 ",
            "The entity {0} retrieved from repository {1} during the {2} operation has invalid contents: {3}",
            "The system is unable continue processing the request.",
            "This error suggests there is a logic error in either this repository, or the home repository for the instance.  Raise a Jira to get this fixed."),
    INVALID_RELATIONSHIP_FROM_STORE(400, "OMRS-REPOSITORY-400-053 ",
            "The relationship {0} retrieved from repository {1} during the {2} operation has invalid contents: {3}",
            "The system is unable continue processing the request.",
            "This error suggests there is a logic error in either this repository, or the home repository for the instance.  Raise a Jira to get this fixed."),
    NULL_INSTANCE_METADATA_COLLECTION_ID(400, "OMRS-REPOSITORY-400-054 ",
            "The instance {0} retrieved from repository {1} during the {2} operation has a null metadata collection id in its header: {3}",
            "The system is unable continue processing the request.",
            "This error suggests there is a logic error in either this repository, or the home repository for the instance.  Raise a Jira to get this fixed."),
    UNEXPECTED_EXCEPTION_FROM_COHORT(400, "OMRS-REPOSITORY-400-055 ",
            "An unexpected exception was received from a repository connector during the {0} operation which had message: {1}",
            "The system is unable continue processing the request.",
            "This error suggests there is a logic error in either this repository, or the home repository for the instance.  Raise a Jira to get this fixed."),
    NO_HOME_FOR_INSTANCE(400, "OMRS-REPOSITORY-400-055 ",
            "The OMRS repository connector operation {0} from the OMRS Enterprise Repository Services can not locate the home repository connector for instance {1} located in metadata collection {2}",
            "The system is unable continue processing the request.",
            "This error suggests there is a logic error in either this repository, or the home repository for the instance.  Raise a Jira to get this fixed."),
    NULL_USER_NAME(400, "OMRS-REST-API-400-001 ",
            "The OMRS REST API for server {0} has been called with a null user name (userId)",
            "The system is unable to access the local metadata repository.",
            "The user name is supplied by the caller to the API. This call needs to be corrected before the server can operate correctly."),
    NO_MORE_ELEMENTS(400, "OMRS-PROPERTIES-400-001 ",
            "No more elements in {0} iterator",
            "A caller stepping through an iterator has requested more elements when there are none left.",
            "Recode the caller to use the hasNext() method to check for more elements before calling next() and then retry."),
    NULL_CLASSIFICATION_PROPERTY_NAME(400, "OMRS-PROPERTIES-400-002 ",
            "No name provided for entity classification",
            "A classification with a null name is assigned to an entity.   This value should come from a metadata repository, and always be filled in.",
            "Look for other error messages to identify the source of the problem.  Identify the metadata repository where the asset came from.  Correct the cause of the error and then retry."),
    NULL_PROPERTY_NAME(400, "OMRS-PROPERTIES-400-003 ",
            "Null property name passed to properties object",
            "A request to set an additional property failed because the property name passed was null",
            "Recode the call to the property object with a valid property name and retry."),
    ARRAY_OUT_OF_BOUNDS(400, "OMRS-PROPERTIES-400-004 ",
            "{0} is unable to add a new element to location {1} of an array of size {2} value",
            "There is an error in the update of an ArrayPropertyValue.",
            "Recode the call to the property object with a valid element location and retry."),
    BAD_ATTRIBUTE_TYPE(400, "OMRS-PROPERTIES-400-005 ",
            "AttributeDefs may only be of primitive, collection or enum type. {0} of category {1} is not allowed.",
            "There is an error in the creation of an AttributeDefType.",
            "Recode the call to the AttributeDefType object with a valid type."),
    REPOSITORY_URL_MALFORMED(400, "OMRS-CONNECTOR-400-001 ",
            "The Open Metadata Repository Server URL {0} is not in a recognized format",
            "The system is unable to connect to the open metadata repository to retrieve metadata properties.",
            "Retry the request when the connection configuration for this repository is corrected."),
    NULL_CONNECTION(400, "OMRS-CONNECTOR-400-002 ",
            "The connection passed to OMASConnectedAssetProperties for connector {0} is null.",
            "The system is unable to populate the ConnectedAssetProperties object because it needs the connection to identify the asset.",
            "Look for other error messages to identify what caused this error.  When the issue is resolved, retry the request."),
    NULL_OMRS_CONNECTION(400, "OMRS-CONNECTOR-400-003 ",
            "The connection passed to the EnterpriseOMRSRepositoryConnector is null.",
            "The system is unable to populate the EnterpriseOMRSRepositoryConnector object because it needs the connection to identify the repository.",
            "Look for other error messages to identify what caused this error.  When the issue is resolved, retry the request."),
    INVALID_OMRS_CONNECTION(400, "OMRS-CONNECTOR-400-004 ",
            "The connection {0} passed to the EnterpriseOMRSRepositoryConnector is invalid.",
            "The system is unable to populate the EnterpriseOMRSRepositoryConnector object because it needs the connection to identify the repository.",
            "Look for other error messages to identify what caused this error.  When the issue is resolved, retry the request."),
    NULL_TOPIC_CONNECTOR(400, "OMRS-TOPIC-CONNECTOR-400-001 ",
            "Unable to send or receive events for source {0} because the connector to the OMRS Topic failed to initialize",
            "The local server will not connect to the cohort.",
            "The connection to the connector is configured in the server configuration.  " +
                                 "Review previous error messages to determine the precise error in the " +
                                 "start up configuration. " +
                                 "Correct the configuration and reconnect the server to the cohort. "),
    NULL_REGISTRY_STORE(400, "OMRS-COHORT-REGISTRY-404-001 ",
            "The Open Metadata Repository Cohort Registry Store for cohort {0} is not available.",
            "The system is unable to process registration requests from the open metadata repository cohort.",
            "Correct the configuration for the registry store connection in the server configuration. " +
            "Retry the request when the registry store configuration is correct."),
    INVALID_LOCAL_METADATA_COLLECTION_ID(400, "OMRS-COHORT-REGISTRY-400-002 ",
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
    NULL_AUDIT_LOG_STORE(400, "OMRS-AUDIT-LOG-400-001 ",
            "The Audit Log Store for server {0} is not available.",
            "The system is unable to process any open metadata registry services (OMRS) requests because " +
                                 "the audit log for this server is unavailable.",
            "Correct the configuration for the audit log store connection in the server configuration. " +
                                "Retry the request when the audit log store configuration is correct."),
    NULL_ARCHIVE_STORE(400, "OMRS-ARCHIVE-MANAGER-400-001 ",
            "An open metadata archive configured for server {0} is not accessible.",
             "The system is unable to process the contents of this open metadata archive.  " +
                               "Other services may fail if they were dependent on this open metadata archive.",
             "Correct the configuration for the open metadata archive connection in the server configuration. " +
                                 "Retry the request when the open metadata archive configuration is correct."),
    NULL_EVENT_MAPPER(400, "OMRS-LOCAL-REPOSITORY-400-001 ",
             "The repository event mapper configured for the local repository for server {0} is not accessible.",
             "The system is unable to create the repository event mapper which means that events from the " +
                              "local repository will not be captured and processed.  " +
                              "Other services may fail if they were dependent on this event notification.",
             "Correct the configuration for the repository event mapper connection in the server configuration. " +
                               "Retry the request when the repository event mapper configuration is correct."),
    DUPLICATE_COHORT_NAME(400, "OMRS-METADATA-HIGHWAY-404-001 ",
            "There are more than one cohort configurations with the same name of {0}.",
            "The system is unable to connect to more than one cohort with the same name.",
            "Correct the configuration for the cohorts in the server configuration. " +
                                 "Retry the request when the cohort configuration is correct."),
    CONFLICTING_ENTERPRISE_TYPEDEFS(400, "OMRS-ENTERPRISE-REPOSITORY-400-001 ",
            "Conflicting TypeDefs have been detected.",
            "The system is unable to create a reliable list of TypeDefs for the enterprise.",
            "Details of the conflicts and the steps necessary to repair the situation can be found in the audit log. " +
                                  "Retry the request when the cohort configuration is correct."),
    NO_TYPEDEFS_DEFINED(400, "OMRS-ENTERPRISE-REPOSITORY-400-002 ",
            "No TypeDefs have been defined in any of the connected repositories.",
            "The system is unable to create a list of TypeDefs for the enterprise.",
            "Look for errors in the set up of the repositories in the audit log and verify that TypeDefs are configured. " +
                                            "Retry the request when the cohort configuration is correct."),
    DUPLICATE_TYPE_IN_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-001 ",
            "The same type {0} of category {1} has been added twice to an open metadata archive. First version was {2} and the second was {3}.",
            "The build of the archive terminates.",
            "Verify the definition of the types being added to the archive. Once the definitions have been corrected, rerun the request."),
    DUPLICATE_INSTANCE_IN_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-001 ",
            "The {0} instance {1} has been added twice to an open metadata archive. First version was {2} and the second was {3}.",
            "The build of the archive terminates.",
            "Verify the definition of the instance being added to the archive. Once the definitions have been corrected, rerun the request."),
    DUPLICATE_TYPENAME_IN_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-002 ",
            "The same type name {0} has been added twice to an open metadata archive. First version was {1} and the second was {2}.",
            "The build of the archive terminates.",
            "Verify the definition of the types being added to the archive. Once the definitions have been corrected, rerun the request."),
    DUPLICATE_GUID_IN_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-003 ",
            "The guid {0} has been used twice to an open metadata archive. First version was {1} and the second was {2}.",
            "The build of the archive terminates.",
            "Verify the definition of the elements being added to the archive. Once the definitions have been corrected, rerun the request."),
    MISSING_TYPE_IN_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-004 ",
            "The type {0} of category {1} is not found in an open metadata archive.",
            "The build of the archive terminates.",
            "Verify the definition of the elements being added to the archive. Once the definitions have been corrected, rerun the request."),
    MISSING_NAME_FOR_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-005 ",
            "A request for a type from category {0} passed a null name.",
            "The build of the archive terminates.",
            "Verify the definition of the elements being added to the archive. Once the definitions have been corrected, rerun the request."),
    DUPLICATE_ENDDEF1_NAME_IN_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-006 ",
            "endDef2 type {0} and EndDef1 name {1} in RelationshipDef {2} are incorrect, because entity {0} already has a {1} attribute or is referred to in an existing relationshipDef with an endDef with name {1}.",
            "The build of the archive terminates.",
            "Verify the definition of the types being added to the archive. Once the definitions have been corrected, rerun the request."),
    DUPLICATE_ENDDEF2_NAME_IN_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-007 ",
            "EndDef1 type {0} and EndDef2 name {1} in RelationshipDef {2} are incorrect, because entity {0} already has a {1} attribute or is referred to in an existing relationshipDef with an endDef with name {1}.",
            "The build of the archive terminates.",
            "Verify the definition of the types being added to the archive. Once the definitions have been corrected, rerun the request."),
    DUPLICATE_RELATIONSHIP_ATTR_IN_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-008 ",
            "Duplicate attribute name {0} is defined in RelationshipDef {2}. ",
            "The build of the archive terminates.",
            "Verify the definition of the types being added to the archive. Once the definitions have been corrected, rerun the request."),
    DUPLICATE_ENTITY_ATTR_IN_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-009 ",
            "Duplicate attribute name {0} is defined in EntityDef {2}. ",
            "The build of the archive terminates.",
            "Verify the definition of the types being added to the archive. Once the definitions have been corrected, rerun the request."),
    DUPLICATE_CLASSIFICATION_ATTR_IN_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-010 ",
            "Duplicate attribute name {0} is defined in ClassificationDef {2}. ",
            "The build of the archive terminates.",
            "Verify the definition of the types being added to the archive. Once the definitions have been corrected, rerun the request."),
    BLANK_TYPENAME_IN_ARCHIVE(400, "OMRS-ARCHIVEBUILDER-400-011 ",
            "Type name {0} is invalid because it contains a blank character. ",
            "The build of the archive terminates.",
            "Verify the definition of the types being added to the archive. Once the definitions have been corrected, rerun the request."),
    NULL_LOG_RECORD(400, "OMRS-AUDITLOG-400-001 ",
            "A null log record has been passed by the audit log to the audit log store.",
            "The audit log store throws an exception and the log record is not written to the audit log store.",
            "This is probably an internal error in the audit log.  Raise a jira to get this fixed."),
    REPOSITORY_NOT_AVAILABLE(404, "OMRS-REPOSITORY-404-001 ",
            "The open metadata repository connector for server {0} is not active and is unable to service the {1} request",
            "The system is unable to retrieve any metadata properties from this repository.",
            "Retry the request when the repository connector is active."),
    ENTITY_NOT_KNOWN(404, "OMRS-REPOSITORY-404-002 ",
            "The entity identified with guid \"{0}\" passed on the {1} call is not known to the open metadata repository {2}",
            "The system is unable to retrieve the properties for the requested entity because the supplied guid is not recognized.",
            "The guid is supplied by the caller to the server.  It may have a logic problem that has corrupted the guid, or the entity has been deleted since the guid was retrieved."),
    RELATIONSHIP_NOT_KNOWN(404, "OMRS-REPOSITORY-404-003 ",
            "The relationship identified with guid \"{0}\" passed on the {1} call is not known to the open metadata repository {2}",
            "The system is unable to retrieve the properties for the requested relationship because the supplied guid is not recognized.",
            "The guid is supplied by the caller to the OMRS.  It may have a logic problem that has corrupted the guid, or the relationship has been deleted since the guid was retrieved."),
    TYPEDEF_NOT_KNOWN(404, "OMRS-REPOSITORY-404-004 ",
            "The TypeDef \"{0}\" (guid = {1}) passed on the {2} parameter of the {3} operation is not known to the open metadata repository {4}",
            "The system is unable to retrieve the properties for the requested TypeDef because the supplied identifier is not recognized.",
            "The identifier is supplied by the caller.  It may have a logic problem that has corrupted the identifier, or the typedef has been deleted since the identifier was retrieved."),
    TYPEDEF_NOT_KNOWN_FOR_INSTANCE(404, "OMRS-REPOSITORY-404-005 ",
            "The TypeDef \"{0}\" of category \"{1}\" passed by the {2} operation is not known to the open metadata repository {3}",
            "The system is unable to retrieve the properties for the requested TypeDef because the supplied identifier is not recognized.",
            "The identifier is supplied by the caller.  It may have a logic problem that has corrupted the identifier, or the typedef has been deleted since the identifier was retrieved."),
    ATTRIBUTE_TYPEDEF_NOT_KNOWN(404, "OMRS-REPOSITORY-404-006 ",
            "The AttributeTypeDef \"{0}\" (guid = {1}) passed on the {2} call is not known to the open metadata repository {3}",
            "The system is unable to retrieve the properties for the requested TypeDef because the supplied identifier is not recognized.",
            "The identifier is supplied by the caller.  It may have a logic problem that has corrupted the identifier, or the typedef has been deleted since the identifier was retrieved."),
    TYPEDEF_ID_NOT_KNOWN(404, "OMRS-REPOSITORY-404-007 ",
            "The TypeDef unique identifier {0} passed as parameter {1} on a {2} request to open metadata repository {3} is not known to this repository",
            "The system is unable to retrieve the properties for the requested TypeDef because the supplied identifiers are not recognized.",
            "The identifier is supplied by the caller.  It may have a logic problem that has corrupted the identifier, or the TypeDef has been deleted since the identifier was retrieved."),
    ATTRIBUTE_TYPEDEF_ID_NOT_KNOWN(404, "OMRS-REPOSITORY-404-008 ",
            "The AttributeTypeDef \"{0}\" (guid {1}) passed on a {2} request to open metadata repository {3} is not known to this repository",
            "The system is unable to retrieve the properties for the requested AttributeTypeDef because the supplied identifiers are not recognized.",
            "The identifier is supplied by the caller.  It may have a logic problem that has corrupted the identifier, or the AttributeTypeDef has been deleted since the identifier was retrieved."),
    TYPEDEF_NAME_NOT_KNOWN(404, "OMRS-REPOSITORY-404-009 ",
            "The TypeDef unique name {0} passed on a {1} request to open metadata repository {2} is not known to this repository",
            "The system is unable to retrieve the properties for the requested TypeDef because the supplied identifiers are not recognized.",
            "The identifier is supplied by the caller.  It may have a logic problem that has corrupted the identifier, or the TypeDef has been deleted since the identifier was retrieved."),
    ATTRIBUTE_TYPEDEF_NAME_NOT_KNOWN(404, "OMRS-REPOSITORY-404-010 ",
            "The TypeDef unique name {0} passed on a {1} request to open metadata repository {2} is not known to this repository",
            "The system is unable to retrieve the properties for the requested TypeDef because the supplied identifiers are not recognized.",
            "The identifier is supplied by the caller.  It may have a logic problem that has corrupted the identifier, or the TypeDef has been deleted since the identifier was retrieved."),
    COHORT_NOT_CONNECTED(404, "OMRS-REPOSITORY-CONNECTOR-404-002 ",
            "The Open Metadata Repository Servers in the cohort are not available",
            "The system is unable to retrieve any metadata properties from this repository.",
            "Retry the request when the repository server is available."),
    INVALID_COHORT_CONFIG(404, "OMRS-REPOSITORY-CONNECTOR-404-003 ",
            "The open metadata repository servers in the cohort are not configured correctly",
            "The root cause of this error is recorded in previous exceptions.",
            "Review the other error messages to determine the source of the error.  When these are resolved, retry the request."),
    METADATA_HIGHWAY_NOT_AVAILABLE(404, "OMRS-METADATA-HIGHWAY-404-001 ",
            "The local server's metadata highway communication components are failing to initialize",
            "The root cause of this error is recorded in previous exceptions.",
            "Review the other error messages to determine the source of the error.  When these are resolved, retry the request."),
    COHORT_DISCONNECT_FAILED(404, "OMRS-METADATA-HIGHWAY-404-002 ",
            "The local server is unable to disconnect from an open metadata repository cohort {0}",
            "The root cause of this error is recorded in previous exceptions.",
            "Review the other error messages to determine the source of the error.  When these are resolved, retry the request."),
    TOPIC_CONNECTOR_NOT_AVAILABLE(404, "OMRS-TOPIC-CONNECTOR-404-001 ",
            "The OMRS Topic Connector is not available",
            "The system is not able to process events sent between repositories in the open metadata cohort.",
            "Correct the configuration for the OMRS Topic Connector (in the OMRS Configuration). Retry the request when the topic connector configuration is correct."),
    REMOTE_REPOSITORY_ERROR(404, "OMRS-REST-REPOSITORY-CONNECTOR-404-001 ",
            "A call to the {0} of the open metadata repository server {1} results in an exception with message {2}",
            "The system is unable to retrieve any metadata properties from this repository.",
            "Retry the request when the repository server is available."),
    ENTERPRISE_NOT_SUPPORTED(405, "OMRS-ENTERPRISE-REPOSITORY-CONNECTOR-405-001 ",
            "The requested method {0} is not supported by the EnterpriseOMRSRepositoryConnector",
            "The system is not able to process the requested method because it is not supported by the " +
                                     "Open Metadata Repository Services (OMRS) Enterprise Repository Services.",
            "Correct the application that called this method."),
    INVALID_PRIMITIVE_CLASS_NAME(500, "OMRS-METADATA-COLLECTION-500-001 ",
            "The Java class \'{0}\' for PrimitiveDefCategory {1} is not known",
            "There is an internal error in Java class PrimitiveDefCategory as it has been set up with an invalid class.",
            "Raise a Jira to get this fixed."),
    INVALID_PRIMITIVE_VALUE(500, "OMRS-METADATA-COLLECTION-500-002 ",
            "The primitive value should be stored in Java class \'{0}\' since it is of PrimitiveDefCategory {1}",
            "There is an internal error in the creation of a PrimitiveTypeValue.",
            "Raise a Jira to get this fixed."),
    INVALID_PRIMITIVE_CATEGORY(500, "OMRS-METADATA-COLLECTION-500-003 ",
            "There is a problem in the definition of primitive type {0}.",
            "There is an internal error in the creation of a PrimitiveTypeValue.",
            "Raise a Jira to get this fixed."),
    NULL_HOME_METADATA_COLLECTION_ID(500, "OMRS-METADATA-COLLECTION-500-004 ",
            "Null home metadata collection identifier found in metadata instance {0} from open metadata repository {1}",
            "A request to retrieve a metadata instance (entity or relationship) has encountered a homeless metadata instance.",
            "Locate the open metadata repository that supplied the instance and correct the logic in its OMRSRepositoryConnector."),
    NULL_CONFIG(500, "OMRS-OPERATIONAL-SERVICES-500-001 ",
            "No configuration has been passed to the Open Metadata Repository Services (OMRS) on initialization",
            "here is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NULL_METADATA_COLLECTION_ID(500, "OMRS-METADATA-COLLECTION-500-004 ",
            "The open metadata repository connector \"{0}\" has been initialized with a null metadata collection identifier",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    BAD_INTERNAL_PAGING(500, "OMRS-METADATA-COLLECTION-500-005 ",
            "Open metadata repository {0} has used an invalid paging parameter during the {1} operation",
            "There is an internal error in the OMRS repository connector.",
            "Raise a Jira to get this fixed."),
    BAD_INTERNAL_SEQUENCING(500, "OMRS-METADATA-COLLECTION-500-005 ",
            "Open metadata repository {0} has used an invalid sequencing parameter during the {1} operation",
            "There is an internal error in the OMRS repository connector.",
            "Raise a Jira to get this fixed."),
    NO_REPOSITORY_CONNECTOR_FOR_COLLECTION(500, "OMRS-METADATA-COLLECTION-500-006 ",
            "Unable to complete operation {0} to open metadata repository {1} because the repository connector is null",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NO_REPOSITORY_VALIDATOR_FOR_COLLECTION(500, "OMRS-METADATA-COLLECTION-500-007 ",
            "Unable to complete operation {0} to open metadata repository {1} because the repository validator is null",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NO_REPOSITORY_HELPER_FOR_COLLECTION(500, "OMRS-METADATA-COLLECTION-500-008 ",
            "Unable to complete operation {0} to open metadata repository {1} because the repository connector is null",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    REPOSITORY_LOGIC_ERROR(500, "OMRS-METADATA-COLLECTION-500-009 ",
            "Open metadata repository {0} has encountered an unexpected exception during the {1} operation.  The message was {2}",
            "There is an internal error in the OMRS repository connector.",
            "Raise a Jira to get this fixed."),
    NULL_INSTANCE_TYPE(500, "OMRS-METADATA-COLLECTION-500-010 ",
           "During the {0} operation, open metadata repository {1} retrieved an instance from its metadata store that has a null type",
           "There is an internal error in the OMRS repository connector.",
           "Raise a Jira to get this fixed."),
    INACTIVE_INSTANCE_TYPE(500, "OMRS-METADATA-COLLECTION-500-011 ",
           "During the {0} operation, open metadata repository {1} retrieved an instance (guid={2}) from its metadata store that has an inactive type called {3} (type guid = {4})",
           "There is an internal error in the OMRS repository connector.",
           "Raise a Jira to get this fixed."),
    NULL_COHORT_NAME(500, "OMRS-COHORT-MANAGER-500-001 ",
            "OMRSCohortManager has been initialized with a null cohort name",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NULL_LOCAL_METADATA_COLLECTION(500, "OMRS-LOCAL-REPOSITORY-500-001 ",
            "The local repository services have been initialized with a null real metadata collection.",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NULL_ENTERPRISE_METADATA_COLLECTION(500, "OMRS-ENTERPRISE-REPOSITORY-500-001 ",
            "The enterprise repository services has detected a repository connector with a null metadata collection.",
            "There is an internal error in the Open Metadata Repository Services (OMRS) operation.",
            "Raise a Jira to get this fixed."),
    BAD_TYPEDEF(500, "OMRS-CONTENT-MANAGER-500-001 ",
            "The repository content manager has detected an invalid TypeDef from {0}.",
            "There is an internal error in the Open Metadata Repository Services (OMRS) operation.",
            "Raise a Jira to get this fixed."),
    BAD_TYPEDEF_ATTRIBUTE_NAME(500, "OMRS-CONTENT-MANAGER-500-002 ",
            "The repository content manager has detected an invalid attribute name in a TypeDef from {0}.",
            "There is an internal error in the Open Metadata Repository Services (OMRS) operation.",
            "Raise a Jira to get this fixed."),
    NULL_TYPEDEF_ATTRIBUTE(500, "OMRS-CONTENT-MANAGER-500-003 ",
            "The repository content manager has detected a null attribute in a TypeDef from {0}.",
            "There is an internal error in the Open Metadata Repository Services (OMRS) operation.",
            "Raise a Jira to get this fixed."),
    BAD_CATEGORY_FOR_TYPEDEF_ATTRIBUTE(500, "OMRS-CONTENT-MANAGER-500-004 ",
            "Source {0} has requested type {1} with an incompatible category of {2} from repository content manager.",
            "There is an error in the Open Metadata Repository Services (OMRS) operation - probably in the source component.",
            "Raise a Jira to get this fixed."),
    ARCHIVE_UNAVAILABLE(500, "OMRS-OPEN-METADATA-TYPES-500-001 ",
            "The archive builder failed to initialize.",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NULL_EXCHANGE_RULE(500, "OMRS-EVENT-MANAGEMENT-500-001 ",
            "A null exchange rule has been passed to one of the event management components on method {0}.",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NULL_REPOSITORY_VALIDATOR(500, "OMRS-EVENT-MANAGEMENT-500-002 ",
            "A null repository validator has been passed to one of the event management components.",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    NULL_REPOSITORY_HELPER(500, "OMRS-EVENT-MANAGEMENT-500-003 ",
            "A null repository helper has been passed to one of the event management components.",
            "There is an internal error in the OMRS initialization.",
            "Raise a Jira to get this fixed."),
    METADATA_COLLECTION_ID_MISMATCH(500, "OMRS-REST-REPOSITORY-CONNECTOR-500-001 ",
            "A remote open metadata repository {0} returned a metadata collection identifier of {1} on its REST API after it registered with the cohort using a metadata collection identifier of {2}",
            "There is a configuration error in the remote open metadata repository.",
            "Review the set up of the remote repository.  It may be that the server-url-root parameter is incorrectly set and is clashing with the setting in another server registered with the same cohort."),
    NULL_REMOTE_METADATA_COLLECTION_ID(500, "OMRS-REST-REPOSITORY-CONNECTOR-500-002 ",
            "A remote open metadata repository {0} returned a null metadata collection identifier on its REST API.  It registered with the cohort using a metadata collection identifier of {1}",
            "There is an internal error in the remote open metadata repository.",
             "Raise a Jira to get this fixed."),
    METHOD_NOT_IMPLEMENTED(501, "OMRS-METADATA-COLLECTION-501-001 ",
            "OMRSMetadataCollection method {0} for OMRS Connector {1} to repository type {2} is not implemented.",
            "A method in MetadataCollectionBase was called which means that the connector's OMRSMetadataCollection " +
                                   "(a subclass of MetadataCollectionBase) does not have a complete implementation.",
            "Raise a Jira to get this fixed."),
    NO_REPOSITORIES(503, "OMRS-ENTERPRISE-REPOSITORY-503-001 ",
            "There are no open metadata repositories available for access service {0}.",
            "The configuration for the server is set up so there is no local repository and no remote repositories " +
                            "connected through the open metadata repository cohorts.  " +
                            "This may because of one or more configuration errors.",
            "Retry the request once the configuration is changed."),
    ENTERPRISE_DISCONNECTED(503, "OMRS-ENTERPRISE-REPOSITORY-503-002 ",
            "The enterprise repository services are disconnected from the open metadata repositories.",
            "The server has shutdown (or failed to set up) access to the open metadata repositories.",
            "Retry the request once the open metadata repositories are connected."),
    NULL_COHORT_METADATA_COLLECTION(503, "OMRS-ENTERPRISE-REPOSITORY-503-003 ",
            "The enterprise repository services has detected a repository connector from cohort {0} for metadata collection identifier {1} that has a null metadata collection API object.",
            "There is an internal error in the OMRS Repository Connector implementation.",
            "Raise a Jira to get this fixed."),
    NULL_CONTENT_MANAGER(503, "OMRS-LOCAL-REPOSITORY-503-001 ",
            "A null repository content manager has been passed to one of the local repository's components on method {0}.",
            "There is an internal error in the OMRS Local Repository Connector implementation, or the way it has been initialized.",
            "Raise a Jira to get this fixed."),
    NULL_SOURCE_NAME(503, "OMRS-LOCAL-REPOSITORY-503-002 ",
            "A null repository content manager has been passed to one of the local repository's components on method {0}.",
            "There is an internal error in the OMRS Local Repository Connector implementation, or the way it has been initialized.",
            "Raise a Jira to get this fixed."),
    LOCAL_REPOSITORY_CONFIGURATION_ERROR(503, "OMRS-LOCAL-REPOSITORY-503-003 ",
            "The connection to the local open metadata repository server is not configured correctly",
            "The root cause of this error is recorded in previous exceptions.",
            "Review the other error messages to determine the source of the error.  When these are resolved, retry the request."),
    BAD_LOCAL_REPOSITORY_CONNECTION(503, "OMRS-LOCAL-REPOSITORY-503-004 ",
            "The connection to the local open metadata repository server is not configured correctly",
            "The root cause of this error is recorded in previous exceptions.",
            "Review the other error messages to determine the source of the error.  When these are resolved, retry the request."),
    VALIDATION_LOGIC_ERROR(503, "OMRS-LOCAL-REPOSITORY-503-005 ",
             "An OMRS repository connector or access service {0} has passed an invalid parameter to the repository validator {1} operation as part of the {2} request",
             "The open metadata component has called the repository validator operations in the wrong order or has a similar logic error.",
             "Raise a Jira to get this fixed."),
    HELPER_LOGIC_ERROR(503, "OMRS-LOCAL-REPOSITORY-503-006 ",
             "An OMRS repository connector {0} has passed an invalid parameter to the repository helper {1} operation as part of the {2} request",
             "The open metadata component has called the repository helper operations in the wrong order or has a similar logic error.",
             "Raise a Jira to get this fixed."),
    CONTENT_MANAGER_LOGIC_ERROR(503, "OMRS-LOCAL-REPOSITORY-503-007 ",
             "An OMRS repository connector {0} has passed an invalid parameter to the repository content manager {1} operation as part of the {2} request",
             "The open metadata component has called the repository helper operations in the wrong order or has a similar logic error.",
             "Raise a Jira to get this fixed."),
    NULL_CLASSIFICATION_CREATED(503, "OMRS-LOCAL-REPOSITORY-503-008 ",
             "An OMRS repository connector or access server {0} has passed a null classification to the repository helper {1} operation as part of the {2} request",
             "The repository connector has called the repository helper operations in the wrong order or has a similar logic error.",
             "Raise a Jira to get this fixed."),
    NO_LOCAL_REPOSITORY(503, "OMRS-REST-API-503-001 ",
            "There is no local repository to support REST API call {0}",
            "The server has received a call on its open metadata repository REST API services but is unable to process it because the local repository is not active.",
            "If the server is supposed to have a local repository correct the server configuration and restart the server."),
    NULL_RESPONSE_FROM_API(503, "OMRS-REST-API-503-002 ",
            "A null response was received from REST API call {0} to repository {1}",
            "The server has issued a call to the open metadata repository REST API services in a remote repository and has received a null response.",
            "Look for errors in the remote repository's audit log and console to understand and correct the source of the error."),
    CLIENT_SIDE_REST_API_ERROR(503, "OMRS-REST-API-503-003 ",
            "A client-side exception was received from API call {0} to repository {1}.  The error message was {2}",
            "The server has issued a call to the open metadata repository REST API services in a remote repository and has received an exception from the local client libraries.",
            "Look for errors in the local repository's audit log and console to understand and correct the source of the error.")

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

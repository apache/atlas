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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Arrays;

/**
 * The OMRSAuditCode is used to define the message content for the OMRS Audit Log.
 *
 * The 5 fields in the enum are:
 * <ul>
 *     <li>Log Message Id - to uniquely identify the message</li>
 *     <li>Severity - is this an event, decision, action, error or exception</li>
 *     <li>Log Message Text - includes placeholder to allow additional values to be captured</li>
 *     <li>Additional Information - further parameters and data relating to the audit message (optional)</li>
 *     <li>SystemAction - describes the result of the situation</li>
 *     <li>UserAction - describes how a user should correct the situation</li>
 * </ul>
 */
public enum OMRSAuditCode
{
    OMRS_INITIALIZING("OMRS-AUDIT-0001",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The Open Metadata Repository Services (OMRS) is initializing",
                      "The local server has started up the OMRS.",
                      "No action is required.  This is part of the normal operation of the server."),

    ENTERPRISE_ACCESS_INITIALIZING("OMRS-AUDIT-0002",
                      OMRSAuditLogRecordSeverity.INFO,
                      "Enterprise access through the Enterprise Repository Services is initializing",
                      "The local server has started the enterprise access support provided by the " +
                                           "enterprise repository services.",
                      "No action is required.  This is part of the normal operation of the server."),

    LOCAL_REPOSITORY_INITIALIZING("OMRS-AUDIT-0003",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The local repository is initializing with metadata collection id {0}",
                      "The local server has started to initialize the local repository.",
                      "No action is required.  This is part of the normal operation of the server."),

    METADATA_HIGHWAY_INITIALIZING("OMRS-AUDIT-0004",
                      OMRSAuditLogRecordSeverity.INFO,
                      "Connecting to the metadata highway",
                      "The local server has started to initialize the communication with the open metadata " +
                                          "repository cohorts.",
                      "No action is required.  This is part of the normal operation of the server."),

    COHORT_INITIALIZING("OMRS-AUDIT-0005",
                      OMRSAuditLogRecordSeverity.INFO,
                      "Connecting to cohort {0}",
                      "The local server has started to initialize the communication with the named " +
                                "open metadata repository cohort.",
                      "No action is required.  This is part of the normal operation of the server."),

    COHORT_CONFIG_ERROR("OMRS-AUDIT-0006",
                      OMRSAuditLogRecordSeverity.EXCEPTION,
                      "Configuration error detected while connecting to cohort {0}, error message was: {2}",
                      "The local server has started to initialize the communication with the named " +
                                "open metadata repository cohort and a configuration error was detected.",
                      "Review the exception and resolve the issue it documents. " +
                                "Then disconnect and reconnect this server to the cohort."),

    OMRS_INITIALIZED("OMRS-AUDIT-0007",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The Open Metadata Repository Services (OMRS) has initialized",
                      "The local server has completed the initialization of the OMRS.",
                      "No action is required.  This is part of the normal operation of the server."),

    COHORT_DISCONNECTING("OMRS-AUDIT-0008",
                      OMRSAuditLogRecordSeverity.INFO,
                      "Disconnecting from cohort {0}",
                      "The local server has started to shutdown the communication with the named " +
                                 "open metadata repository cohort.",
                      "No action is required.  This is part of the normal operation of the server."),

    COHORT_PERMANENTLY_DISCONNECTING("OMRS-AUDIT-0009",
                      OMRSAuditLogRecordSeverity.INFO,
                      "Unregistering from cohort {0}",
                      "The local server has started to unregister from all future communication with the named " +
                                 "open metadata repository cohort.",
                      "No action is required.  This is part of the normal operation of the server."),

    OMRS_DISCONNECTING("OMRS-AUDIT-0010",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The Open Metadata Repository Services (OMRS) is disconnecting the open metadata repositories",
                      "The local server has completed the initialization of the OMRS.",
                      "No action is required.  This is part of the normal operation of the server."),

    OMRS_DISCONNECTED("OMRS-AUDIT-0011",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The Open Metadata Repository Services (OMRS) has disconnected from the open metadata repositories",
                      "The local server has completed the disconnection of the OMRS.",
                      "No action is required.  This is part of the normal operation of the server."),

    NO_LOCAL_REPOSITORY("OMRS-AUDIT-0012",
                      OMRSAuditLogRecordSeverity.INFO,
                      "No events will be sent to the open metadata repository cohort {0} because the local metadata collection id is null",
                      "The local server will not send outbound events because there is no local metadata repository.",
                      "Validate that the server is configured without a local metadata repository.  " +
                                "If there should be a metadata repository then, verify the server configuration is" +
                                "correct and look for errors that have occurred during server start up." +
                                "If necessary, correct the configuration and restart the server."),

    LOCAL_REPOSITORY_FAILED_TO_START("OMRS-AUDIT-0013",
                      OMRSAuditLogRecordSeverity.EXCEPTION,
                      "Unable to start processing in the local repository due to error {0}",
                      "The local server will not process events.",
                      "Review previous error messages to determine the precise error in the " +
                                 "start up configuration. " +
                                 "Correct the configuration and restart the server. "),

    LOCAL_REPOSITORY_FAILED_TO_DISCONNECT("OMRS-AUDIT-0014",
                      OMRSAuditLogRecordSeverity.EXCEPTION,
                      "Unable to disconnect processing in the local repository due to error {0}",
                      "The local server may not shutdown cleanly.",
                      "Review previous error messages to determine the precise error. Correct the cause and restart the server. "),

    OMRS_TOPIC_LISTENER_START("OMRS-AUDIT-0015",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The listener thread for an OMRS Topic Connector for topic {0} has started",
                      "The listener thread will process inbound events",
                      "No action is required.  This is part of the normal operation of the server."),

    OMRS_TOPIC_LISTENER_SHUTDOWN("OMRS-AUDIT-0016",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The listener thread for the OMRS Topic Connector for topic {0} has shutdown",
                      "The listener thread will process inbound events",
                      "No action is required.  This is part of the normal operation of the server."),

    NULL_TOPIC_CONNECTOR("OMRS-AUDIT-0017",
                      OMRSAuditLogRecordSeverity.EXCEPTION,
                      "Unable to send or receive events for cohort {0} because the connector to the OMRS Topic failed to initialize",
                      "The local server will not connect to the cohort.",
                      "The connection to the connector is configured in the server configuration.  " +
                                 "Review previous error messages to determine the precise error in the " +
                                 "start up configuration. " +
                                 "Correct the configuration and reconnect the server to the cohort. "),

    NEW_ENTERPRISE_CONNECTOR("OMRS-AUDIT-0018",
                      OMRSAuditLogRecordSeverity.INFO,
                      "An enterprise OMRS Connector has been created for the {0} open metadata access service",
                      "The connector will support access to the connected open metadata repositories.",
                      "No action is required.  This is part of the normal operation of the server."),

    STARTING_ENTERPRISE_CONNECTOR("OMRS-AUDIT-0019",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The enterprise OMRS Connector for the {0} open metadata access service has started",
                      "The connector will support access to the connected open metadata repositories.",
                      "No action is required.  This is part of the normal operation of the server."),

    DISCONNECTING_ENTERPRISE_CONNECTOR("OMRS-AUDIT-0020",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The enterprise OMRS Connector for the {0} open metadata access service has shutdown",
                      "The connector will support access to the connected open metadata repositories.",
                      "No action is required.  This is part of the normal operation of the server."),

    ENTERPRISE_CONNECTOR_FAILED("OMRS-AUDIT-0019",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The create of an enterprise OMRS Connector for the {0} open metadata access service failed with this error message: {1}",
                      "The connector will support access to the connected open metadata repositories.",
                      "No action is required.  This is part of the normal operation of the server."),

    PROCESSING_ARCHIVE("OMRS-AUDIT-0050",
                       OMRSAuditLogRecordSeverity.INFO,
                       "The Open Metadata Repository Services (OMRS) is about to process open metadata archive {0}",
                       "The local server is about to local types and instances from an open metadata archive.",
                       "No action is required.  This is part of the normal operation of the server."),

    EMPTY_ARCHIVE("OMRS-AUDIT-0051",
                       OMRSAuditLogRecordSeverity.ERROR,
                       "The Open Metadata Repository Services (OMRS) is unable to process an open metadata archive because it is empty",
                       "The local server is skipping an open metadata archive because it is empty.",
                       "Review the list of archives for the server and determine which archive is in error.  " +
                          "Request a new version of the archive or remove it from the server's archive list."),

    NULL_PROPERTIES_IN_ARCHIVE("OMRS-AUDIT-0052",
                        OMRSAuditLogRecordSeverity.ERROR,
                        "The Open Metadata Repository Services (OMRS) is unable to process an open metadata archive because it is empty",
                        "The local server is skipping an open metadata archive because it is empty.",
                        "Review the list of archives for the server and determine which archive is in error. " +
                           "Request a new version of the archive or remove it from the server's archive list."),

    COMPLETED_ARCHIVE("OMRS-AUDIT-0053",
                       OMRSAuditLogRecordSeverity.INFO,
                       "The Open Metadata Repository Services (OMRS) has loaded {0} types and {1} instances from open metadata archive {2}",
                       "The local server has completed the processing of the open metadata archive.",
                       "No action is required.  This is part of the normal operation of the server."),

    EVENT_PROCESSING_ERROR("OMRS-AUDIT-0100",
                      OMRSAuditLogRecordSeverity.EXCEPTION,
                      "Unable process an incoming event {0} due to exception {1}",
                      "The information in the event is not available to the server.",
                      "Review the exception to determine the source of the error and correct it. "),

    REGISTERED_WITH_COHORT("OMRS-AUDIT-0101",
                      OMRSAuditLogRecordSeverity.INFO,
                      "Registering with open metadata repository cohort {0} using metadata collection id {1}",
                      "The local server has sent a registration event to the other members of the cohort.",
                           "No action is required.  This is part of the normal operation of the server."),

    RE_REGISTERED_WITH_COHORT("OMRS-AUDIT-0102",
                      OMRSAuditLogRecordSeverity.INFO,
                      "Refreshing registration information from open metadata repository cohort {0}",
                      "The local server has sent a registration refresh request to the other members of the cohort as " +
                                      "part of its routine to re-connect with the open metadata repository cohort.",
                      "No action is required.  This is part of the normal operation of the server."),

    UNREGISTERING_FROM_COHORT("OMRS-AUDIT-0103",
                      OMRSAuditLogRecordSeverity.INFO,
                      "Unregistering with open metadata repository cohort {0} using metadata collection id {1}",
                      "The local server has sent a unregistration event to the other members of the cohort as " +
                                      "part of its routine to permanently disconnect with the open metadata repository cohort.",
                      "No action is required.  This is part of the normal operation of the server."),

    SEND_REGISTRY_EVENT_ERROR("OMRS-AUDIT-0104",
                      OMRSAuditLogRecordSeverity.EXCEPTION,
                      "Unable to send a registry event for cohort {0} due to an error in the OMRS Topic Connector",
                      "The local server is unable to properly manage registration events for the metadata " +
                                      "repository cohort. The cause of the error is recorded in the accompanying exception.",
                      "Review the exception and resolve the issue it documents.  " +
                                      "Then disconnect and reconnect this server to the cohort."),

    REFRESHING_REGISTRATION_WITH_COHORT("OMRS-AUDIT-0105",
                      OMRSAuditLogRecordSeverity.INFO,
                      "Refreshing registration with open metadata repository cohort {0} using " +
                                                "metadata collection id {1} at the request of server {2}",
                      "The local server has sent a re-registration event to the other members of the cohort in " +
                                                "response to a registration refresh event from another member of the cohort.",
                      "No action is required.  This is part of the normal operation of the server."),

    INCOMING_CONFLICTING_LOCAL_METADATA_COLLECTION_ID("OMRS-AUDIT-0106",
                      OMRSAuditLogRecordSeverity.ACTION,
                      "Registration request for this server in cohort {0} was rejected by server {1} that " +
                                                        "hosts metadata collection {2} because the local metadata " +
                                                        "collection id {3} is not unique for this cohort",
                      "The local server will not receive metadata from the open metadata repository " +
                                                        "with the same metadata collection id. " +
                                                        "There is a chance of metadata integrity issues since " +
                                                        "a metadata instance can be updated in two places.",
                      "It is necessary to update the local metadata collection Id to remove the conflict."),

    INCOMING_CONFLICTING_METADATA_COLLECTION_ID("OMRS-AUDIT-0107",
                      OMRSAuditLogRecordSeverity.ACTION,
                      "Two servers in cohort {0} are using the same metadata collection identifier {1}",
                      "The local server will not be able to distinguish ownership of metadata " +
                                                        "from these open metadata repositories" +
                                                        "with the same metadata collection id. " +
                                                        "There is a chance of metadata integrity issues since " +
                                                        "a metadata instance can be updated in two places.",
                      "It is necessary to update the local metadata collection Id to remove the conflict."),

    INCOMING_BAD_CONNECTION("OMRS-AUDIT-0108",
                      OMRSAuditLogRecordSeverity.ACTION,
                      "Registration error occurred in cohort {0} because remote server {1} that hosts " +
                                    "metadata collection {2} is unable to use connection {3} to create a " +
                                    "connector to this local server",
                      "The remote server will not be able to query metadata from this local server.",
                      "This error may be caused because the connection is incorrectly " +
                                    "configured, or that the jar file for the connector is not available in the remote server."),

    NEW_MEMBER_IN_COHORT("OMRS-AUDIT-0109",
                      OMRSAuditLogRecordSeverity.INFO,
                      "A new registration request has been received for cohort {0} from server {1} that " +
                                 "hosts metadata collection {2}",
                      "The local server will process the registration request and if the parameters are correct, " +
                                 "it will accept the new member.",
                      "No action is required.  This is part of the normal operation of the server."),

    MEMBER_LEFT_COHORT("OMRS-AUDIT-0110",
                      OMRSAuditLogRecordSeverity.INFO,
                      "Server {0} hosting metadata collection {1} has left cohort {2}",
                      "The local server will process the incoming unregistration request and if the parameters are correct, " +
                               "it will remove the former member from its cohort registry store.",
                      "No action is required.  This is part of the normal operation of the server. " +
                               "Any metadata from this remote repository that is stored in the local repository will no longer be updated.  " +
                               "It may be kept in the local repository for reference or removed by calling the administration REST API."),

    REFRESHED_MEMBER_IN_COHORT("OMRS-AUDIT-0111",
                      OMRSAuditLogRecordSeverity.INFO,
                      "A re-registration request has been received for cohort {0} from server {1} that " +
                                       "hosts metadata collection {2}",
                      "The local server will process the registration request and if the parameters are correct, " +
                                       "it will accept the latest registration values for this member.",
                      "No action is required.  This is part of the normal operation of the server."),

    OUTGOING_CONFLICTING_METADATA_COLLECTION_ID("OMRS-AUDIT-0112",
                      OMRSAuditLogRecordSeverity.ACTION,
                      "Registration request received from cohort {0} was rejected by the " +
                                                        "local server because the remote server {1} is using a metadata " +
                                                        "collection Id {2} that is not unique in the cohort",
                      "The remote server will not exchange metadata with this local server.",
                      "It is necessary to update the TypeDef to remove the conflict " +
                                                        "before the remote server will exchange metadata with this server."),

    OUTGOING_BAD_CONNECTION("OMRS-AUDIT-0113",
                      OMRSAuditLogRecordSeverity.ACTION,
                      "Registration error occurred in cohort {0} because the local server is not able to use " +
                                    "the remote connection {1} supplied by server {2} that hosts metadata " +
                                    "collection {3} to create a connector to its metadata repository",
                      "The local server is not able to query metadata from the remote server.",
                      "This error may be caused because the connection is incorrectly " +
                                    "configured, or that the jar file for the connector is not available in the " +
                                    "local server."),

    CREATE_REGISTRY_FILE("OMRS-AUDIT-0114",
                      OMRSAuditLogRecordSeverity.INFO,
                      "Creating new cohort registry store {0}",
                      "The local server is creating a new cohort registry store. " +
                                 "The local server should continue to operate correctly.",
                      "Verify that the local server is connecting to the open metadata repository cohort for" +
                                 "the first time."),

    UNUSABLE_REGISTRY_FILE("OMRS-AUDIT-0115",
                      OMRSAuditLogRecordSeverity.EXCEPTION,
                      "Unable to write to cohort registry store {0}",
                      "The local server can not write to the cohort registry store. " +
                                   "This is a serious issue because the local server is not able to record its " +
                                   "interaction with other servers in the cohort.",
                      "Shutdown the local server and resolve the issue with the repository store."),

    NULL_MEMBER_REGISTRATION("OMRS-AUDIT-0116",
                      OMRSAuditLogRecordSeverity.ERROR,
                      "Unable to read or write to cohort registry store {0} because registration information is null",
                      "The local server can not manage a member registration in the cohort registry store because " +
                                     "the registration information is null. " +
                                     "This is a serious issue because the local server is not able to record its " +
                                     "interaction with other servers in the cohort.",
                      "Shutdown the local server and resolve the issue with the cohort registry."),

    MISSING_MEMBER_REGISTRATION("OMRS-AUDIT-0117",
                      OMRSAuditLogRecordSeverity.ERROR,
                      "Unable to process the remote registration for {0} from cohort registry store {1} " +
                                        "because registration information is not stored",
                      "The local server can not process a member registration event from the cohort registry store " +
                                        "because the registration information is not stored. " +
                                        "This may simply be a timing issue. " +
                                        "However, it may be the result of an earlier issue with the " +
                                        "local cohort registry store.",
                      "Verify that there are no issues with writing to the cohort registry store."),

    INCOMING_CONFLICTING_TYPEDEFS("OMRS-AUDIT-0201",
                      OMRSAuditLogRecordSeverity.ACTION,
                      "Server {1} in cohort {0} that hosts metadata collection {2} has detected that " +
                                          "TypeDef {3} ({4}) in the local " +
                                          "server conflicts with TypeDef {5} ({6}) in the remote server",
                      "The remote server may not be able to exchange metadata with this local server.",
                      "It is necessary to update the TypeDef to remove the conflict before the " +
                                          "remote server will exchange metadata with this server."),

    INCOMING_TYPEDEF_PATCH_MISMATCH("OMRS-AUDIT-0202",
                      OMRSAuditLogRecordSeverity.ACTION,
                      "Registration request for this server in cohort {0} was rejected by server {1} that " +
                                            "hosts metadata collection {2} because TypeDef {3} ({4}) in the local " +
                                            "server is at version {5} which is different from this TypeDef in the " +
                                            "remote server which is at version {6}",
                      "The remote server may not be able to exchange metadata with this local server.",
                      "It is necessary to update the TypeDef to remove the conflict to ensure that the remote server " +
                                            "can exchange metadata with this server."),

    OUTGOING_CONFLICTING_TYPEDEFS("OMRS-AUDIT-0203",
                      OMRSAuditLogRecordSeverity.ACTION,
                      "The local server has detected a conflict in cohort {0} in the registration request " +
                                          "from server {1} that hosts metadata collection {2}. TypeDef " +
                                          "{3} ({4}) in the local server conflicts with TypeDef {5} ({6}) in the remote server",
                      "The local server will not exchange metadata with this remote server.",
                      "It is necessary to update the TypeDef to remove the conflict before the local " +
                                          "server will exchange metadata with this server."),

    OUTGOING_TYPEDEF_PATCH_MISMATCH("OMRS-AUDIT-0204",
                      OMRSAuditLogRecordSeverity.ACTION,
                      "The local server in cohort {0} has rejected a TypeDef update from server {1} that hosts metadata " +
                                            "collection {2} because the version of TypeDef {3} ({4}) in the local server " +
                                            "is at version {5} is different from this TypeDef in the remote server " +
                                            "which is at version {6}",
                      "The local server will not exchange metadata with this remote server.",
                      "It is necessary to update the TypeDef to remove the conflict before the local server will " +
                                            "exchange metadata with the remote server."),

    OUTGOING_TYPE_MISMATCH("OMRS-AUDIT-0205",
                      OMRSAuditLogRecordSeverity.ACTION,
                      "The local server in cohort {0} has rejected a TypeDef update from server {1} that hosts " +
                                   "metadata collection {2} because the version of TypeDef {3} ({4}) in the local " +
                                   "server is at version {5} is different from this TypeDef in the remote server " +
                                   "which is at version {6}",
                      "The local server will not exchange metadata with this remote server.",
                      "It is necessary to update the TypeDef to remove the conflict before the local server will " +
                                   "exchange metadata with the remote server."),

    NEW_TYPE_ADDED("OMRS-AUDIT-0301",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The local server has added a new type called {0} with a unique identifier of {1} and a version number of {2} from {3}",
                      "The local server will be able to manage metadata instances of this type.",
                      "No action required.  This message is for information only."),

    NEW_TYPE_NOT_SUPPORTED("OMRS-AUDIT-0302",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The local server is unable to add a new type called {0} with a unique identifier of {1} and a version number of {2} because the server does not support this feature",
                      "The local server will be able to manage metadata instances of this type.",
                      "No action required.  This message is for information only."),

    TYPE_UPDATED("OMRS-AUDIT-0303",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The local server has updated an existing type called {0} with a unique identifier of {1} to version number of {2} from {3}",
                      "The local server will be able to manage metadata instances of this latest version of the type.",
                      "No action required.  This message is for information only."),

    TYPE_REMOVED("OMRS-AUDIT-0304",
                      OMRSAuditLogRecordSeverity.INFO,
                      "The local server has removed an existing type called {0} with a unique identifier of {1} to version number of {2}",
                      "The local server will be no longer be able to manage metadata instances of this type.",
                      "No action required.  This message is for information only."),

    TYPE_IDENTIFIER_MISMATCH("OMRS-AUDIT-0305",
                      OMRSAuditLogRecordSeverity.ERROR,
                      "The local server has detected a conflict with an existing type called {0} with a unique identifier of {1}. This does not match the type name {2} and unique identifier {3} passed to it on a request",
                      "The local server will be no longer be able to manage metadata instances of this type.",
                      "This is a serious error since it may cause metadata to be corrupted.  " +
                                     "First check the caller to ensure it is operating properly.  " +
                                     "Then investigate the source of the type and any other errors."),

    PROCESS_UNKNOWN_EVENT("OMRS-AUDIT-8001",
                      OMRSAuditLogRecordSeverity.EVENT,
                      "Received unknown event: {0}",
                      "The local server has received an unknown event from another member of the metadata repository " +
                                  "cohort and is unable to process it. " +
                                  "This is possible if a server in the cohort is at a higher level than this server and " +
                                  "is using a more advanced version of the protocol. " +
                                  "The local server should continue to operate correctly.",
                      "Verify that the event is a new event type introduced after this server was written."),

    NULL_OMRS_EVENT_RECEIVED("OMRS-AUDIT-9002",
                      OMRSAuditLogRecordSeverity.EXCEPTION,
                      "Unable to process a received event because its content is null",
                      "The system is unable to process an incoming event.",
                      "This may be caused by an internal logic error or the receipt of an incompatible OMRSEvent"),

    SEND_TYPEDEF_EVENT_ERROR("OMRS-AUDIT-9003",
                      OMRSAuditLogRecordSeverity.EXCEPTION,
                      "Unable to send a TypeDef event for cohort {0} due to an error in the OMRS Topic Connector",
                      "The local server is unable to properly manage TypeDef events for the metadata " +
                                      "repository cohort. The cause of the error is recorded in the accompanying exception.",
                      "Review the exception and resolve the issue it documents.  " +
                                      "Then disconnect and reconnect this server to the cohort."),

    SEND_INSTANCE_EVENT_ERROR("OMRS-AUDIT-9005",
                        OMRSAuditLogRecordSeverity.EXCEPTION,
                        "Unable to send or receive a metadata instance event for cohort {0} due to an error in the OMRS Topic Connector",
                        "The local server is unable to properly manage the replication of metadata instances for " +
                                "the metadata repository cohort. The cause of the error is recorded in the accompanying exception.",
                        "Review the exception and resolve the issue it documents. " +
                                      "Then disconnect and reconnect this server to the cohort."),

    NULL_REGISTRY_PROCESSOR("OMRS-AUDIT-9006",
                        OMRSAuditLogRecordSeverity.EXCEPTION,
                        "Unable to send or receive a registry event because the event processor is null",
                        "The local server is unable to properly manage registration events for the metadata " +
                                "repository cohort.",
                        "This is an internal logic error.  Raise a JIRA, including the audit log, to get this fixed."),

    NULL_TYPEDEF_PROCESSOR("OMRS-AUDIT-9007",
                       OMRSAuditLogRecordSeverity.EXCEPTION,
                       "Unable to send or receive a TypeDef event because the event processor is null",
                       "The local server is unable to properly manage the exchange of TypeDefs for the metadata " +
                               "repository cohort.",
                       "This is an internal logic error.  Raise a JIRA, including the audit log, to get this fixed."),

    NULL_INSTANCE_PROCESSOR("OMRS-AUDIT-9008",
                        OMRSAuditLogRecordSeverity.EXCEPTION,
                        "Unable to send or receive a metadata instance event because the event processor is null",
                        "The local server is unable to properly manage the replication of metadata instances for " +
                                "the metadata repository cohort.",
                        "This is an internal logic error.  Raise a JIRA, including the audit log, to get this fixed."),

    NULL_OMRS_CONFIG("OMRS-AUDIT-9009",
                        OMRSAuditLogRecordSeverity.EXCEPTION,
                            "Unable to initialize part of the Open Metadata Repository Service (OMRS) because the configuration is null",
                            "The local server is unable to properly manage the replication of metadata instances for " +
                                    "the metadata repository cohort.",
                            "This is an internal logic error.  Raise a JIRA, including the audit log, to get this fixed."),

    SENT_UNKNOWN_EVENT("OMRS-AUDIT-9010",
                        OMRSAuditLogRecordSeverity.EXCEPTION,
                        "Unable to send an event because the event is of an unknown type",
                        "The local server may not be communicating properly with other servers in " +
                                "the metadata repository cohort.",
                        "This is an internal logic error.  Raise a JIRA, including the audit log, to get this fixed."),
    UNEXPECTED_EXCEPTION_FROM_EVENT("OMRS-AUDIT-9011",
                       OMRSAuditLogRecordSeverity.EXCEPTION,
                       "An incoming event of type {0} from {1} ({2}) generated an exception with message {3}",
                       "The contents of the event were not accepted by the local repository.",
                       "Review the exception and resolve the issue it documents."),
    ENTERPRISE_TOPIC_DISCONNECT_ERROR("OMRS-AUDIT-9012",
                       OMRSAuditLogRecordSeverity.EXCEPTION,
                       "Disconnecting from the enterprise topic connector generated an exception with message {0}",
                       "The server may not have disconnected from the topic cleanly.",
                       "Review the exception and resolve the issue it documents.")

    ;

    private String                     logMessageId;
    private OMRSAuditLogRecordSeverity severity;
    private String                     logMessage;
    private String                     systemAction;
    private String                     userAction;

    private static final Logger log = LoggerFactory.getLogger(OMRSAuditCode.class);


    /**
     * The constructor for OMRSAuditCode expects to be passed one of the enumeration rows defined in
     * OMRSAuditCode above.   For example:
     *
     *     OMRSAuditCode   auditCode = OMRSAuditCode.SERVER_NOT_AVAILABLE;
     *
     * This will expand out to the 4 parameters shown below.
     *
     * @param messageId - unique Id for the message
     * @param severity - severity of the message
     * @param message - text for the message
     * @param systemAction - description of the action taken by the system when the condition happened
     * @param userAction - instructions for resolving the situation, if any
     */
    OMRSAuditCode(String                     messageId,
                  OMRSAuditLogRecordSeverity severity,
                  String                     message,
                  String                     systemAction,
                  String                     userAction)
    {
        this.logMessageId = messageId;
        this.severity = severity;
        this.logMessage = message;
        this.systemAction = systemAction;
        this.userAction = userAction;
    }


    /**
     * Returns the unique identifier for the error message.
     *
     * @return logMessageId
     */
    public String getLogMessageId()
    {
        return logMessageId;
    }


    /**
     * Return the severity of the audit log record.
     *
     * @return OMRSAuditLogRecordSeverity enum
     */
    public OMRSAuditLogRecordSeverity getSeverity()
    {
        return severity;
    }

    /**
     * Returns the log message with the placeholders filled out with the supplied parameters.
     *
     * @param params - strings that plug into the placeholders in the logMessage
     * @return logMessage (formatted with supplied parameters)
     */
    public String getFormattedLogMessage(String... params)
    {
        if (log.isDebugEnabled())
        {
            log.debug(String.format("<== OMRS Audit Code.getMessage(%s)", Arrays.toString(params)));
        }

        MessageFormat mf = new MessageFormat(logMessage);
        String result = mf.format(params);

        if (log.isDebugEnabled())
        {
            log.debug(String.format("==> OMRS Audit Code.getMessage(%s): %s", Arrays.toString(params), result));
        }

        return result;
    }



    /**
     * Returns a description of the action taken by the system when the condition that caused this exception was
     * detected.
     *
     * @return systemAction String
     */
    public String getSystemAction()
    {
        return systemAction;
    }


    /**
     * Returns instructions of how to resolve the issue reported in this exception.
     *
     * @return userAction String
     */
    public String getUserAction()
    {
        return userAction;
    }
}

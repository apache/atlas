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
package org.apache.atlas.omag.admin;


import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.omag.configuration.properties.AccessServiceConfig;
import org.apache.atlas.omag.configuration.properties.OMAGServerConfig;
import org.apache.atlas.omag.ffdc.exception.OMAGConfigurationErrorException;
import org.apache.atlas.omag.ffdc.exception.OMAGInvalidParameterException;
import org.apache.atlas.omag.ffdc.exception.OMAGNotAuthorizedException;
import org.apache.atlas.omrs.admin.properties.CohortConfig;
import org.apache.atlas.omrs.admin.properties.EnterpriseAccessConfig;
import org.apache.atlas.omrs.admin.properties.LocalRepositoryConfig;

import java.util.List;

/**
 * OMAGServerAdministration defines the administrative interface for an Open Metadata and Governance (OMAG) Server.
 * It is used to create both the Java client and the RESTful server-side implementation.  It provides all of the
 * configuration properties for the Open Metadata Access Services (OMASs) and delegates administration requests
 * to the Open Metadata Repository Services (OMRS).
 *
 * <p>
 *     There are four types of operations supported by OMAGServerAdministration:
 * </p>
 * <ul>
 *     <li>
 *         Basic configuration - these methods use the minimum of configuration information to run the
 *         server using default properties.
 *     </li>
 *     <li>
 *         Advanced Configuration - provides access to all configuration properties to provide
 *         fine-grained control of the server.
 *     </li>
 *     <li>
 *         Initialization and shutdown - these methods control the initialization and shutdown of the
 *         open metadata and governance services based on the supplied configuration.
 *     </li>
 *     <li>
 *         Operational status and control - these methods query the status of the open metadata and governance
 *         services as well as the audit log.
 *     </li>
 * </ul>
 */
public interface OMAGServerAdministration
{
    /*
     * =============================================================
     * Configure server - basic options using defaults
     */

    /**
     * Set up the root URL for this server that is used to construct full URL paths to calls for
     * this server's REST interfaces.  The default value is "localhost:8080".
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param serverURLRoot - String url.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or serverURLRoot parameter.
     */
    void setServerURLRoot(String    userId,
                          String    serverName,
                          String    serverURLRoot) throws OMAGNotAuthorizedException,
                                                          OMAGInvalidParameterException;


    /**
     * Set up the descriptive type of the server.  This value is added to distributed events to
     * make it easier to understand the source of events.  The default value is "Open Metadata and Governance Server".
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param serverType - short description for the type of server.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or serverType parameter.
     */
    void setServerType(String    userId,
                       String    serverName,
                       String    serverType) throws OMAGNotAuthorizedException,
                                                    OMAGInvalidParameterException;


    /**
     * Set up the name of the organization that is running this server.  This value is added to distributed events to
     * make it easier to understand the source of events.  The default value is null.
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param organizationName - String name of the organization.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or organizationName parameter.
     */
    void setOrganizationName(String    userId,
                             String    serverName,
                             String    organizationName) throws OMAGNotAuthorizedException,
                                                                OMAGInvalidParameterException;

    /**
     * Set an upper limit in the page size that can be requested on a REST call to the server.  The default
     * value is 1000.
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param maxPageSize - max number of elements that can be returned on a request.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or maxPageSize parameter.
     */
    void setMaxPageSize(String  userId,
                        String  serverName,
                        int     maxPageSize) throws OMAGNotAuthorizedException,
                                                    OMAGInvalidParameterException;


    /**
     * Set up whether the access services should be enabled or not.  This is controlled by the serviceMode.
     * The default is serviceMode=enabled for all access services that are installed into this server and
     * serviceMode=disabled for those services that are not installed.   The configuration properties
     * for each access service can be changed from their default using setAccessServicesConfig operation.
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param serviceMode - OMAGServiceMode enum.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or serviceMode parameter.
     */
    void setAccessServicesMode(String            userId,
                               String            serverName,
                               OMAGServiceMode   serviceMode) throws OMAGNotAuthorizedException,
                                                                     OMAGInvalidParameterException;


    /**
     * Set up the type of local repository.  There are three choices: No local Repository, Local Graph Repository
     * and Repository Proxy.  The default is No Local Repository.  If the local repository mode is set to
     * Repository Proxy then it is necessary to provide the connection to the local repository using the
     * setRepositoryProxyConnection operation.
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param localRepositoryMode - LocalRepositoryMode enum - NO_LOCAL_REPOSITORY, LOCAL_GRAPH_REPOSITORY
     * or REPOSITORY_PROXY.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or localRepositoryMode parameter.
     */
    void setLocalRepositoryMode(String               userId,
                                String               serverName,
                                LocalRepositoryMode  localRepositoryMode) throws OMAGNotAuthorizedException,
                                                                                 OMAGInvalidParameterException;


    /**
     * Provide the connection to the local repository - used when the local repository mode is set to repository proxy.
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param repositoryProxyConnection - connection to the OMRS repository connector.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or repositoryProxyConnection parameter
     * @throws OMAGConfigurationErrorException - the local repository mode has not been set
     */
    void setRepositoryProxyConnection(String     userId,
                                      String     serverName,
                                      Connection repositoryProxyConnection) throws OMAGNotAuthorizedException,
                                                                                   OMAGInvalidParameterException,
                                                                                   OMAGConfigurationErrorException;


    /**
     * Provide the connection to the local repository's event mapper if needed.  The default value is null which
     * means no event mapper.  An event mapper is needed if the local repository has additional APIs that can change
     * the metadata in the repository without going through the open metadata and governance services.
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param localRepositoryEventMapper - connection to the OMRS repository event mapper.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or localRepositoryEventMapper parameter.
     * @throws OMAGConfigurationErrorException - the local repository mode has not been set
     */
    void setLocalRepositoryEventMapper(String      userId,
                                       String      serverName,
                                       Connection  localRepositoryEventMapper) throws OMAGNotAuthorizedException,
                                                                                      OMAGInvalidParameterException,
                                                                                      OMAGConfigurationErrorException;


    /**
     * Set up the mode for an open metadata repository cohort.  This is a group of open metadata repositories that
     * are sharing metadata.  An OMAG server can connect to zero, one or more cohorts.  Each cohort needs
     * a unique name.  The members of the cohort use a shared topic to exchange registration information and
     * events related to changes in their supported metadata types and instances.  They are also able to
     * query each other's metadata directly through REST calls.
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param cohortName - name of the cohort.
     * @param serviceMode - OMAGServiceMode enum - ENABLED or DISABLED.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName, cohortName or serviceMode parameter.
     */
    void setCohortMode(String           userId,
                       String           serverName,
                       String           cohortName,
                       OMAGServiceMode  serviceMode) throws OMAGNotAuthorizedException,
                                                            OMAGInvalidParameterException;


    /*
     * =============================================================
     * Configure server - advanced options overriding defaults
     */


    /**
     * Set up the configuration for all of the open metadata access services (OMASs).  This overrides
     * the current values.
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param accessServicesConfig - list of configuration properties for each access service.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or accessServicesConfig parameter.
     */
    void setAccessServicesConfig(String                    userId,
                                 String                    serverName,
                                 List<AccessServiceConfig> accessServicesConfig) throws OMAGNotAuthorizedException,
                                                                                        OMAGInvalidParameterException;


    /**
     * Set up the configuration for the local repository.  This overrides the current values.
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param localRepositoryConfig - configuration properties for the local repository.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or localRepositoryConfig parameter.
     */
    void setLocalRepositoryConfig(String                   userId,
                                  String                   serverName,
                                  LocalRepositoryConfig    localRepositoryConfig) throws OMAGNotAuthorizedException,
                                                                                         OMAGInvalidParameterException;


    /**
     * Set up the configuration that controls the enterprise repository services.  These services are part
     * of the Open Metadata Repository Services (OMRS).  They provide federated queries and federated event
     * notifications that cover metadata from the local repository plus any repositories connected via
     * open metadata repository cohorts.
     *
     * @param userId - user that is issuing the request
     * @param serverName - local server name
     * @param enterpriseAccessConfig - enterprise repository services configuration properties.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or enterpriseAccessConfig parameter.
     */
    void setEnterpriseAccessConfig(String                  userId,
                                   String                  serverName,
                                   EnterpriseAccessConfig  enterpriseAccessConfig) throws OMAGNotAuthorizedException,
                                                                                          OMAGInvalidParameterException;


    /**
     * Set up the configuration properties for a cohort.  This may reconfigure an existing cohort or create a
     * cohort.  Use setCohortMode to delete a cohort.
     *
     * @param userId - user that is issuing the request
     * @param serverName - local server name
     * @param cohortName - name of the cohort
     * @param cohortConfig - configuration for the cohort
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName, cohortName or cohortConfig parameter.
     */
    void setCohortConfig(String        userId,
                         String        serverName,
                         String        cohortName,
                         CohortConfig  cohortConfig) throws OMAGNotAuthorizedException,
                                                            OMAGInvalidParameterException;


    /*
     * =============================================================
     * Query current configuration
     */


    /**
     * Return the complete set of configuration properties in use by the server.
     *
     * @param userId - user that is issuing the request
     * @param serverName - local server name
     * @return OMAGServerConfig properties
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName parameter.
     */
    OMAGServerConfig  getCurrentConfiguration(String     userId,
                                              String     serverName) throws OMAGNotAuthorizedException,
                                                                            OMAGInvalidParameterException;


    /*
     * =============================================================
     * Initialization and shutdown
     */

    /**
     * Initialize the open metadata and governance services using the stored configuration information.
     *
     * @param userId - user that is issuing the request
     * @param serverName - local server name
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - the server name is invalid
     * @throws OMAGConfigurationErrorException - there is a problem using the supplied configuration
     */
    void initialize (String        userId,
                     String        serverName) throws OMAGNotAuthorizedException,
                                                      OMAGInvalidParameterException,
                                                      OMAGConfigurationErrorException;


    /**
     * Initialize the open metadata and governance services using the supplied information.
     *
     * @param userId - user that is issuing the request
     * @param configuration - properties used to initialize the services
     * @param serverName - local server name
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - the serverName is invalid.
     * @throws OMAGConfigurationErrorException - there is a problem using the supplied configuration
     */
    void initialize (String             userId,
                     String             serverName,
                     OMAGServerConfig   configuration) throws OMAGNotAuthorizedException,
                                                              OMAGInvalidParameterException,
                                                              OMAGConfigurationErrorException;


    /**
     * Terminate any open metadata and governance services.
     *
     * @param userId - user that is issuing the request
     * @param serverName - local server name
     * @param permanent - Is the server being shutdown permanently - if yes, the local server will unregister from
     *                  its open metadata repository cohorts.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - the serverName is invalid.
     */
    void terminate (String     userId,
                    String     serverName,
                    boolean    permanent) throws OMAGNotAuthorizedException,
                                                  OMAGInvalidParameterException;


    /*
     * =============================================================
     * Operational status and control
     */

    /* placeholder */
}

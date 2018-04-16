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
package org.apache.atlas.omas.connectedasset.admin;


import org.apache.atlas.omag.configuration.properties.AccessServiceConfig;
import org.apache.atlas.omag.configuration.registration.AccessServiceAdmin;
import org.apache.atlas.omag.ffdc.exception.OMAGConfigurationErrorException;
import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;
import org.apache.atlas.omrs.topicconnectors.OMRSTopicConnector;

public class ConnectedAssetAdmin implements AccessServiceAdmin
{
    private OMRSRepositoryConnector repositoryConnector = null;
    private OMRSTopicConnector      omrsTopicConnector  = null;
    private AccessServiceConfig     accessServiceConfig = null;
    private OMRSAuditLog            auditLog            = null;
    private String                  serverUserName      = null;


    /**
     * Default constructor
     */
    public ConnectedAssetAdmin()
    {
    }

    /**
     * Initialize the access service.
     *
     * @param accessServiceConfigurationProperties - specific configuration properties for this access service.
     * @param enterpriseOMRSTopicConnector - connector for receiving OMRS Events from the cohorts
     * @param enterpriseOMRSRepositoryConnector - connector for querying the cohort repositories
     * @param auditLog - audit log component for logging messages.
     * @param serverUserName - user id to use on OMRS calls where there is no end user.
     * @throws OMAGConfigurationErrorException - invalid parameters in the configuration properties.
     */
    public void initialize(AccessServiceConfig     accessServiceConfigurationProperties,
                           OMRSTopicConnector      enterpriseOMRSTopicConnector,
                           OMRSRepositoryConnector enterpriseOMRSRepositoryConnector,
                           OMRSAuditLog            auditLog,
                           String                  serverUserName) throws OMAGConfigurationErrorException
    {
        this.accessServiceConfig = accessServiceConfigurationProperties;
        this.omrsTopicConnector = enterpriseOMRSTopicConnector;

        this.repositoryConnector = enterpriseOMRSRepositoryConnector;

        this.auditLog = auditLog;
        this.serverUserName = serverUserName;

    }


    /**
     * Shutdown the access service.
     */
    public void shutdown()
    {
        /*
         * Nothing to do until set up out topic.
         */
    }
}

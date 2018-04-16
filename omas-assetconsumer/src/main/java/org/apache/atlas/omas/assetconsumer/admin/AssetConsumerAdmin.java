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
package org.apache.atlas.omas.assetconsumer.admin;

import org.apache.atlas.omag.configuration.properties.AccessServiceConfig;
import org.apache.atlas.omag.configuration.registration.AccessServiceAdmin;
import org.apache.atlas.omag.ffdc.exception.OMAGConfigurationErrorException;
import org.apache.atlas.omas.assetconsumer.auditlog.AssetConsumerAuditCode;
import org.apache.atlas.omas.assetconsumer.listener.AssetConsumerOMRSTopicListener;
import org.apache.atlas.omas.assetconsumer.server.AssetConsumerRESTServices;
import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;
import org.apache.atlas.omrs.topicconnectors.OMRSTopicConnector;

public class AssetConsumerAdmin implements AccessServiceAdmin
{
    private OMRSRepositoryConnector repositoryConnector = null;
    private OMRSTopicConnector      omrsTopicConnector  = null;
    private AccessServiceConfig     accessServiceConfig = null;
    private OMRSAuditLog            auditLog            = null;
    private String                  serverUserName      = null;

    private AssetConsumerOMRSTopicListener  omrsTopicListener = null;

    /**
     * Default constructor
     */
    public AssetConsumerAdmin()
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
        final String            actionDescription = "initialize";
        AssetConsumerAuditCode  auditCode;

        auditCode = AssetConsumerAuditCode.SERVICE_INITIALIZING;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());

        this.repositoryConnector = enterpriseOMRSRepositoryConnector;
        AssetConsumerRESTServices.setRepositoryConnector(accessServiceConfigurationProperties.getAccessServiceName(),
                                                         repositoryConnector);

        this.accessServiceConfig = accessServiceConfigurationProperties;
        this.omrsTopicConnector = enterpriseOMRSTopicConnector;

        if (omrsTopicConnector != null)
        {
            auditCode = AssetConsumerAuditCode.SERVICE_REGISTERED_WITH_TOPIC;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            omrsTopicListener = new AssetConsumerOMRSTopicListener(accessServiceConfig.getAccessServiceOutTopic(),
                                                                   repositoryConnector.getRepositoryHelper(),
                                                                   repositoryConnector.getRepositoryValidator(),
                                                                   accessServiceConfig.getAccessServiceName());

            omrsTopicConnector.registerListener(omrsTopicListener);
        }

        this.auditLog = auditLog;
        this.serverUserName = serverUserName;

        auditCode = AssetConsumerAuditCode.SERVICE_INITIALIZED;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());
    }


    /**
     * Shutdown the access service.
     */
    public void shutdown()
    {
        final String            actionDescription = "shutdown";
        AssetConsumerAuditCode  auditCode;

        auditCode = AssetConsumerAuditCode.SERVICE_SHUTDOWN;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());
    }
}

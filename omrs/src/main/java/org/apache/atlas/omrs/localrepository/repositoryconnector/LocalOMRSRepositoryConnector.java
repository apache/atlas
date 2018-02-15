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
package org.apache.atlas.omrs.localrepository.repositoryconnector;

import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.omrs.eventmanagement.*;
import org.apache.atlas.omrs.eventmanagement.events.OMRSInstanceEventProcessor;
import org.apache.atlas.omrs.eventmanagement.events.OMRSTypeDefEventProcessor;
import org.apache.atlas.omrs.eventmanagement.repositoryeventmapper.OMRSRepositoryEventMapper;
import org.apache.atlas.omrs.localrepository.OMRSLocalRepository;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryContentManager;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSTypeDefValidator;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;


/**
 * LocalOMRSRepositoryConnector provides access the local metadata repository plus manages outbound
 * repository events.
 *
 * It passes each request to both the real OMRS connector for the local metadata repository and an
 * OMRSEventPublisher.  The OMRSEventPublisher will use its configuration to decide if it needs to
 * pass on the request to the rest of the metadata repository cohort.
 */
public class LocalOMRSRepositoryConnector extends OMRSRepositoryConnector implements OMRSLocalRepository
{
    private String                       localServerName                  = null;
    private String                       localServerType                  = null;
    private String                       localOrganizationName            = null;
    private OMRSRepositoryEventMapper    repositoryEventMapper            = null;
    private OMRSRepositoryContentManager localTypeDefManager              = null;
    private OMRSInstanceEventProcessor   incomingInstanceEventProcessor   = null;
    private OMRSRepositoryEventManager   outboundRepositoryEventManager   = null;
    private OMRSRepositoryEventProcessor outboundRepositoryEventProcessor = null;

    private String                       localMetadataCollectionId        = null;
    private LocalOMRSMetadataCollection  metadataCollection               = null;

    private OMRSRepositoryConnector      realLocalConnector               = null;
    private OMRSMetadataCollection       realMetadataCollection           = null;

    /**
     * Constructor used by the LocalOMRSConnectorProvider.  It provides the information necessary to run the
     * local repository.
     *
     * @param localServerName - name of the local server
     * @param localServerType - type of the local server
     * @param localOrganizationName - name of organization that owns the server
     * @param realLocalConnector - connector to the local repository
     * @param repositoryEventMapper - optional event mapper for local repository
     * @param outboundRepositoryEventManager - event manager to call for outbound events.
     * @param localTypeDefManager - localTypeDefManager for supporting OMRS in managing TypeDefs.
     * @param saveExchangeRule - rule to determine what events to save to the local repository.
     */
    protected LocalOMRSRepositoryConnector(String                          localServerName,
                                           String                          localServerType,
                                           String                          localOrganizationName,
                                           OMRSRepositoryConnector         realLocalConnector,
                                           OMRSRepositoryEventMapper       repositoryEventMapper,
                                           OMRSRepositoryEventManager      outboundRepositoryEventManager,
                                           OMRSRepositoryContentManager localTypeDefManager,
                                           OMRSRepositoryEventExchangeRule saveExchangeRule)
    {
        this.localServerName = localServerName;
        this.localServerType = localServerType;
        this.localOrganizationName = localOrganizationName;

        this.realLocalConnector = realLocalConnector;
        this.realMetadataCollection = realLocalConnector.getMetadataCollection();
        this.repositoryEventMapper = repositoryEventMapper;
        this.outboundRepositoryEventManager = outboundRepositoryEventManager;

        /*
         * Incoming events are processed directly with real local connector to avoid the outbound event
         * propagation managed by LocalOMRSMetadataCollection.
         */
        this.localTypeDefManager = localTypeDefManager;
        if (localTypeDefManager != null)
        {
            localTypeDefManager.setupEventProcessor(this,
                                                    realLocalConnector,
                                                    saveExchangeRule,
                                                    outboundRepositoryEventManager);

        }

        this.incomingInstanceEventProcessor = new LocalOMRSInstanceEventProcessor(localMetadataCollectionId,
                                                                                  realLocalConnector,
                                                                                  localTypeDefManager,
                                                                                  saveExchangeRule);

        /*
         * The repositoryEventMapper is a plug-in component that handles repository events for
         * repository that have additional APIs for managing metadata and need their own mechanism for
         * sending OMRS Repository Events.  If there is no repositoryEventMapper then the localOMRSMetadataCollection
         * will send the outbound repository events.
         */
        if (repositoryEventMapper != null)
        {
            repositoryEventMapper.setRepositoryEventProcessor(outboundRepositoryEventManager);
        }
        else
        {
            /*
             * When outboundRepositoryEventProcessor is not null then the local metadata collection creates events.
             * Otherwise it assumes the event mapper will produce events.
             */
            this.outboundRepositoryEventProcessor = outboundRepositoryEventManager;
        }
    }


    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem disconnecting the connector.
     */
    public void disconnect() throws ConnectorCheckedException
    {
        if (realLocalConnector  != null)
        {
            realLocalConnector.disconnect();
        }
    }


    /*
     * ==============================
     * OMRSMetadataCollectionManager
     */

    /**
     * Set up the unique Id for this metadata collection.
     *
     * @param metadataCollectionId - String unique Id
     */
    public void setMetadataCollectionId(String     metadataCollectionId)
    {
        this.localMetadataCollectionId = metadataCollectionId;

        /*
         * Initialize the metadata collection only once the connector is properly set up.
         */
        metadataCollection = new LocalOMRSMetadataCollection(localMetadataCollectionId,
                                                             localServerName,
                                                             localServerType,
                                                             localOrganizationName,
                                                             realMetadataCollection,
                                                             outboundRepositoryEventProcessor,
                                                             localTypeDefManager);

    }

    /**
     * Returns the metadata collection object that provides an OMRS abstraction of the metadata within
     * a metadata repository.
     *
     * @return OMRSMetadataCollection - metadata information retrieved from the metadata repository.
     */
    public OMRSMetadataCollection getMetadataCollection()
    {
        if (metadataCollection == null)
        {
            // TODO Throw Error
        }

        return metadataCollection;
    }

    /*
     * ====================================
     * OMRSLocalRepository
     */

    /**
     * Returns the unique identifier (guid) of the local repository's metadata collection.
     *
     * @return String guid
     */
    public String getMetadataCollectionId()
    {
        return localMetadataCollectionId;
    }


    /**
     * Returns the Connection to the local repository that can be used by remote servers to create
     * an OMRS repository connector to call this server in order to access the local repository.
     *
     * @return Connection object
     */
    public Connection getLocalRepositoryRemoteConnection()
    {
        return super.connection;
    }


    /**
     * Return the TypeDefValidator.  This is used to validate that a list of type definitions (TypeDefs) are
     * compatible with the local repository.
     *
     * @return OMRSTypeDefValidator object for the local repository.
     */
    public OMRSTypeDefValidator getTypeDefValidator()
    {
        return localTypeDefManager;
    }


    /**
     * Return the event manager that the local repository uses to distribute events from the local repository.
     *
     * @return outbound repository event manager
     */
    public OMRSRepositoryEventManager getOutboundRepositoryEventManager()
    {
        return outboundRepositoryEventManager;
    }


    /**
     * Return the TypeDef event processor that should be passed all incoming TypeDef events received
     * from the cohorts that this server is a member of.
     *
     * @return OMRSTypeDefEventProcessor for the local repository.
     */
    public OMRSTypeDefEventProcessor getIncomingTypeDefEventProcessor()
    {
        return localTypeDefManager;
    }


    /**
     * Return the instance event processor that should be passed all incoming instance events received
     * from the cohorts that this server is a member of.
     *
     * @return OMRSInstanceEventProcessor for the local repository.
     */
    public OMRSInstanceEventProcessor getIncomingInstanceEventProcessor()
    {
        return incomingInstanceEventProcessor;
    }
}

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
import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.omrs.eventmanagement.*;
import org.apache.atlas.omrs.eventmanagement.events.OMRSInstanceEventProcessor;
import org.apache.atlas.omrs.eventmanagement.events.OMRSTypeDefEventProcessor;
import org.apache.atlas.omrs.eventmanagement.repositoryeventmapper.OMRSRepositoryEventMapperConnector;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.ffdc.exception.RepositoryErrorException;
import org.apache.atlas.omrs.localrepository.OMRSLocalRepository;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.*;
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
    private static final String   repositoryEventMapperName = "LocalRepositoryEventMapper";
    private static final String   repositoryName            = "LocalRepository";

    /*
     * The repository content manager is the TypeDefManager for the Local OMRS Metadata Collection,
     * The TypeDefValidator for the CohortRegistry and the incoming TypeDef Event Processor for the Archive
     * Manager and EventListener
     */
    private OMRSTypeDefValidator               typeDefValidator;
    private OMRSTypeDefManager                 typeDefManager;
    private OMRSTypeDefEventProcessor          incomingTypeDefEventProcessor;

    private OMRSInstanceEventProcessor         incomingInstanceEventProcessor   = null;
    private OMRSRepositoryEventProcessor       outboundRepositoryEventProcessor = null;
    private OMRSRepositoryEventManager         outboundRepositoryEventManager   = null;
    private OMRSRepositoryEventExchangeRule    saveExchangeRule                 = null;

    private OMRSRepositoryConnector            realLocalConnector               = null;
    private OMRSRepositoryEventMapperConnector realEventMapper                  = null;


    /**
     * Constructor used by the LocalOMRSConnectorProvider.  It provides the information necessary to run the
     * local repository.
     *
     * @param realLocalConnector - connector to the local repository
     * @param realEventMapper - optional event mapper for local repository
     * @param outboundRepositoryEventManager - event manager to call for outbound events.
     * @param repositoryContentManager - repositoryContentManager for supporting OMRS in managing TypeDefs.
     * @param saveExchangeRule - rule to determine what events to save to the local repository.
     */
    protected LocalOMRSRepositoryConnector(OMRSRepositoryConnector            realLocalConnector,
                                           OMRSRepositoryEventMapperConnector realEventMapper,
                                           OMRSRepositoryEventManager         outboundRepositoryEventManager,
                                           OMRSRepositoryContentManager       repositoryContentManager,
                                           OMRSRepositoryEventExchangeRule    saveExchangeRule)
    {
        this.realLocalConnector = realLocalConnector;
        this.realEventMapper = realEventMapper;

        this.outboundRepositoryEventManager = outboundRepositoryEventManager;
        this.saveExchangeRule = saveExchangeRule;

        /*
         * The repository content manager is the TypeDefManager for the Local OMRS Metadata Collection,
         * The TypeDefValidator for the CohortRegistry and the incoming TypeDef Event Processor for the Archive
         * Manager and EventListener
         */
        this.typeDefValidator = repositoryContentManager;
        this.typeDefManager = repositoryContentManager;
        this.incomingTypeDefEventProcessor = repositoryContentManager;

        /*
         * Incoming events are processed directly with real local connector to avoid the outbound event
         * propagation managed by LocalOMRSMetadataCollection.
         */
        if (repositoryContentManager != null)
        {
            repositoryContentManager.setupEventProcessor(this,
                                                         realLocalConnector,
                                                         saveExchangeRule,
                                                         outboundRepositoryEventManager);
        }

        /*
         * The realEventMapper is a plug-in component that handles repository events for
         * repository that have additional APIs for managing metadata and need their own mechanism for
         * sending OMRS Repository Events.  If there is no realEventMapper then the localOMRSMetadataCollection
         * will send the outbound repository events.
         */
        if (realEventMapper != null)
        {
            realEventMapper.initialize(repositoryEventMapperName,
                                       realLocalConnector);
            realEventMapper.setRepositoryEventProcessor(outboundRepositoryEventManager);
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
     * Indicates that the connector is completely configured and can begin processing.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public void start() throws ConnectorCheckedException
    {
        super.start();

        if (realLocalConnector != null)
        {
            realLocalConnector.start();
        }

        if (realEventMapper != null)
        {
            realEventMapper.start();
        }
    }


    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public void disconnect() throws ConnectorCheckedException
    {
        super.disconnect();

        if (realLocalConnector  != null)
        {
            realLocalConnector.disconnect();
        }

        if (realEventMapper != null)
        {
            realEventMapper.disconnect();
        }
    }


    /*
     * ==============================
     * OMRSMetadataCollectionManager
     */

    /**
     * Set up a repository helper object for the repository connector to use.
     *
     * @param repositoryHelper - helper object for building TypeDefs and metadata instances.
     */
    public void setRepositoryHelper(OMRSRepositoryHelper   repositoryHelper)
    {
        super.setRepositoryHelper(repositoryHelper);

        if (realLocalConnector != null)
        {
            realLocalConnector.setRepositoryHelper(repositoryHelper);
        }

        if (realEventMapper != null)
        {
            realEventMapper.setRepositoryHelper(repositoryHelper);
        }
    }


    /**
     * Set up a repository validator for the repository connector to use.
     *
     * @param repositoryValidator - validator object to check the validity of TypeDefs and metadata instances.
     */
    public void setRepositoryValidator(OMRSRepositoryValidator  repositoryValidator)
    {
        super.setRepositoryValidator(repositoryValidator);

        if (realLocalConnector != null)
        {
            realLocalConnector.setRepositoryValidator(repositoryValidator);
        }

        if (realEventMapper != null)
        {
            realEventMapper.setRepositoryValidator(repositoryValidator);
        }
    }


    /**
     * Set up the maximum PageSize
     *
     * @param maxPageSize - maximum number of elements that can be retrieved on a request.
     */
    public void setMaxPageSize(int    maxPageSize)
    {
        super.setMaxPageSize(maxPageSize);

        if (realLocalConnector != null)
        {
            realLocalConnector.setMaxPageSize(maxPageSize);
        }
    }


    /**
     * Set up the name of the server where the metadata collection resides.
     *
     * @param serverName - String name
     */
    public void  setServerName(String      serverName)
    {
        super.setServerName(serverName);

        if (realLocalConnector != null)
        {
            realLocalConnector.setServerName(serverName);
        }

        if (realEventMapper != null)
        {
            realEventMapper.setServerName(serverName);
        }
    }


    /**
     * Set up the descriptive string describing the type of the server.  This might be the
     * name of the product, or similar identifier.
     *
     * @param serverType - String server type
     */
    public void setServerType(String serverType)
    {
        super.setServerType(serverType);

        if (realLocalConnector != null)
        {
            realLocalConnector.setServerType(serverType);
        }

        if (realEventMapper != null)
        {
            realEventMapper.setServerType(serverType);
        }
    }



    /**
     * Set up the name of the organization that runs/owns the server.
     *
     * @param organizationName - String organization name
     */
    public void setOrganizationName(String organizationName)
    {
        super.setOrganizationName(organizationName);

        if (realLocalConnector != null)
        {
            realLocalConnector.setOrganizationName(organizationName);
        }

        if (realEventMapper != null)
        {
            realEventMapper.setOrganizationName(organizationName);
        }
    }


    /**
     * Set up the unique Id for this metadata collection.
     *
     * @param metadataCollectionId - String unique Id
     */
    public void setMetadataCollectionId(String     metadataCollectionId)
    {
        final String methodName = "setMetadataCollectionId";

        super.setMetadataCollectionId(metadataCollectionId);

        if (realLocalConnector != null)
        {
            realLocalConnector.setMetadataCollectionId(metadataCollectionId);
        }

        if (realEventMapper != null)
        {
            realEventMapper.setMetadataCollectionId(metadataCollectionId);
        }

        this.incomingInstanceEventProcessor = new LocalOMRSInstanceEventProcessor(metadataCollectionId,
                                                                                  super.serverName,
                                                                                  realLocalConnector,
                                                                                  super.repositoryHelper,
                                                                                  super.repositoryValidator,
                                                                                  saveExchangeRule,
                                                                                  outboundRepositoryEventProcessor);

        try
        {
            /*
             * Initialize the metadata collection only once the connector is properly set up.
             */
            metadataCollection = new LocalOMRSMetadataCollection(this,
                                                                 super.serverName,
                                                                 super.repositoryHelper,
                                                                 super.repositoryValidator,
                                                                 metadataCollectionId,
                                                                 this.getLocalServerName(),
                                                                 this.getLocalServerType(),
                                                                 this.getOrganizationName(),
                                                                 realLocalConnector.getMetadataCollection(),
                                                                 outboundRepositoryEventProcessor,
                                                                 typeDefManager);
        }
        catch (Throwable   error)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_METADATA_COLLECTION;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(realLocalConnector.getRepositoryName());

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction(),
                                              error);
        }
    }

    /**
     * Returns the metadata collection object that provides an OMRS abstraction of the metadata within
     * a metadata repository.
     *
     * @return OMRSMetadataCollection - metadata information retrieved from the metadata repository.
     * @throws RepositoryErrorException - no metadata collection
     */
    public OMRSMetadataCollection getMetadataCollection() throws RepositoryErrorException
    {
        final String      methodName = "getMetadataCollection";

        if (metadataCollection == null)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_METADATA_COLLECTION;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
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
        return super.metadataCollectionId;
    }


    /**
     * Returns the Connection to the local repository that can be used by remote servers to create
     * an OMRS repository connector to call this server in order to access the local repository.
     *
     * @return Connection object
     */
    public Connection getLocalRepositoryRemoteConnection()
    {
        return new Connection(super.connection);
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
        return incomingTypeDefEventProcessor;
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


    /**
     * Return the local server name - used for outbound events.
     *
     * @return String name
     */
    public String getLocalServerName() { return super.serverName; }


    /**
     * Return the local server type - used for outbound events.
     *
     * @return String name
     */
    public String getLocalServerType() { return super.serverType; }


    /**
     * Return the name of the organization that owns this local repository.
     *
     * @return String name
     */
    public String getOrganizationName() { return super.organizationName; }
}

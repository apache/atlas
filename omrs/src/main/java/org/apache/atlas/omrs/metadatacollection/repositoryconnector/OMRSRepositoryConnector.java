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
package org.apache.atlas.omrs.metadatacollection.repositoryconnector;

import org.apache.atlas.ocf.ConnectorBase;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.ffdc.exception.RepositoryErrorException;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;


/**
 * The OMRSRepositoryConnector defines the interface for an OMRS Repository Connector.  It is an abstract
 * class since not all of the methods for OMRSMetadataCollectionManager are implemented.
 */
public abstract class OMRSRepositoryConnector extends ConnectorBase implements OMRSMetadataCollectionManager
{
    protected OMRSRepositoryHelper    repositoryHelper     = null;
    protected OMRSRepositoryValidator repositoryValidator  = null;
    protected String                  serverName           = null;
    protected String                  serverType           = null;
    protected String                  organizationName     = null;
    protected int                     maxPageSize          = 1000;

    protected String                  metadataCollectionId = null;
    protected OMRSMetadataCollection  metadataCollection   = null;


    /**
     * Default constructor - nothing to do
     */
    public OMRSRepositoryConnector()
    {
    }


    /**
     * Set up a repository helper object for the repository connector to use.
     *
     * @param repositoryHelper - helper object for building TypeDefs and metadata instances.
     */
    public void setRepositoryHelper(OMRSRepositoryHelper repositoryHelper)
    {
        this.repositoryHelper = repositoryHelper;
    }


    /**
     * Set up a repository validator for the repository connector to use.
     *
     * @param repositoryValidator - validator object to check the validity of TypeDefs and metadata instances.
     */
    public void setRepositoryValidator(OMRSRepositoryValidator repositoryValidator)
    {
        this.repositoryValidator = repositoryValidator;
    }


    /**
     * Set up the name of the server where the metadata collection resides.
     *
     * @param serverName - String name
     */
    public void  setServerName(String      serverName)
    {
        this.serverName = serverName;
    }


    /**
     * Set up the descriptive string describing the type of the server.  This might be the
     * name of the product, or similar identifier.
     *
     * @param serverType - String server type
     */
    public void setServerType(String serverType)
    {
        this.serverType = serverType;
    }


    /**
     * Set up the name of the organization that runs/owns the server.
     *
     * @param organizationName - String organization name
     */
    public void setOrganizationName(String organizationName)
    {
        this.organizationName = organizationName;
    }


    /**
     * Set up the unique Id for this metadata collection.
     *
     * @param metadataCollectionId - String unique Id
     */
    public void setMetadataCollectionId(String         metadataCollectionId)
    {
        this.metadataCollectionId = metadataCollectionId;
    }


    /**
     * Set up the maximum PageSize
     *
     * @param maxPageSize - maximum number of elements that can be retrieved on a request.
     */
    public void setMaxPageSize(int    maxPageSize)
    {
        this.maxPageSize = maxPageSize;
    }


    /**
     * Throw a RepositoryErrorException if the connector is not active.
     *
     * @throws RepositoryErrorException repository connector has not started or has been disconnected.
     */
    public void validateRepositoryIsActive(String  methodName) throws RepositoryErrorException
    {
        if (! super.isActive())
        {
            OMRSErrorCode errorCode = OMRSErrorCode.REPOSITORY_NOT_AVAILABLE;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(serverName, methodName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }
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
            final String      methodName = "getMetadataCollection";

            OMRSErrorCode errorCode = OMRSErrorCode.NULL_METADATA_COLLECTION;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(serverName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        return metadataCollection;
    }
}
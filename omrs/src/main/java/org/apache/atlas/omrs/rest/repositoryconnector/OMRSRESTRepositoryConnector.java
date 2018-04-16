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
package org.apache.atlas.omrs.rest.repositoryconnector;

import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.ffdc.exception.RepositoryErrorException;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;

/**
 * The OMRSRESTRepositoryConnector is a connector to a remote Apache Atlas repository (or any other metadata repository
 * that supports the OMRS REST APIs).  This is the connector used by the EnterpriseOMRSRepositoryConnector to make a direct call
 * to another open metadata repository.
 */
public class OMRSRESTRepositoryConnector extends OMRSRepositoryConnector
{
    private OMRSRESTMetadataCollection  metadataCollection   = null;

    /**
     * Default constructor used by the OCF Connector Provider.
     */
    public OMRSRESTRepositoryConnector()
    {
        /*
         * Nothing to do (yet !)
         */
    }


    /**
     * Set up the unique Id for this metadata collection.
     *
     * @param metadataCollectionId - String unique Id
     */
    public void setMetadataCollectionId(String     metadataCollectionId)
    {
        this.metadataCollectionId = metadataCollectionId;

        /*
         * Initialize the metadata collection.
         */
        metadataCollection = new OMRSRESTMetadataCollection(this,
                                                            super.repositoryName,
                                                            repositoryHelper,
                                                            repositoryValidator,
                                                            metadataCollectionId);
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
        if (metadataCollection == null)
        {
            final String      methodName = "getMetadataCollection";

            OMRSErrorCode errorCode = OMRSErrorCode.NULL_METADATA_COLLECTION;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(super.serverName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
        return metadataCollection;
    }


    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem disconnecting the connector.
     */
    public void disconnect() throws ConnectorCheckedException
    {
    }

}
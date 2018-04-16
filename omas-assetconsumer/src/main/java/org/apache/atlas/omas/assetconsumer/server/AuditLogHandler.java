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
package org.apache.atlas.omas.assetconsumer.server;

import org.apache.atlas.omas.assetconsumer.ffdc.AssetConsumerErrorCode;
import org.apache.atlas.omas.assetconsumer.ffdc.exceptions.InvalidParameterException;
import org.apache.atlas.omas.assetconsumer.ffdc.exceptions.PropertyServerException;
import org.apache.atlas.omas.assetconsumer.ffdc.exceptions.UserNotAuthorizedException;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;

/**
 * AuditLogHandler manages the logging of audit records for the asset.
 */
public class AuditLogHandler
{
    private String                  serviceName;
    private OMRSRepositoryConnector repositoryConnector;



    /**
     * Construct the audit log handler with a link to the property server's connector and this access service's
     * official name.
     *
     * @param serviceName - name of this service
     * @param repositoryConnector - connector to the property server.
     */
    public AuditLogHandler(String                  serviceName,
                           OMRSRepositoryConnector repositoryConnector)
    {
        this.serviceName = serviceName;
        this.repositoryConnector = repositoryConnector;
    }


    /**
     * Creates an Audit log record for the asset.  This log record is stored in the Asset's Audit Log.
     *
     * @param userId - String - userId of user making request.
     * @param assetGUID - String - unique id for the asset.
     * @param connectorInstanceId - String - (optional) id of connector in use (if any).
     * @param connectionName - String - (optional) name of the connection (extracted from the connector).
     * @param connectorType - String - (optional) type of connector in use (if any).
     * @param contextId - String - (optional) function name, or processId of the activity that the caller is performing.
     * @param message - log record content.
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem adding the asset properties to
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public void  addLogMessageToAsset(String      userId,
                                      String      assetGUID,
                                      String      connectorInstanceId,
                                      String      connectionName,
                                      String      connectorType,
                                      String      contextId,
                                      String      message) throws InvalidParameterException,
                                                                  PropertyServerException,
                                                                  UserNotAuthorizedException
    {
        // todo
    }


    /**
     * Check that there is a repository connector.
     *
     * @param methodName - name of the method being called
     * @return metadata collection that provides access to the properties in the property server
     * @throws PropertyServerException - exception thrown if the repository connector
     */
    private OMRSMetadataCollection validateRepositoryConnector(String   methodName) throws PropertyServerException
    {
        if (this.repositoryConnector == null)
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.OMRS_NOT_INITIALIZED;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(methodName);

            throw new PropertyServerException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());

        }

        if (! this.repositoryConnector.isActive())
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.OMRS_NOT_AVAILABLE;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(methodName);

            throw new PropertyServerException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        try
        {
            return repositoryConnector.getMetadataCollection();
        }
        catch (Throwable error)
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.NO_METADATA_COLLECTION;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName);

            throw new PropertyServerException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }
}

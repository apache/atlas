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
 * ErrorHandler provides common validation routines for the other handler classes
 */
public class ErrorHandler
{
    private OMRSRepositoryConnector   repositoryConnector;


    /**
     * Typical constructor providing access to the repository connector for this access service.
     *
     * @param repositoryConnector - connector object
     */
    public ErrorHandler(OMRSRepositoryConnector   repositoryConnector)
    {
        this.repositoryConnector = repositoryConnector;
    }


    /**
     * Throw an exception if the supplied userId is null
     *
     * @param userId - user name to validate
     * @param methodName - name of the method making the call.
     * @throws InvalidParameterException - the userId is null
     */
    public  void validateUserId(String userId,
                                String methodName) throws InvalidParameterException
    {
        if (userId == null)
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.NULL_USER_ID;
            String                 errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(methodName);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }
    }


    /**
     * Throw an exception if the supplied unique identifier is null
     *
     * @param guid - unique identifier to validate
     * @param parameterName - name of the parameter that passed the guid.
     * @param methodName - name of the method making the call.
     * @throws InvalidParameterException - the guid is null
     */
    public  void validateGUID(String guid,
                              String parameterName,
                              String methodName) throws InvalidParameterException
    {
        if (guid == null)
        {
            AssetConsumerErrorCode errorCode    = AssetConsumerErrorCode.NULL_GUID;
            String                 errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(parameterName, methodName);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }
    }


    /**
     * Throw an exception if the supplied enum is null
     *
     * @param enumValue - enum value to validate
     * @param parameterName - name of the parameter that passed the enum.
     * @param methodName - name of the method making the call.
     * @throws InvalidParameterException - the enum is null
     */
    public  void validateEnum(Object enumValue,
                              String parameterName,
                              String methodName) throws InvalidParameterException
    {
        if (enumValue == null)
        {
            AssetConsumerErrorCode errorCode    = AssetConsumerErrorCode.NULL_ENUM;
            String                 errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(parameterName, methodName);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }
    }


    /**
     * Throw an exception if the supplied name is null
     *
     * @param name - unique name to validate
     * @param parameterName - name of the parameter that passed the name.
     * @param methodName - name of the method making the call.
     * @throws InvalidParameterException - the guid is null
     */
    public  void validateName(String name,
                              String parameterName,
                              String methodName) throws InvalidParameterException
    {
        if (name == null)
        {
            AssetConsumerErrorCode errorCode    = AssetConsumerErrorCode.NULL_NAME;
            String                 errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(parameterName, methodName);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }
    }


    /**
     * Throw an exception if the supplied text field is null
     *
     * @param text - unique name to validate
     * @param parameterName - name of the parameter that passed the name.
     * @param methodName - name of the method making the call.
     * @throws InvalidParameterException - the guid is null
     */
    public  void validateText(String text,
                              String parameterName,
                              String methodName) throws InvalidParameterException
    {
        if (text == null)
        {
            AssetConsumerErrorCode errorCode    = AssetConsumerErrorCode.NULL_TEXT;
            String                 errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(parameterName, methodName);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }
    }


    /**
     * Check that there is a repository connector.
     *
     * @param methodName - name of the method being called
     * @return metadata collection that provides access to the properties in the property server
     * @throws PropertyServerException - exception thrown if the repository connector
     */
    public OMRSMetadataCollection validateRepositoryConnector(String   methodName) throws PropertyServerException
    {
        if (this.repositoryConnector == null)
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.OMRS_NOT_INITIALIZED;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName);

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
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName);

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
            String                 errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName);

            throw new PropertyServerException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Throw an exception if the supplied userId is not authorized to perform a request
     *
     * @param userId - user name to validate
     * @param methodName - name of the method making the call.
     * @param serverName - name of this server
     * @param serviceName - name of this access service
     * @throws UserNotAuthorizedException - the userId is unauthorised for the request
     */
    public  void handleUnauthorizedUser(String userId,
                                        String methodName,
                                        String serverName,
                                        String serviceName) throws UserNotAuthorizedException
    {
        AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.USER_NOT_AUTHORIZED;
        String                 errorMessage = errorCode.getErrorMessageId()
                + errorCode.getFormattedErrorMessage(userId,
                                                     methodName,
                                                     serviceName,
                                                     serverName);

        throw new UserNotAuthorizedException(errorCode.getHTTPErrorCode(),
                                             this.getClass().getName(),
                                             methodName,
                                             errorMessage,
                                             errorCode.getSystemAction(),
                                             errorCode.getUserAction());

    }


    /**
     * Throw an exception if the supplied userId is not authorized to perform a request
     *
     * @param error - caught exception
     * @param methodName - name of the method making the call.
     * @param serverName - name of this server
     * @param serviceName - name of this access service
     * @throws PropertyServerException - unexpected exception from property server
     */
    public  void handleRepositoryError(Throwable  error,
                                       String     methodName,
                                       String     serverName,
                                       String     serviceName) throws PropertyServerException
    {
        AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.PROPERTY_SERVER_ERROR;
        String                 errorMessage = errorCode.getErrorMessageId()
                                            + errorCode.getFormattedErrorMessage(error.getMessage(),
                                                                                 methodName,
                                                                                 serviceName,
                                                                                 serverName);

        throw new PropertyServerException(errorCode.getHTTPErrorCode(),
                                          this.getClass().getName(),
                                          methodName,
                                          errorMessage,
                                          errorCode.getSystemAction(),
                                          errorCode.getUserAction());

    }


    /**
     * Throw an exception if the supplied userId is not authorized to perform a request
     *
     * @param error - caught exception
     * @param assetGUID - unique identifier for the requested asset
     * @param methodName - name of the method making the call
     * @param serverName - name of this server
     * @param serviceName - name of this access service
     * @throws InvalidParameterException - unexpected exception from property server
     */
    public  void handleUnknownAsset(Throwable  error,
                                    String     assetGUID,
                                    String     methodName,
                                    String     serverName,
                                    String     serviceName) throws InvalidParameterException
    {
        AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.UNKNOWN_ASSET;
        String                 errorMessage = errorCode.getErrorMessageId()
                                            + errorCode.getFormattedErrorMessage(assetGUID,
                                                                                 methodName,
                                                                                 serviceName,
                                                                                 serverName,
                                                                                 error.getMessage());

        throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                            this.getClass().getName(),
                                            methodName,
                                            errorMessage,
                                            errorCode.getSystemAction(),
                                            errorCode.getUserAction());

    }
}

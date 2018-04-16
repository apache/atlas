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


import org.apache.atlas.ocf.properties.ElementOrigin;
import org.apache.atlas.ocf.properties.beans.*;
import org.apache.atlas.omas.assetconsumer.ffdc.AssetConsumerErrorCode;
import org.apache.atlas.omas.assetconsumer.ffdc.exceptions.*;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;
import org.apache.atlas.omrs.metadatacollection.properties.MatchCriteria;
import org.apache.atlas.omrs.metadatacollection.properties.instances.*;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.PrimitiveDefCategory;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * ConnectionHandler retrieves Connection objects from the property server.  It runs server-side in the AssetConsumer
 * OMAS and retrieves Connections through the OMRSRepositoryConnector.
 */
public class ConnectionHandler
{
    private static final String connectionTypeGUID                      = "114e9f8f-5ff3-4c32-bd37-a7eb42712253";
    private static final String connectionConnectorTypeRelationshipGUID = "e542cfc1-0b4b-42b9-9921-f0a5a88aaf96";
    private static final String connectionEndpointRelationshipGUID      = "887a7132-d6bc-4b92-a483-e80b60c86fb2";
    private static final String qualifiedNamePropertyName               = "qualifiedName";
    private static final String displayNamePropertyName                 = "displayName";
    private static final String additionalPropertiesName                = "additionalProperties";
    private static final String securePropertiesName                    = "securedProperties";
    private static final String descriptionPropertyName                 = "description";
    private static final String connectorProviderPropertyName           = "connectorProviderClassName";
    private static final String endpointPropertyName                    = "name";
    private static final String endpointAddressPropertyName             = "networkAddress";
    private static final String endpointProtocolPropertyName            = "protocol";
    private static final String endpointEncryptionPropertyName          = "encryptionMethod";

    private String                  serviceName;
    private OMRSRepositoryHelper    repositoryHelper = null;
    private String                  serverName = null;
    private ErrorHandler            errorHandler = null;

    /**
     * Construct the connection handler with a link to the property server's connector and this access service's
     * official name.
     *
     * @param serviceName - name of this service
     * @param repositoryConnector - connector to the property server.
     */
    public ConnectionHandler(String                  serviceName,
                             OMRSRepositoryConnector repositoryConnector)
    {
        this.serviceName = serviceName;
        if (repositoryConnector != null)
        {
            this.repositoryHelper = repositoryConnector.getRepositoryHelper();
            this.serverName = repositoryConnector.getServerName();
            errorHandler = new ErrorHandler(repositoryConnector);
        }
    }


    /**
     * Returns the connection object corresponding to the supplied connection name.
     *
     * @param userId - String - userId of user making request.
     * @param name - this may be the qualifiedName or displayName of the connection.
     *
     * @return Connection retrieved from property server
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws UnrecognizedConnectionNameException - there is no connection defined for this name.
     * @throws AmbiguousConnectionNameException - there is more than one connection defined for this name.
     * @throws PropertyServerException - there is a problem retrieving information from the property (metadata) server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    Connection getConnectionByName(String   userId,
                                   String   name) throws InvalidParameterException,
                                                         UnrecognizedConnectionNameException,
                                                         AmbiguousConnectionNameException,
                                                         PropertyServerException,
                                                         UserNotAuthorizedException
    {
        final  String   methodName = "getConnectionByName";
        final  String   nameParameter = "name";

        errorHandler.validateUserId(userId, methodName);
        errorHandler.validateName(name, nameParameter, methodName);

        OMRSMetadataCollection  metadataCollection = errorHandler.validateRepositoryConnector(methodName);

        try
        {
            InstanceProperties     properties    = null;

            properties = repositoryHelper.addStringPropertyToInstance(serviceName,
                                                                      properties,
                                                                      qualifiedNamePropertyName,
                                                                      name,
                                                                      methodName);

            properties = repositoryHelper.addStringPropertyToInstance(serviceName,
                                                                      properties,
                                                                      displayNamePropertyName,
                                                                      name,
                                                                      methodName);

            List<EntityDetail> connections = metadataCollection.findEntitiesByProperty(userId,
                                                                                       connectionTypeGUID,
                                                                                       properties,
                                                                                       MatchCriteria.ANY,
                                                                                       0,
                                                                                       null,
                                                                                       null,
                                                                                       null,
                                                                                       null,
                                                                                       null,
                                                                                       2);

            if (connections == null)
            {
                AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.CONNECTION_NOT_FOUND;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(name, serverName, null);

                throw new UnrecognizedConnectionNameException(errorCode.getHTTPErrorCode(),
                                                              this.getClass().getName(),
                                                              methodName,
                                                              errorMessage,
                                                              errorCode.getSystemAction(),
                                                              errorCode.getUserAction());
            }
            else if (connections.isEmpty())
            {
                AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.CONNECTION_NOT_FOUND;
                String  errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(name,
                                                                                                          serverName,
                                                                                                          null);

                throw new UnrecognizedConnectionNameException(errorCode.getHTTPErrorCode(),
                                                              this.getClass().getName(),
                                                              methodName,
                                                              errorMessage,
                                                              errorCode.getSystemAction(),
                                                              errorCode.getUserAction());
            }
            else if (connections.size() > 1)
            {
                AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.TOO_MANY_CONNECTIONS;
                String  errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(name,
                                                                                                          serverName,
                                                                                                          Integer.toString(connections.size()));

                throw new AmbiguousConnectionNameException(errorCode.getHTTPErrorCode(),
                                                           this.getClass().getName(),
                                                           methodName,
                                                           errorMessage,
                                                           errorCode.getSystemAction(),
                                                           errorCode.getUserAction());
            }
            else
            {
                return this.getConnectionFromRepository(userId, metadataCollection, connections.get(0));
            }
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);
        }
        catch (AmbiguousConnectionNameException  error)
        {
            throw error;
        }
        catch (UnrecognizedConnectionNameException  error)
        {
            throw error;
        }
        catch (Throwable  error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }

        return null;
    }


    /**
     * Returns the connection object corresponding to the supplied connection GUID.
     *
     * @param userId - String - userId of user making request.
     * @param guid - the unique id for the connection within the property server.
     *
     * @return Connection retrieved from the property server
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws UnrecognizedConnectionGUIDException - the supplied GUID is not recognized by the metadata repository.
     * @throws PropertyServerException - there is a problem retrieving information from the property (metadata) server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public Connection getConnectionByGUID(String     userId,
                                          String     guid) throws InvalidParameterException,
                                                                  UnrecognizedConnectionGUIDException,
                                                                  PropertyServerException,
                                                                  UserNotAuthorizedException
    {
        final  String   methodName = "getConnectionByGUID";
        final  String   guidParameter = "guid";

        errorHandler.validateUserId(userId, methodName);
        errorHandler.validateGUID(guid, guidParameter, methodName);

        OMRSMetadataCollection  metadataCollection = errorHandler.validateRepositoryConnector(methodName);
        EntityDetail            connectionEntity = null;

        try
        {
            connectionEntity = metadataCollection.getEntityDetail(userId, guid);
        }
        catch (org.apache.atlas.omrs.ffdc.exception.EntityNotKnownException error)
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.CONNECTION_NOT_FOUND;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(guid,
                                                                                                            serverName,
                                                                                                            error.getErrorMessage());

            throw new UnrecognizedConnectionGUIDException(errorCode.getHTTPErrorCode(),
                                                          this.getClass().getName(),
                                                          methodName,
                                                          errorMessage,
                                                          errorCode.getSystemAction(),
                                                          errorCode.getUserAction());
        }
        catch (org.apache.atlas.omrs.ffdc.exception.EntityProxyOnlyException error)
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.PROXY_CONNECTION_FOUND;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(guid,
                                                                                                            serverName,
                                                                                                            error.getErrorMessage());

            throw new UnrecognizedConnectionGUIDException(errorCode.getHTTPErrorCode(),
                                                          this.getClass().getName(),
                                                          methodName,
                                                          errorMessage,
                                                          errorCode.getSystemAction(),
                                                          errorCode.getUserAction());
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }

        if (connectionEntity != null)
        {
            return this.getConnectionFromRepository(userId, metadataCollection, connectionEntity);
        }

        return null;
    }




    /**
     * Return a Connection bean filled with values retrieved from the property server.
     *
     * @param userId - name of user
     * @param metadataCollection - accessor object for the property server
     * @param connectionEntity - root entity object
     * @return Connection bean
     * @throws PropertyServerException - a problem communicating with the property server
     */
    private Connection  getConnectionFromRepository(String                  userId,
                                                    OMRSMetadataCollection  metadataCollection,
                                                    EntityDetail            connectionEntity) throws PropertyServerException
    {
        final  String   methodName = "getConnectionFromRepository";

        ConnectorType connectorType = getConnectorTypeFromRepository(userId, metadataCollection, connectionEntity);
        Endpoint      endpoint      = getEndpointFromRepository(userId, metadataCollection, connectionEntity);

        Connection    connection    = new Connection();

        connection.setGUID(connectionEntity.getGUID());
        connection.setURL(connectionEntity.getInstanceURL());
        connection.setType(this.getElementType(connectionEntity));
        connection.setQualifiedName(repositoryHelper.getStringProperty(serviceName,
                                                                       qualifiedNamePropertyName,
                                                                       connectionEntity.getProperties(),
                                                                       methodName));
        connection.setDisplayName(repositoryHelper.getStringProperty(serviceName,
                                                                     displayNamePropertyName,
                                                                     connectionEntity.getProperties(),
                                                                     methodName));

        connection.setDescription(repositoryHelper.getStringProperty(serviceName,
                                                                     descriptionPropertyName,
                                                                     connectionEntity.getProperties(),
                                                                     methodName));

        connection.setConnectorType(connectorType);
        connection.setEndpoint(endpoint);


        connection.setAdditionalProperties(this.getAdditionalPropertiesFromEntity(additionalPropertiesName,
                                                                                  connectionEntity.getProperties(),
                                                                                  methodName));

        connection.setSecuredProperties(this.getAdditionalPropertiesFromEntity(securePropertiesName,
                                                                               connectionEntity.getProperties(),
                                                                               methodName));

        return connection;
    }


    /**
     * Return a ConnectorType bean filled with values retrieved from the property server for the connector type
     * associated with the supplied connection.
     *
     * @param userId - name of user
     * @param metadataCollection - accessor object for the property server
     * @param connectionEntity - root entity object
     * @return ConnectorType bean
     * @throws PropertyServerException - a problem communicating with the property server
     */
    private ConnectorType  getConnectorTypeFromRepository(String                  userId,
                                                          OMRSMetadataCollection  metadataCollection,
                                                          EntityDetail            connectionEntity) throws PropertyServerException
    {
        final   String  methodName = "getConnectorTypeFromRepository";

        ConnectorType connectorType = null;

        EntityDetail    endpointEntity = this.getEntityForRelationshipType(userId,
                                                                           connectionEntity,
                                                                           connectionConnectorTypeRelationshipGUID,
                                                                           metadataCollection,
                                                                           methodName);

        if (endpointEntity != null)
        {
            connectorType = new ConnectorType();

            connectorType.setGUID(connectionEntity.getGUID());
            connectorType.setURL(connectionEntity.getInstanceURL());
            connectorType.setType(this.getElementType(connectionEntity));
            connectorType.setQualifiedName(repositoryHelper.getStringProperty(serviceName,
                                                                              qualifiedNamePropertyName,
                                                                              connectionEntity.getProperties(),
                                                                              methodName));
            connectorType.setDisplayName(repositoryHelper.getStringProperty(serviceName,
                                                                            displayNamePropertyName,
                                                                            connectionEntity.getProperties(),
                                                                            methodName));
            connectorType.setDescription(repositoryHelper.getStringProperty(serviceName,
                                                                            descriptionPropertyName,
                                                                            connectionEntity.getProperties(),
                                                                            methodName));
            connectorType.setConnectorProviderClassName(repositoryHelper.getStringProperty(serviceName,
                                                                                           connectorProviderPropertyName,
                                                                                           connectionEntity.getProperties(),
                                                                                           methodName));

            connectorType.setAdditionalProperties(this.getAdditionalPropertiesFromEntity(additionalPropertiesName,
                                                                                         connectionEntity.getProperties(),
                                                                                         methodName));

        }




        return connectorType;
    }


    /**
     * Return an Endpoint bean filled with values retrieved from the property server for the endpoint
     * associated with the supplied connection.
     *
     * @param userId - name of user
     * @param metadataCollection - accessor object for the property server
     * @param connectionEntity - root entity object
     * @return Endpoint bean
     * @throws PropertyServerException - a problem communicating with the property server
     */
    private Endpoint  getEndpointFromRepository(String                  userId,
                                                OMRSMetadataCollection  metadataCollection,
                                                EntityDetail            connectionEntity) throws PropertyServerException
    {
        final   String  methodName = "getEndpointFromRepository";

        Endpoint endpoint = null;

        EntityDetail    endpointEntity = this.getEntityForRelationshipType(userId,
                                                                           connectionEntity,
                                                                           connectionEndpointRelationshipGUID,
                                                                           metadataCollection,
                                                                           methodName);

        if (endpointEntity != null)
        {
            endpoint = new Endpoint();

            endpoint.setGUID(connectionEntity.getGUID());
            endpoint.setURL(connectionEntity.getInstanceURL());
            endpoint.setType(this.getElementType(connectionEntity));
            endpoint.setQualifiedName(repositoryHelper.getStringProperty(serviceName,
                                                                         qualifiedNamePropertyName,
                                                                         connectionEntity.getProperties(),
                                                                         methodName));
            endpoint.setDisplayName(repositoryHelper.getStringProperty(serviceName,
                                                                       endpointPropertyName,
                                                                       connectionEntity.getProperties(),
                                                                       methodName));
            endpoint.setDescription(repositoryHelper.getStringProperty(serviceName,
                                                                       descriptionPropertyName,
                                                                       connectionEntity.getProperties(),
                                                                       methodName));
            endpoint.setAddress(repositoryHelper.getStringProperty(serviceName,
                                                                       endpointAddressPropertyName,
                                                                       connectionEntity.getProperties(),
                                                                       methodName));
            endpoint.setProtocol(repositoryHelper.getStringProperty(serviceName,
                                                                       endpointProtocolPropertyName,
                                                                       connectionEntity.getProperties(),
                                                                       methodName));
            endpoint.setEncryptionMethod(repositoryHelper.getStringProperty(serviceName,
                                                                       endpointEncryptionPropertyName,
                                                                       connectionEntity.getProperties(),
                                                                       methodName));
            endpoint.setAdditionalProperties(this.getAdditionalPropertiesFromEntity(additionalPropertiesName,
                                                                                    connectionEntity.getProperties(),
                                                                                    methodName));
        }

        return endpoint;
    }


    /**
     * Create an ElementType by extracting relevant information from the supplied instance.
     *
     * @param instance - instance to extract properties from
     * @return resulting elementType
     */
    private ElementType  getElementType(InstanceHeader   instance)
    {
        ElementType  elementType  = null;
        InstanceType instanceType = instance.getType();

        if (instanceType != null)
        {
            elementType = new ElementType();

            elementType.setElementTypeId(instanceType.getTypeDefGUID());
            elementType.setElementTypeName(instanceType.getTypeDefName());
            elementType.setElementTypeDescription(instanceType.getTypeDefDescription());
            elementType.setElementTypeVersion(instanceType.getTypeDefVersion());
            elementType.setElementAccessServiceURL(serverName);
            elementType.setElementHomeMetadataCollectionId(instance.getMetadataCollectionId());

            switch (instance.getInstanceProvenanceType())
            {
                case UNKNOWN:
                    elementType.setElementOrigin(null);
                    break;

                case CONTENT_PACK:
                    elementType.setElementOrigin(ElementOrigin.CONTENT_PACK);
                    break;

                case LOCAL_COHORT:
                    elementType.setElementOrigin(ElementOrigin.LOCAL_COHORT);
                    break;

                case EXPORT_ARCHIVE:
                    elementType.setElementOrigin(ElementOrigin.EXPORT_ARCHIVE);
                    break;

                case DEREGISTERED_REPOSITORY:
                    elementType.setElementOrigin(ElementOrigin.DEREGISTERED_REPOSITORY);
                    break;
            }
        }

        return elementType;
    }


    /**
     * Extract an additional properties object from the instance properties within a map property value.
     *
     * @param propertyName - name of the property that is a map
     * @param properties - instance properties containing the map property
     * @param methodName - calling method
     * @return an AdditionalProperties object or null
     */
    private AdditionalProperties getAdditionalPropertiesFromEntity(String              propertyName,
                                                                   InstanceProperties  properties,
                                                                   String              methodName)
    {
        /*
         * Extract the map property
         */
        InstanceProperties mapProperty = repositoryHelper.getMapProperty(serviceName,
                                                                         propertyName,
                                                                         properties,
                                                                         methodName);

        if (mapProperty != null)
        {
            /*
             * The contents should be primitives.  Need to step through all of the property names
             * and add each primitive value to a map.  This map is then used to set up the additional properties
             * object for return
             */
            Iterator<String>      additionalPropertyNames = mapProperty.getPropertyNames();

            if (additionalPropertyNames != null)
            {
                Map<String,Object> additionalPropertiesMap = new HashMap<>();

                while (additionalPropertyNames.hasNext())
                {
                    String                 additionalPropertyName  = additionalPropertyNames.next();
                    InstancePropertyValue  additionalPropertyValue = mapProperty.getPropertyValue(additionalPropertyName);

                    if (additionalPropertyValue != null)
                    {
                        /*
                         * If the property is not primitive it is ignored.
                         */
                        if (additionalPropertyValue.getInstancePropertyCategory() == InstancePropertyCategory.PRIMITIVE)
                        {
                            PrimitivePropertyValue primitivePropertyValue = (PrimitivePropertyValue) additionalPropertyValue;

                            additionalPropertiesMap.put(additionalPropertyName, primitivePropertyValue.getPrimitiveValue());
                        }
                    }
                }

                if (! additionalPropertiesMap.isEmpty())
                {
                    AdditionalProperties   additionalProperties = new AdditionalProperties();

                    additionalProperties.setAdditionalProperties(additionalPropertiesMap);
                    return additionalProperties;
                }
            }
        }

        return null;
    }

    /**
     * Return the entity a the other end of the requested relationship type.  The assumption is that this is a 0..1
     * relationship so one entity (or null) is returned.  If lots of relationships are found then the
     * PropertyServerException is thrown.
     *
     * @param userId - user making the request.
     * @param anchorEntity - starting entity.
     * @param relationshipTypeGUID - identifier for the relationship to follow
     * @param metadataCollection - metadata collection to retrieve instances from
     * @param methodName - name of calling method
     * @return retrieved entity or null
     * @throws PropertyServerException - problem access the property server
     */
    private  EntityDetail  getEntityForRelationshipType(String                 userId,
                                                        EntityDetail           anchorEntity,
                                                        String                 relationshipTypeGUID,
                                                        OMRSMetadataCollection metadataCollection,
                                                        String                 methodName) throws PropertyServerException
    {
        try
        {
            List<Relationship> relationships = metadataCollection.getRelationshipsForEntity(userId,
                                                                                            anchorEntity.getGUID(),
                                                                                            relationshipTypeGUID,
                                                                                            0,
                                                                                            null,
                                                                                            null,
                                                                                            null,
                                                                                            null,
                                                                                            100);

            if (relationships != null)
            {
                if (relationships.size() == 1)
                {
                    Relationship  relationship = relationships.get(0);

                    EntityProxy  requiredEnd = relationship.getEntityOneProxy();
                    if (anchorEntity.getGUID().equals(requiredEnd.getGUID()))
                    {
                        requiredEnd = relationship.getEntityTwoProxy();
                    }

                    return metadataCollection.getEntityDetail(userId, requiredEnd.getGUID());
                }
            }
        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }

        return null;
    }
}

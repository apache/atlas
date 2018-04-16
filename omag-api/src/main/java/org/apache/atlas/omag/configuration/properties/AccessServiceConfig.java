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
package org.apache.atlas.omag.configuration.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.ocf.properties.ElementOrigin;
import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.ocf.properties.beans.ConnectorType;
import org.apache.atlas.ocf.properties.beans.ElementType;
import org.apache.atlas.ocf.properties.beans.Endpoint;
import org.apache.atlas.omag.configuration.registration.AccessServiceDescription;
import org.apache.atlas.omag.configuration.registration.AccessServiceOperationalStatus;
import org.apache.atlas.omag.configuration.registration.AccessServiceRegistration;
import org.apache.atlas.omrs.topicconnectors.kafka.KafkaOMRSTopicProvider;

import java.io.Serializable;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * AccessServiceConfig provides the configuration for a single Open Metadata Access Service (OMAS)
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class AccessServiceConfig implements Serializable
{
    private static final long serialVersionUID = 1L;

    private static final String      defaultInTopicName = "InTopic";
    private static final String      defaultOutTopicName = "OutTopic";

    private int                            accessServiceId                = 0;
    private String                         accessServiceAdminClass        = null;
    private String                         accessServiceName              = null;
    private String                         accessServiceDescription       = null;
    private String                         accessServiceWiki              = null;
    private AccessServiceOperationalStatus accessServiceOperationalStatus = null;
    private Connection                     accessServiceInTopic           = null;
    private Connection                     accessServiceOutTopic          = null;
    private Map<String, String>            accessServiceOptions           = null;


    /**
     * Default constructor for use with Jackson libraries
     */
    public AccessServiceConfig()
    {
    }


    /**
     * Set up the default values for an access service using an access service description.
     *
     * @param accessServiceRegistration - AccessServiceDescription enum
     */
    public AccessServiceConfig(AccessServiceRegistration accessServiceRegistration)
    {
        this.accessServiceId = accessServiceRegistration.getAccessServiceCode();
        this.accessServiceName = accessServiceRegistration.getAccessServiceName();
        this.accessServiceAdminClass = accessServiceRegistration.getAccessServiceAdminClassName();
        this.accessServiceDescription = accessServiceRegistration.getAccessServiceDescription();
        this.accessServiceWiki = accessServiceRegistration.getAccessServiceWiki();
        this.accessServiceOperationalStatus = accessServiceRegistration.getAccessServiceOperationalStatus();
        this.accessServiceInTopic = this.getDefaultTopicConnection(defaultInTopicName,
                                                                   accessServiceRegistration.getAccessServiceInTopic());
        this.accessServiceOutTopic = this.getDefaultTopicConnection(defaultOutTopicName,
                                                                    accessServiceRegistration.getAccessServiceOutTopic());
    }


    /**
     * Return the code number (ordinal) for this access service.
     *
     * @return int ordinal
     */
    public int getAccessServiceId()
    {
        return accessServiceId;
    }


    /**
     * Set up the code number (ordinal) for this access service.
     *
     * @param accessServiceId int ordinal
     */
    public void setAccessServiceId(int accessServiceId)
    {
        this.accessServiceId = accessServiceId;
    }


    /**
     * Return the Java class name of the admin interface for this access service.
     *
     * @return String class name implementing the org.apache.atlas.omag.configuration.registration.AccessServiceAdmin
     * interface.
     */
    public String getAccessServiceAdminClass()
    {
        return accessServiceAdminClass;
    }


    /**
     * Set up the Java class name of the admin interface for this access service.
     *
     * @param accessServiceAdminClass - String class name implementing the
     *                                org.apache.atlas.omag.configuration.registration.AccessServiceAdmin interface.
     */
    public void setAccessServiceAdminClass(String accessServiceAdminClass)
    {
        this.accessServiceAdminClass = accessServiceAdminClass;
    }


    /**
     * Return the name of the access service.
     *
     * @return String name
     */
    public String getAccessServiceName()
    {
        return accessServiceName;
    }


    /**
     * Set up the name of the access service.
     *
     * @param accessServiceName - String name
     */
    public void setAccessServiceName(String accessServiceName)
    {
        this.accessServiceName = accessServiceName;
    }


    /**
     * Return the short description of the access service.  The default value is in English but this can be changed.
     *
     * @return String description
     */
    public String getAccessServiceDescription()
    {
        return accessServiceDescription;
    }


    /**
     * Set up the short description of the access service.
     *
     * @param accessServiceDescription - String description
     */
    public void setAccessServiceDescription(String accessServiceDescription)
    {
        this.accessServiceDescription = accessServiceDescription;
    }


    /**
     * Return the wiki page link for the access service.  The default value points to a page on the Atlas
     * confluence wiki.
     *
     * @return String url
     */
    public String getAccessServiceWiki()
    {
        return accessServiceWiki;
    }


    /**
     * Set up the wiki page link for the access service.
     *
     * @param accessServiceWiki - String url
     */
    public void setAccessServiceWiki(String accessServiceWiki)
    {
        this.accessServiceWiki = accessServiceWiki;
    }


    /**
     * Return the status of this access service.
     *
     * @return AccessServiceOperationalStatus enum
     */
    public AccessServiceOperationalStatus getAccessServiceOperationalStatus()
    {
        return accessServiceOperationalStatus;
    }


    /**
     * Set up the status of the access service.
     *
     * @param accessServiceOperationalStatus - AccessServiceOperationalStatus enum
     */
    public void setAccessServiceOperationalStatus(AccessServiceOperationalStatus accessServiceOperationalStatus)
    {
        this.accessServiceOperationalStatus = accessServiceOperationalStatus;
    }


    /**
     * Return the OCF Connection for the topic used to pass requests to this access service.
     * The default values are constructed from the access service name.
     *
     * @return Connection for InTopic
     */
    public Connection getAccessServiceInTopic()
    {
        return accessServiceInTopic;
    }


    /**
     * Set up the OCF Connection for the topic used to pass requests to this access service.
     *
     * @param accessServiceInTopic - Connection properties
     */
    public void setAccessServiceInTopic(Connection accessServiceInTopic)
    {
        this.accessServiceInTopic = accessServiceInTopic;
    }


    /**
     * Return the OCF Connection for the topic used by this access service to publish events.
     * The default values are constructed from the access service name.
     *
     * @return Connection for OutTopic
     */
    public Connection getAccessServiceOutTopic()
    {
        return accessServiceOutTopic;
    }


    /**
     * Set up the OCF Connection of the topic used by this access service to publish events.
     *
     * @param accessServiceOutTopic - Connection properties
     */
    public void setAccessServiceOutTopic(Connection accessServiceOutTopic)
    {
        this.accessServiceOutTopic = accessServiceOutTopic;
    }


    /**
     * Return the options for this access service. These are properties that are specific to the access service.
     *
     * @return Map from String to String
     */
    public Map<String, String> getAccessServiceOptions()
    {
        return accessServiceOptions;
    }


    /**
     * Set up the options for this access service.  These are properties that are specific to the access service.
     *
     * @param accessServiceOptions - Map from String to String
     */
    public void setAccessServiceOptions(Map<String, String> accessServiceOptions)
    {
        this.accessServiceOptions = accessServiceOptions;
    }


    /**
     * Return default values for the topic connection.
     *
     * @param connectionName - name to use in the connection object
     * @param topicName - name of the topic
     * @return Connection object
     */
    private Connection getDefaultTopicConnection(String    connectionName,
                                                 String    topicName)
    {
        String  description = connectionName + " for " + accessServiceName;

        final String endpointGUID = "f6e296ae-d001-44b2-80c9-b8240a246d61";
        final String connectorTypeGUID = "1db88a02-475f-43f9-b226-3b807f0caba5";
        final String connectionGUID = "bb32263c-a9ce-4262-98b0-b629a9d08614";

        final String endpointDescription = "OMRS default cohort registry endpoint.";

        Endpoint endpoint = new Endpoint();

        endpoint.setType(this.getEndpointType());
        endpoint.setGUID(endpointGUID);
        endpoint.setQualifiedName(topicName);
        endpoint.setDisplayName(topicName);
        endpoint.setDescription(description);
        endpoint.setAddress(topicName);

        final String connectorTypeJavaClassName = KafkaOMRSTopicProvider.class.getName();

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(this.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(topicName);
        connectorType.setDisplayName(topicName);
        connectorType.setDescription(description);
        connectorType.setConnectorProviderClassName(connectorTypeJavaClassName);

        Connection connection = new Connection();

        connection.setType(this.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(description);
        connection.setEndpoint(endpoint);
        connection.setConnectorType(connectorType);

        return connection;
    }


    /**
     * Return the standard type for an endpoint.
     *
     * @return ElementType object
     */
    public ElementType getEndpointType()
    {
        final String        elementTypeId                   = "dbc20663-d705-4ff0-8424-80c262c6b8e7";
        final String        elementTypeName                 = "Endpoint";
        final long          elementTypeVersion              = 1;
        final String        elementTypeDescription          = "Description of the network address and related information needed to call a software service.";
        final String        elementAccessServiceURL         = null;
        final ElementOrigin elementOrigin                   = ElementOrigin.LOCAL_COHORT;
        final String        elementHomeMetadataCollectionId = null;

        ElementType elementType = new ElementType();

        elementType.setElementTypeId(elementTypeId);
        elementType.setElementTypeName(elementTypeName);
        elementType.setElementTypeVersion(elementTypeVersion);
        elementType.setElementTypeDescription(elementTypeDescription);
        elementType.setElementAccessServiceURL(elementAccessServiceURL);
        elementType.setElementOrigin(elementOrigin);
        elementType.setElementHomeMetadataCollectionId(elementHomeMetadataCollectionId);

        return elementType;
    }


    /**
     * Return the standard type for a connector type.
     *
     * @return ElementType object
     */
    public ElementType getConnectorTypeType()
    {
        final String        elementTypeId                   = "954421eb-33a6-462d-a8ca-b5709a1bd0d4";
        final String        elementTypeName                 = "ConnectorType";
        final long          elementTypeVersion              = 1;
        final String        elementTypeDescription          = "A set of properties describing a type of connector.";
        final String        elementAccessServiceURL         = null;
        final ElementOrigin elementOrigin                   = ElementOrigin.LOCAL_COHORT;
        final String        elementHomeMetadataCollectionId = null;

        ElementType elementType = new ElementType();

        elementType.setElementTypeId(elementTypeId);
        elementType.setElementTypeName(elementTypeName);
        elementType.setElementTypeVersion(elementTypeVersion);
        elementType.setElementTypeDescription(elementTypeDescription);
        elementType.setElementAccessServiceURL(elementAccessServiceURL);
        elementType.setElementOrigin(elementOrigin);
        elementType.setElementHomeMetadataCollectionId(elementHomeMetadataCollectionId);

        return elementType;
    }


    /**
     * Return the standard type for a connection type.
     *
     * @return ElementType object
     */
    public ElementType getConnectionType()
    {
        final String        elementTypeId                   = "114e9f8f-5ff3-4c32-bd37-a7eb42712253";
        final String        elementTypeName                 = "Connection";
        final long          elementTypeVersion              = 1;
        final String        elementTypeDescription          = "A set of properties to identify and configure a connector instance.";
        final String        elementAccessServiceURL         = null;
        final ElementOrigin elementOrigin                   = ElementOrigin.LOCAL_COHORT;
        final String        elementHomeMetadataCollectionId = null;

        ElementType elementType = new ElementType();

        elementType.setElementTypeId(elementTypeId);
        elementType.setElementTypeName(elementTypeName);
        elementType.setElementTypeVersion(elementTypeVersion);
        elementType.setElementTypeDescription(elementTypeDescription);
        elementType.setElementAccessServiceURL(elementAccessServiceURL);
        elementType.setElementOrigin(elementOrigin);
        elementType.setElementHomeMetadataCollectionId(elementHomeMetadataCollectionId);

        return elementType;
    }
}

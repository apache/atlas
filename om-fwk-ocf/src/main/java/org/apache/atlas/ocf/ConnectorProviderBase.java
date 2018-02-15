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
package org.apache.atlas.ocf;

import org.apache.atlas.ocf.properties.Connection;

import org.apache.atlas.ocf.ffdc.ConnectionCheckedException;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.ffdc.OCFErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * ConnectorProviderBase is a base class for a new connector provider.  It manages all of the class loading
 * for subclass implementations of the connector provider along with the generation of new connector guids.
 *
 * ConnectorProviderBase creates a connector instance with the class name from the private variable called
 * connectorClassName.  This class name is initialized to null.  If the getConnector method is called when
 * the connectorClassName is null, it throws ConnectorCheckedException.
 * This is its default behaviour.
 *
 * To use the ConnectorProviderBase, create a new class that extends the ConnectorProviderBase class
 * and in the constructor call super.setConnectorClassName("your connector's class name");
 */
public abstract class ConnectorProviderBase extends ConnectorProvider
{
    private String                connectorClassName = null;

    private final int             hashCode = UUID.randomUUID().hashCode();

    private static final Logger   log = LoggerFactory.getLogger(ConnectorProviderBase.class);

    /**
     * Typical constructor
     */
    public ConnectorProviderBase ()
    {
        /*
         * Nothing to do
         */
    }


    /**
     * Each connector has a guid to make it easier to correlate log messages from the various components that
     * serve it.  It uses a type 4 (pseudo randomly generated) UUID.
     * The UUID is generated using a cryptographically strong pseudo random number generator.
     *
     * @return guid for a new connector instance
     */
    protected String  getNewConnectorGUID()
    {
        UUID     newUUID = UUID.randomUUID();

        return newUUID.toString();
    }


    /**
     * Return the class name for the connector that the connector provider generates.
     *
     * @return connectorClassName - will be null initially.
     */
    public  String   getConnectorClassName()
    {
        return connectorClassName;
    }


    /**
     * Update the class name for this connector provider.
     *
     * @param newConnectorClassName - this must be a valid Java class name for a class that implements the
     *                              org.apache.atlas.ocf.Connector interface.
     */
    public  void setConnectorClassName(String   newConnectorClassName)
    {
        if (log.isDebugEnabled())
        {
            log.debug("Connector class name set: " + newConnectorClassName);
        }

        connectorClassName = newConnectorClassName;
    }


    /**
     * Creates a new instance of a connector based on the information in the supplied connection.
     *
     * @param connection - connection that should have all of the properties needed by the Connector Provider
     *                   to create a connector instance.
     * @return Connector - instance of the connector.
     * @throws ConnectionCheckedException - if there are missing or invalid properties in the connection
     * @throws ConnectorCheckedException - if there are issues instantiating or initializing the connector
     */
    public Connector getConnector(Connection    connection) throws ConnectionCheckedException, ConnectorCheckedException
    {
        Connector                connector = null;
        String                   guid = null;

        if (log.isDebugEnabled())
        {
            log.debug("getConnector called");
        }

        if (connection == null)
        {
            /*
             * It is not possible to create a connector without a connection.
             */
            OCFErrorCode   errorCode = OCFErrorCode.NULL_CONNECTION;
            String         errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new ConnectionCheckedException(errorCode.getHTTPErrorCode(),
                                                 this.getClass().getName(),
                                                 "getConnector",
                                                 errorMessage,
                                                 errorCode.getSystemAction(),
                                                 errorCode.getUserAction());
        }


        /*
         * Validate that a subclass (or external class) has set up the class name of the connector.
         */
        if (connectorClassName == null)
        {
            /*
             * This instance of the connector provider is not initialised with the connector's class name so
             * throw an exception because a connector can not be generated.
             */
            OCFErrorCode   errorCode = OCFErrorCode.NULL_CONNECTOR_CLASS;
            String         errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new ConnectionCheckedException(errorCode.getHTTPErrorCode(),
                                                 this.getClass().getName(),
                                                 "getConnector",
                                                 errorMessage,
                                                 errorCode.getSystemAction(),
                                                 errorCode.getUserAction());
        }


        /*
         * Generate a new GUID for the connector.
         */
        guid = getNewConnectorGUID();


        /*
         * Create a new instance of the connector and initialize it with the guid and connection.
         */
        try
        {
            Class      connectorClass = Class.forName(connectorClassName);
            Object     potentialConnector = connectorClass.newInstance();

            connector = (Connector)potentialConnector;
            connector.initialize(guid, connection);
        }
        catch (ClassNotFoundException classException)
        {
            /*
             * Wrap exception in the ConnectionCheckedException with a suitable message
             */
            OCFErrorCode  errorCode = OCFErrorCode.UNKNOWN_CONNECTOR;
            String        connectionName = connection.getConnectionName();
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(connectorClassName, connectionName);

            throw new ConnectionCheckedException(errorCode.getHTTPErrorCode(),
                                                 this.getClass().getName(),
                                                 "getConnector",
                                                 errorMessage,
                                                 errorCode.getSystemAction(),
                                                 errorCode.getUserAction(),
                                                 classException);
        }
        catch (LinkageError   linkageError)
        {
            /*
             * Wrap linkage error in an exception
             */
            OCFErrorCode  errorCode = OCFErrorCode.INCOMPLETE_CONNECTOR;
            String        connectionName = connection.getConnectionName();
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(connectorClassName, connectionName);

            throw new ConnectionCheckedException(errorCode.getHTTPErrorCode(),
                                                 this.getClass().getName(),
                                                 "getConnector",
                                                 errorMessage,
                                                 errorCode.getSystemAction(),
                                                 errorCode.getUserAction(),
                                                 linkageError);
        }
        catch (ClassCastException  castException)
        {
            /*
             * Wrap class cast exception in a connection exception with error message to say that
             */
            OCFErrorCode  errorCode = OCFErrorCode.NOT_CONNECTOR;
            String        connectionName = connection.getConnectionName();
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(connectorClassName, connectionName);

            throw new ConnectionCheckedException(errorCode.getHTTPErrorCode(),
                                                 this.getClass().getName(),
                                                 "getConnector",
                                                 errorMessage,
                                                 errorCode.getSystemAction(),
                                                 errorCode.getUserAction(),
                                                 castException);
        }
        catch (Throwable unexpectedSomething)
        {
            /*
             * Wrap throwable in a connection exception with error message to say that there was a problem with
             * the connector implementation.
             */
            OCFErrorCode     errorCode = OCFErrorCode.INVALID_CONNECTOR;
            String           connectionName = connection.getConnectionName();
            String           errorMessage = errorCode.getErrorMessageId()
                                          + errorCode.getFormattedErrorMessage(connectorClassName, connectionName);

            throw new ConnectionCheckedException(errorCode.getHTTPErrorCode(),
                                                 this.getClass().getName(),
                                                 "getConnector",
                                                 errorMessage,
                                                 errorCode.getSystemAction(),
                                                 errorCode.getUserAction(),
                                                 unexpectedSomething);
        }


        /*
         * Return the initialized connector ready for use.
         */

        if (log.isDebugEnabled())
        {
            log.debug("getConnector returns: " + connector.getConnectorInstanceId() + ", " + connection.getConnectionName());
        }

        return connector;
    }


    /**
     * Provide a common implementation of hashCode for all OCF Connector Provider objects.  The UUID is unique and
     * is randomly assigned and so its hashCode is as good as anything to describe the hash code of the properties
     * object.
     *
     * @return random UUID as hashcode
     */
    public int hashCode()
    {
        return hashCode;
    }


    /**
     * Provide a common implementation of equals for all OCF Connector Provider objects.  The UUID is unique and
     * is randomly assigned and so its hashCode is as good as anything to evaluate the equality of the connector
     * provider object.
     *
     * @param object - object to test
     * @return boolean flag
     */
    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }

        ConnectorProviderBase that = (ConnectorProviderBase) object;

        if (hashCode != that.hashCode)
        {
            return false;
        }

        return connectorClassName != null ? connectorClassName.equals(that.connectorClassName) : that.connectorClassName == null;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "ConnectorProviderBase{" +
                "connectorClassName='" + connectorClassName + '\'' +
                ", hashCode=" + hashCode +
                '}';
    }
}
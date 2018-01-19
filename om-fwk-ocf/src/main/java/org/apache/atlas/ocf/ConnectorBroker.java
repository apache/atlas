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

import org.apache.atlas.ocf.ffdc.*;
import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.ocf.properties.ConnectorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * The ConnectorBroker is a generic factory for Open Connector Framework (OCF) Connectors.
 * The OCF provides a default implementation because all of the implementation that is specific to a
 * particular type of connector is delegated to the connector provider specified in the connection.
 */
public class ConnectorBroker
{
    private static final Logger   log = LoggerFactory.getLogger(ConnectorBroker.class);
    private final        int      hashCode = UUID.randomUUID().hashCode();


    /**
     * Typical constructor
     */
    public ConnectorBroker()
    {
        /* Nothing to do */
    }


    /**
     * Creates a new instance of a connector using the name of the connector provider in the supplied connection.
     *
     * @param connection - properties for the connector and connector provider.
     * @return new connector instance.
     * @throws ConnectionCheckedException - an error with the connection.
     * @throws ConnectorCheckedException - an error initializing the connector.
     */
    public Connector getConnector(Connection connection) throws ConnectionCheckedException, ConnectorCheckedException
    {
        ConnectorProvider    connectorProvider = null;
        Connector            connectorInstance = null;

        if (log.isDebugEnabled())
        {
            log.debug("==> ConnectorBroker.getConnector()");
        }

        if (connection == null)
        {
            /*
             * It is not possible to create a connector without a connection.
             */
            OCFErrorCode  errorCode = OCFErrorCode.NULL_CONNECTION;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new ConnectionCheckedException(errorCode.getHTTPErrorCode(),
                                                 this.getClass().getName(),
                                                 "getConnector",
                                                 errorMessage,
                                                 errorCode.getSystemAction(),
                                                 errorCode.getUserAction());
        }


        /*
         * Within the connection is a structure called the connector type.  This defines the factory for a new
         * connector instance.  This factory is called the Connector Provider.
         */
        ConnectorType       requestedConnectorType = connection.getConnectorType();

        if (requestedConnectorType == null)
        {
            /*
             * It is not possible to create a connector without a connector type since it
             * holds the name of the connector provider's Java class.  Build an exception.
             */
            OCFErrorCode  errorCode = OCFErrorCode.NULL_CONNECTOR_TYPE;
            String        connectionName = connection.getConnectionName();
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(connectionName);

            throw new ConnectionCheckedException(errorCode.getHTTPErrorCode(),
                                                 this.getClass().getName(),
                                                 "getConnector",
                                                 errorMessage,
                                                 errorCode.getSystemAction(),
                                                 errorCode.getUserAction());
        }


        /*
         * The connection has a valid connector type so the next step is to extract the class name for the
         * connector provider.  This is the specialized factory for the connector.
         */
        String    connectorProviderClassName = requestedConnectorType.getConnectorProviderClassName();

        if (connectorProviderClassName == null)
        {
            /*
             * The connector provider class name is blank so it is not possible to create the
             * connector provider.  Throw an exception.
             */
            OCFErrorCode  errorCode = OCFErrorCode.NULL_CONNECTOR_PROVIDER;
            String        connectionName = connection.getConnectionName();
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(connectionName);

            throw new ConnectionCheckedException(errorCode.getHTTPErrorCode(),
                                                 this.getClass().getName(),
                                                 "getConnector",
                                                 errorMessage,
                                                 errorCode.getSystemAction(),
                                                 errorCode.getUserAction());
        }


        /*
         * Extract the class for the connector provider and then create a connector provider object.
         * These actions may reveal that the class is not known to local JVM (ClassNotFound) or
         * the class is there but its dependencies are not (LinkageError) or it is there and loads
         * but it is not a connector provider (ClassCastException).  Each of these error conditions
         * results in a connection error exception that hopefully guides the consumer to correct
         * the config and/or setup error.
         */
        try
        {
            Class      connectorProviderClass = Class.forName(connectorProviderClassName);
            Object     potentialConnectorProvider = connectorProviderClass.newInstance();

            connectorProvider = (ConnectorProvider)potentialConnectorProvider;
        }
        catch (ClassNotFoundException classException)
        {
            /*
             * Wrap exception in the ConnectionCheckedException with a suitable message
             */
            OCFErrorCode  errorCode = OCFErrorCode.UNKNOWN_CONNECTOR_PROVIDER;
            String        connectionName = connection.getConnectionName();
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(connectorProviderClassName, connectionName);

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
            OCFErrorCode  errorCode = OCFErrorCode.INCOMPLETE_CONNECTOR_PROVIDER;
            String        connectionName = connection.getConnectionName();
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(connectorProviderClassName, connectionName);

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
            OCFErrorCode  errorCode = OCFErrorCode.NOT_CONNECTOR_PROVIDER;
            String        connectionName = connection.getConnectionName();
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(connectorProviderClassName, connectionName);

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
             * the connector provider.
             */
            OCFErrorCode  errorCode = OCFErrorCode.INVALID_CONNECTOR_PROVIDER;
            String        connectionName = connection.getConnectionName();
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(connectorProviderClassName, connectionName);

            throw new ConnectionCheckedException(errorCode.getHTTPErrorCode(),
                                                 this.getClass().getName(),
                                                 "getConnector",
                                                 errorMessage,
                                                 errorCode.getSystemAction(),
                                                 errorCode.getUserAction(),
                                                 unexpectedSomething);
        }


        /*
         * At this point we hopefully have a valid connector provider so all that is left to do is call
         * it to get the connector instance.  This is done in a different try ... catch block from the
         * instantiation of the connector provider so we can separate errors in the Connection from
         * errors generated in the Connector Provider, since both classes are
         * potentially code from a source outside of Apache Atlas.
         */
        try
        {
            connectorInstance = connectorProvider.getConnector(connection);
        }
        catch (ConnectionCheckedException connectionError)
        {
            /*
             * The connector provider has already provided first failure data capture in a ConnectionCheckedException.
             * This exception is rethrown to the caller.
             */
            throw connectionError;
        }
        catch (ConnectorCheckedException connectorError)
        {
            /*
             * The connector provider has already provided first failure data capture in a ConnectorCheckedException.
             * This exception is rethrown to the caller.
             */
            throw connectorError;
        }
        catch (Throwable  unexpectedSomething)
        {
            /*
             * The connector provider threw an unexpected runtime exception or error.  This is wrapped in a
             * ConnectorError and thrown to caller.
             */
            OCFErrorCode errorCode = OCFErrorCode.CAUGHT_EXCEPTION;
            String               errorMessage = errorCode.getErrorMessageId()
                                              + errorCode.getFormattedErrorMessage();

            throw new ConnectorCheckedException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              "getConnector",
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction(),
                                              unexpectedSomething);
        }


        /*
         * If the connector provider has returned a null connector with no exception then raise a generic exception
         * since something has gone wrong.
         */
        if (connectorInstance == null)
        {
            /*
             * Build and throw exception.
             */
            OCFErrorCode errorCode = OCFErrorCode.NULL_CONNECTOR;
            String       errorMessage = errorCode.getErrorMessageId()
                                      + errorCode.getFormattedErrorMessage(connection.getConnectionName());

            throw new OCFRuntimeException(errorCode.getHTTPErrorCode(),
                                          this.getClass().getName(),
                                          "getConnector",
                                          errorMessage,
                                          errorCode.getSystemAction(),
                                          errorCode.getUserAction());
        }

        /*
         * If we reach this point the connector provider has returned a connector that can be passed to the caller.
         */

        if (log.isDebugEnabled())
        {
            log.debug("New connector returned: " + connectorInstance.getConnectorInstanceId());
            log.debug("Using Connection: " + connectorInstance.getConnection().getQualifiedName() + "(" + connectorInstance.getConnection().getDisplayName() + ")");
            log.debug("<== ConnectorBroker.getConnector()");
        }

        return connectorInstance;
    }


    /**
     * Provide an implementation of hashCode for all OCF Connector Broker objects.  The UUID is unique and
     * is randomly assigned and so its hashCode is as good as anything to describe the hash code of the connector
     * broker object.
     */
    public int hashCode()
    {
        return hashCode;
    }


    /**
     * Provide a common implementation of equals for all OCF Connector Broker objects.  The UUID is unique and
     * is randomly assigned and so its hashCode is as good as anything to evaluate the equality of the connector
     * broker object.
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

        ConnectorBroker that = (ConnectorBroker) object;

        return hashCode == that.hashCode;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "ConnectorBroker{" +
                "hashCode=" + hashCode +
                '}';
    }
}
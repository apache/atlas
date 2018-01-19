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

import org.apache.atlas.ocf.ffdc.ConnectionCheckedException;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.properties.Connection;

/**
 * The ConnectorProvider is a formal plug-in interface for the Open Connector Framework (OCF).  It provides a factory
 * class for a specific type of connector.  Therefore is it typical to find the ConnectorProvider and Connector
 * implementation written as a pair.
 *
 * The ConnectorProvider uses the properties stored in a Connection object to initialize itself and its Connector instances.
 * the Connection object has the endpoint properties for the server that the connector must communicate with
 * as well as optional additional properties that may be needed for a particular type of connector.
 *
 * It is suggested that the ConnectorProvider validates the contents of the connection and throws
 * ConnectionErrorExceptions if the connection has missing or invalid properties.  If there are errors detected in the
 * instantiations or initialization of the connector, then these should be thrown as ConnectorErrorExceptions.
 */
public abstract class ConnectorProvider
{
    /**
     * Creates a new instance of a connector based on the information in the supplied connection.
     *
     * @param connection - connection that should have all of the properties needed by the Connector Provider
     *                   to create a connector instance.
     * @return Connector - instance of the connector.
     * @throws ConnectionCheckedException - if there are missing or invalid properties in the connection
     * @throws ConnectorCheckedException - if there are issues instantiating or initializing the connector
     */
    public abstract Connector getConnector(Connection connection) throws ConnectionCheckedException, ConnectorCheckedException;
}
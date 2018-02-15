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
package org.apache.atlas.omrs.enterprise.connectormanager;


/**
 * OMRSConnectorManager provides the methods for connector consumers to register with the connector manager.
 */
public interface OMRSConnectorManager
{
    /**
     * Register the supplied connector consumer with the connector manager.  During the registration
     * request, the connector manager will pass the connector to the local repository and
     * the connectors to all currently registered remote repositories.  Once successfully registered
     * the connector manager will call the connector consumer each time the repositories in the
     * metadata repository cluster changes.
     *
     * @param connectorConsumer OMRSConnectorConsumer interested in details of the connectors to
     *                           all repositories registered in the metadata repository cluster.
     * @return String identifier for the connectorConsumer - used for the unregister call.
     */
    String registerConnectorConsumer(OMRSConnectorConsumer    connectorConsumer);


    /**
     * Unregister a connector consumer from the connector manager so it is no longer informed of
     * changes to the metadata repository cluster.
     *
     * @param connectorConsumerId String identifier of the connector consumer returned on the
     *                             registerConnectorConsumer.
     */
    void unregisterConnectorConsumer(String   connectorConsumerId);
}

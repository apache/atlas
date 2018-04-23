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
package org.apache.atlas.omrs.localrepository;

import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.omrs.eventmanagement.events.OMRSInstanceEventProcessor;
import org.apache.atlas.omrs.eventmanagement.OMRSRepositoryEventManager;
import org.apache.atlas.omrs.eventmanagement.events.OMRSTypeDefEventProcessor;


/**
 * OMRSLocalRepository is an interface used by the OMRS components to retrieve information about the local
 * repository, to register listeners and to get access to the connector for the local repository.
 */
public interface OMRSLocalRepository
{
    /**
     * Returns the unique identifier (guid) of the local repository's metadata collection.
     *
     * @return String guid
     */
    String getMetadataCollectionId();


    /**
     * Returns the Connection to the local repository that can be used by remote servers to create
     * an OMRS repository connector to call this server in order to access the local repository.
     *
     * @return Connection object
     */
    Connection getLocalRepositoryRemoteConnection();


    /**
     * Return the event manager that the local repository uses to
     *
     * @return outbound repository event manager
     */
    OMRSRepositoryEventManager getOutboundRepositoryEventManager();


    /**
     * Return the TypeDef event processor that should be passed all incoming TypeDef events received
     * from the cohorts that this server is a member of.
     *
     * @return OMRSTypeDefEventProcessor for the local repository.
     */
    OMRSTypeDefEventProcessor getIncomingTypeDefEventProcessor();


    /**
     * Return the instance event processor that should be passed all incoming instance events received
     * from the cohorts that this server is a member of.
     *
     * @return OMRSInstanceEventProcessor for the local repository.
     */
    OMRSInstanceEventProcessor getIncomingInstanceEventProcessor();


    /**
     * Return the local server name - used for outbound events.
     *
     * @return String name
     */
    String getLocalServerName();


    /**
     * Return the local server type - used for outbound events.
     *
     * @return String name
     */
    String getLocalServerType();


    /**
     * Return the name of the organization that owns this local repository.
     *
     * @return String name
     */
    String getOrganizationName();
}

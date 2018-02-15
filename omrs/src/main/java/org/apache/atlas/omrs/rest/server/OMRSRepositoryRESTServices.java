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
package org.apache.atlas.omrs.rest.server;

import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;

/**
 * OMRSRepositoryRESTServices provides the server-side support for the OMRS Repository REST Services API.
 * It is a minimal wrapper around the OMRSRepositoryConnector for the local server's metadata collection.
 * If localRepositoryConnector is null when a REST calls is received, the request is rejected.
 */
public class OMRSRepositoryRESTServices
{
    //TODO remember to support getInstanceURL from TypeDefManager
    public static OMRSRepositoryConnector localRepositoryConnector = null;


    /**
     * Set up the local repository connector that will service the REST Calls.
     *
     * @param localRepositoryConnector - link to the local repository responsible for servicing the REST calls.
     *                                 If localRepositoryConnector is null when a REST calls is received, the request
     *                                 is rejected.
     */
    public static void setLocalRepository(OMRSRepositoryConnector    localRepositoryConnector)
    {
        OMRSRepositoryRESTServices.localRepositoryConnector = localRepositoryConnector;
    }
}

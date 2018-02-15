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
package org.apache.atlas.omrs.rest.repositoryconnector;

import org.apache.atlas.omrs.ffdc.exception.NotImplementedRuntimeException;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollectionBase;

/**
 * The OMRSRESTMetadataCollection represents a remote metadata repository that supports the OMRS REST API.
 * Requests to this metadata collection are translated one-for-one to requests to the remote repository since
 * the OMRS REST API has a one-to-one correspondence with the metadata collection.
 */
/*
 * This class is using OMRSMetadataCollectionBase while it is under construction.  It will change to
 * inheriting from OMRSMetadataCollection once it is implemented
 */
public class OMRSRESTMetadataCollection extends OMRSMetadataCollectionBase
{
    private OMRSRESTRepositoryConnector parentConnector = null;

    /**
     * Default constructor.
     *
     * @param parentConnector - connector that this metadata collection supports.  The connector has the information
     *                        to call the metadata repository.
     * @param metadataCollectionId - unique identifier for the metadata collection
     */
    public OMRSRESTMetadataCollection(OMRSRESTRepositoryConnector parentConnector,
                                      String                      metadataCollectionId)
    {
        super(metadataCollectionId);

        /*
         * Save parentConnector since this has the connection information.
         */
        this.parentConnector = parentConnector;

        /*
         * This is a temporary implementation to allow the structural implementation of the connectors to
         * be committed before the metadata collection implementation is complete.
         */
        throw new NotImplementedRuntimeException("OMRSRESTMetadataCollection", "constructor", "ATLAS-1773");
    }
}
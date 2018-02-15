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
package org.apache.atlas.omrs.adapters.igc.v2.repositoryconnector;

import org.apache.atlas.omrs.ffdc.exception.NotImplementedRuntimeException;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollectionBase;

/**
 * The IGCV2OMRSMetadataCollection represents a remote IBM Information Governance Catalog (IGC)
 * metadata repository.  IGC supports its own native REST APIs.
 * Requests to this metadata collection are translated to the IGC REST API calls and the results are
 * transformed to OMRS objects before returning to the caller.
 */
public class IGCV2OMRSMetadataCollection extends OMRSMetadataCollectionBase
{
    private IGCV2OMRSRepositoryConnector parentConnector = null;

    /**
     * Default constructor.
     *
     * @param parentConnector - connector that this metadata collection supports.  The connector has the information
     *                        to call the metadata repository.
     * @param metadataCollectionId  - unique identifier for the repository.
     */
    public IGCV2OMRSMetadataCollection(IGCV2OMRSRepositoryConnector parentConnector,
                                       String                       metadataCollectionId)
    {
        /*
         * The metadata collection Id is the unique Id for the metadata collection.  It is managed by the super class.
         */
        super(metadataCollectionId);
        this.metadataCollectionId = metadataCollectionId;

        /*
         * Save parentConnector since this has the connection information.
         */
        this.parentConnector = parentConnector;

        /*
         * This is a temporary implementation to allow the structural implementation of the connectors to
         * be committed before the metadata collection implementation is complete.
         */
        throw new NotImplementedRuntimeException("IGCV2OMRSMetadataCollection", "constructor", "ATLAS-1774");
    }
}
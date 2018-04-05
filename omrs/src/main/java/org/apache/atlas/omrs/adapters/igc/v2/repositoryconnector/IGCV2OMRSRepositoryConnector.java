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

import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;


/**
 * The IGCV2OMRSRepositoryConnector is a connector to a remote IBM Information Governance Catalog (IGC) repository
 * for releases after 11.7.
 */
public class IGCV2OMRSRepositoryConnector extends OMRSRepositoryConnector
{
    /**
     * Default constructor used by the OCF Connector Provider.
     */
    public IGCV2OMRSRepositoryConnector()
    {
        /*
         * Nothing to do (yet !)
         */
    }


    /**
     * Set up the unique Id for this metadata collection.
     *
     * @param metadataCollectionId - String unique Id
     */
    public void setMetadataCollectionId(String     metadataCollectionId)
    {
        this.metadataCollectionId = metadataCollectionId;

        /*
         * Initialize the metadata collection only once the connector is properly set up.
         */
        super.metadataCollection = new IGCV2OMRSMetadataCollection(this, metadataCollectionId);
    }
}
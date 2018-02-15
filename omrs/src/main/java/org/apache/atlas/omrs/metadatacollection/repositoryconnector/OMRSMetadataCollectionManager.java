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
package org.apache.atlas.omrs.metadatacollection.repositoryconnector;

import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;

/**
 * OMRSRepositoryConnectors are used by OMRS to retrieve metadata from metadata repositories.  Each implementation
 * of the OMRSRepositoryConnector is for a different type of repository.  This interface defines the extension that
 * an OMRSRepositoryConnector must implement over the base connector definition.  It describes the concept of a
 * metadata collection.  This is a collection of metadata that includes the type definitions (TypeDefs) and
 * metadata instances (Entities and Relationships) stored in the repository.
 */
public interface OMRSMetadataCollectionManager
{
    /**
     * Set up the unique Id for this metadata collection.
     *
     * @param metadataCollectionId - String unique Id
     */
    void setMetadataCollectionId(String         metadataCollectionId);


    /**
     * Returns the metadata collection object that provides an OMRS abstraction of the metadata within
     * a metadata repository.
     *
     * @return OMRSMetadataCollection - metadata TypeDefs and instances retrieved from the metadata repository.
     */
    OMRSMetadataCollection getMetadataCollection();
}
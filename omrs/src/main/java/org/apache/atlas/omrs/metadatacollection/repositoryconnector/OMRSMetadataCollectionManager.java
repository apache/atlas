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

import org.apache.atlas.omrs.ffdc.exception.RepositoryErrorException;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
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
     * Set up a repository helper object for the repository connector to use.
     *
     * @param repositoryHelper - helper object for building TypeDefs and metadata instances.
     */
    void setRepositoryHelper(OMRSRepositoryHelper   repositoryHelper);


    /**
     * Set up a repository validator for the repository connector to use.
     *
     * @param repositoryValidator - validator object to check the validity of TypeDefs and metadata instances.
     */
    void setRepositoryValidator(OMRSRepositoryValidator  repositoryValidator);


    /**
     * Return the name of the repository where the metadata collection resides.
     *
     * @return String name
     */
    String  getRepositoryName();


    /**
     * Set up the name of the repository where the metadata collection resides.
     *
     * @param repositoryName - String name
     */
    void  setRepositoryName(String      repositoryName);


    /**
     * Return the name of the server where the metadata collection resides.
     *
     * @return String name
     */
    String getServerName();


    /**
     * Set up the name of the server where the metadata collection resides.
     *
     * @param serverName - String name
     */
    void  setServerName(String      serverName);


    /**
     * Return the descriptive string describing the type of the server.  This might be the
     * name of the product, or similar identifier.
     *
     * @return String name
     */
    String getServerType();


    /**
     * Set up the descriptive string describing the type of the server.  This might be the
     * name of the product, or similar identifier.
     *
     * @param serverType - String server type
     */
    void setServerType(String serverType);


    /**
     * Return the name of the organization that runs/owns the server used to access the repository.
     *
     * @return String name
     */
    String getOrganizationName();


    /**
     * Set up the name of the organization that runs/owns the server used to access the repository.
     *
     * @param organizationName - String organization name
     */
    void setOrganizationName(String organizationName);


    /**
     * Return the maximum PageSize
     *
     * @return maximum number of elements that can be retrieved on a request.
     */
    int getMaxPageSize();


    /**
     * Set up the maximum PageSize
     *
     * @param maxPageSize - maximum number of elements that can be retrieved on a request.
     */
    void setMaxPageSize(int    maxPageSize);


    /**
     * Return the unique Id for this metadata collection.
     *
     * @return String unique Id
     */
    String getMetadataCollectionId();


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
     * @throws RepositoryErrorException - no metadata collection
     */
    OMRSMetadataCollection getMetadataCollection() throws RepositoryErrorException;
}
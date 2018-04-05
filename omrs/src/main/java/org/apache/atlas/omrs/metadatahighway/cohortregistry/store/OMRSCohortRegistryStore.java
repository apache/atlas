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
package org.apache.atlas.omrs.metadatahighway.cohortregistry.store;

import org.apache.atlas.omrs.metadatahighway.cohortregistry.store.properties.MemberRegistration;

import java.util.List;


/**
 * OMRSCohortRegistryStore is a connector to a repository that can store registration information for a cohort registry.
 * Each repository store serves a single repository.  It supports:
 * <ul>
 *     <li>
 *         One LocalRegistration that describes information about the local repository and its registration with
 *         the metadata repository cohort.  Note: the local registration is null if there is no local repository.
 *     </li>
 *     <li>
 *         None to many RemoteRegistrations.  There is a RemoteRegistration for each of the other repositories
 *         registered in the metadata repository cohort.
 *     </li>
 * </ul>
 */
public interface OMRSCohortRegistryStore
{
    /**
     * Save the local registration to the cohort registry store.  This provides details of the local repository's
     * registration with the metadata repository cohort.
     * Any previous local registration information is overwritten.
     *
     * @param localRegistration - details of the local repository's registration with the metadata cohort.
     */
    void saveLocalRegistration(MemberRegistration localRegistration);


    /**
     * Retrieve details of the local registration from the cohort registry store.  A null may be returned if the
     * local registration information has not been saved (typically because this is a new server instance).
     *
     * @return MemberRegistration object containing details for the local repository's registration with the
     * metadata cohort
     */
    MemberRegistration retrieveLocalRegistration();


    /**
     * Remove details of the local registration from the cohort registry store.  This is used when the local
     * repository unregisters from the open metadata repository cohort.
     */
    void removeLocalRegistration();


    /**
     * Save details of a remote registration.  This contains details of one of the other repositories in the
     * metadata repository cohort.  If a remote registration already exists, it is over-written.
     *
     * @param remoteRegistration - details of a remote repository in the metadata repository cohort.
     */
    void saveRemoteRegistration(MemberRegistration  remoteRegistration);


    /**
     * Return a list of all of the remote metadata repositories registered in the metadata repository cohort.
     *
     * @return List of member registrations for remote servers/repositories
     */
    List<MemberRegistration> retrieveRemoteRegistrations();


    /**
     * Return the registration information for a specific metadata repository, identified by its repository's
     * metadata collection Id.   If the metadata collection Id is not recognized then null is returned.
     *
     * @param metadataCollectionId - unique identifier for the repository's metadata collection.
     * @return MemberRegistration object containing details of the remote metadata repository.
     */
    MemberRegistration retrieveRemoteRegistration(String    metadataCollectionId);


    /**
     * Remove details of the requested remote repository's registration from the store.
     *
     * @param metadataCollectionId - unique identifier for the repository's metadata collection
     */
    void removeRemoteRegistration(String    metadataCollectionId);


    /**
     * Remove the local and remote registrations from the cohort registry store since the local server has
     * unregistered from the cohort.
     */
    void clearAllRegistrations();


    /**
     * Flush all changes and close the registry store.
     */
    void close();
}

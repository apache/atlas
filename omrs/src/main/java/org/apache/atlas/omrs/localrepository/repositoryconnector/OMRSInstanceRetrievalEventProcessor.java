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
package org.apache.atlas.omrs.localrepository.repositoryconnector;

import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;

import java.util.List;


/**
 * OMRSInstanceRetrievalEventProcessor defines the interface used by the Enterprise OMRS Repository Connector
 * to pass instance metadata retrieved from remote open metadata repository connectors.
 */
public interface OMRSInstanceRetrievalEventProcessor
{
    /**
     * Pass an entity that has been retrieved from a remote open metadata repository so it can be validated and
     * (if the rules permit) cached in the local repository.
     *
     * @param sourceName - name of the source of this event.
     * @param metadataCollectionId - unique identifier for the metadata from the remote repository
     * @param entity - the retrieved entity.
     * @return Validated and processed entity.
     */
    EntityDetail processRetrievedEntity(String        sourceName,
                                        String        metadataCollectionId,
                                        EntityDetail  entity);


    /**
     * Pass a list of entities that have been retrieved from a remote open metadata repository so they can be
     * validated and (if the rules permit) cached in the local repository.
     *
     * @param sourceName - name of the source of this event.
     * @param metadataCollectionId - unique identifier for the metadata from the remote repository
     * @param entities - the retrieved relationships
     * @return the validated and processed relationships
     */
    List<EntityDetail> processRetrievedEntities(String                    sourceName,
                                                String                    metadataCollectionId,
                                                List<EntityDetail>        entities);


    /**
     * Pass a relationship that has been retrieved from a remote open metadata repository so it can be validated and
     * (if the rules permit) cached in the local repository.
     *
     * @param sourceName - name of the source of this event.
     * @param metadataCollectionId - unique identifier for the metadata from the remote repository
     * @param relationship - the retrieved relationship
     * @return the validated and processed relationship
     */
    Relationship processRetrievedRelationship(String         sourceName,
                                              String         metadataCollectionId,
                                              Relationship   relationship);


    /**
     * Pass a list of relationships that have been retrieved from a remote open metadata repository so they can be
     * validated and (if the rules permit) cached in the local repository.
     *
     * @param sourceName - name of the source of this event.
     * @param metadataCollectionId - unique identifier for the metadata from the remote repository
     * @param relationships - the list of retrieved relationships
     * @return the validated and processed relationships
     */
    List<Relationship> processRetrievedRelationships(String               sourceName,
                                                     String               metadataCollectionId,
                                                     List<Relationship>   relationships);
}

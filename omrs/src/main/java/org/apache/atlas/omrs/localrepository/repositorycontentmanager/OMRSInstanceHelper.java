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
package org.apache.atlas.omrs.localrepository.repositorycontentmanager;


import org.apache.atlas.omrs.ffdc.exception.TypeErrorException;
import org.apache.atlas.omrs.metadatacollection.properties.instances.*;

import java.util.List;


/**
 * OMRSInstanceHelper provides methods to help OMRS connectors and adapters ensure the content of
 * entities and relationships match the type definitions recorded in the TypeDefs.
 */
public interface OMRSInstanceHelper
{
    /**
     * Return an entity with the header and type information filled out.  The caller only needs to add properties
     * and classifications to complete the set up of the entity.
     *
     * @param sourceName - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType - origin of the entity
     * @param userName - name of the creator
     * @param typeName - name of the type
     * @return partially filled out entity - needs classifications and properties
     * @throws TypeErrorException - the type name is not recognized.
     */
    EntityDetail getSkeletonEntity(String                  sourceName,
                                   String                  metadataCollectionId,
                                   InstanceProvenanceType  provenanceType,
                                   String                  userName,
                                   String                  typeName) throws TypeErrorException;


    /**
     * Return a classification with the header and type information filled out.  The caller only needs to add properties
     * and possibility origin information if it is propagated to complete the set up of the classification.
     *
     * @param sourceName - source of the request (used for logging)
     * @param userName - name of the creator
     * @param classificationTypeName - name of the classification type
     * @param entityTypeName - name of the type for the entity that this classification is to be attached to.
     * @return partially filled out classification - needs properties and possibly origin information
     * @throws TypeErrorException - the type name is not recognized as a classification type.
     */
    Classification getSkeletonClassification(String       sourceName,
                                             String       userName,
                                             String       classificationTypeName,
                                             String       entityTypeName) throws TypeErrorException;


    /**
     * Return a relationship with the header and type information filled out.  The caller only needs to add properties
     * to complete the set up of the relationship.
     *
     * @param sourceName - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType - origin type of the relationship
     * @param userName - name of the creator
     * @param typeName - name of the relationship's type
     * @return partially filled out relationship - needs properties
     * @throws TypeErrorException - the type name is not recognized as a relationship type.
     */
    Relationship getSkeletonRelationship(String                  sourceName,
                                         String                  metadataCollectionId,
                                         InstanceProvenanceType  provenanceType,
                                         String                  userName,
                                         String                  typeName) throws TypeErrorException;


    /**
     * Return a filled out entity.  It just needs to add the classifications.
     *
     * @param sourceName - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType - origin of the entity
     * @param userName - name of the creator
     * @param typeName - name of the type
     * @param properties - properties for the entity
     * @param classifications - list of classifications for the entity
     * @return an entity that is filled out
     * @throws TypeErrorException - the type name is not recognized as an entity type
     */
    EntityDetail getNewEntity(String                    sourceName,
                              String                    metadataCollectionId,
                              InstanceProvenanceType    provenanceType,
                              String                    userName,
                              String                    typeName,
                              InstanceProperties        properties,
                              List<Classification>      classifications) throws TypeErrorException;


    /**
     * Return a filled out relationship.
     *
     * @param sourceName - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType - origin of the relationship
     * @param userName - name of the creator
     * @param typeName - name of the type
     * @param properties - properties for the relationship
     * @return a relationship that is filled out
     * @throws TypeErrorException - the type name is not recognized as a relationship type
     */
    Relationship getNewRelationship(String                  sourceName,
                                    String                  metadataCollectionId,
                                    InstanceProvenanceType  provenanceType,
                                    String                  userName,
                                    String                  typeName,
                                    InstanceProperties      properties) throws TypeErrorException;


    /**
     * Return a classification with the header and type information filled out.  The caller only needs to add properties
     * to complete the set up of the classification.
     *
     * @param sourceName - source of the request (used for logging)
     * @param userName - name of the creator
     * @param typeName - name of the type
     * @param entityTypeName - name of the type for the entity that this classification is to be attached to.
     * @param classificationOrigin - origin of classification
     * @param classificationOriginGUID - GUID of original classification if propagated
     * @param properties - properties for the classification
     * @return partially filled out classification - needs properties and possibly origin information
     * @throws TypeErrorException - the type name is not recognized as a classification type.
     */
    Classification getNewClassification(String               sourceName,
                                        String               userName,
                                        String               typeName,
                                        String               entityTypeName,
                                        ClassificationOrigin classificationOrigin,
                                        String               classificationOriginGUID,
                                        InstanceProperties   properties) throws TypeErrorException;
}

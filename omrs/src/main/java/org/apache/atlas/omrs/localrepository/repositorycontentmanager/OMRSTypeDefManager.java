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
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceStatus;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceType;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.AttributeTypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefCategory;


/**
 * OMRSTypeDefManager provides maintenance methods for managing the TypeDefs in the local cache and supporting
 * the OMRSInstanceHelper to build instances that respect their type definition (TypeDef).
 */
public interface OMRSTypeDefManager
{
    /**
     * Cache a definition of a new TypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param newTypeDef - TypeDef structure describing the new TypeDef.
     */
    void addTypeDef(String  sourceName, TypeDef      newTypeDef);


    /**
     * Cache a definition of a new AttributeTypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param newAttributeTypeDef - AttributeTypeDef structure describing the new TypeDef.
     */
    void addAttributeTypeDef(String  sourceName, AttributeTypeDef newAttributeTypeDef);


    /**
     * Update one or more properties of a cached TypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDef - TypeDef structure.
     */
    void updateTypeDef(String  sourceName, TypeDef   typeDef);


    /**
     * Delete a cached TypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param obsoleteTypeDefGUID - String unique identifier for the TypeDef.
     * @param obsoleteTypeDefName - String unique name for the TypeDef.
     */
    void deleteTypeDef(String    sourceName,
                       String    obsoleteTypeDefGUID,
                       String    obsoleteTypeDefName);


    /**
     * Delete a cached AttributeTypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param obsoleteTypeDefGUID - String unique identifier for the AttributeTypeDef.
     * @param obsoleteTypeDefName - String unique name for the AttributeTypeDef.
     */
    void deleteAttributeTypeDef(String    sourceName,
                                String    obsoleteTypeDefGUID,
                                String    obsoleteTypeDefName);


    /**
     * Change the identifiers for a TypeDef.
     *
     * @param sourceName - source of the request (used for logging).
     * @param originalTypeDefGUID - TypeDef's original unique identifier.
     * @param originalTypeDefName - TypeDef's original unique name.
     * @param newTypeDef - updated TypeDef with new identifiers.
     */
    void reIdentifyTypeDef(String   sourceName,
                           String   originalTypeDefGUID,
                           String   originalTypeDefName,
                           TypeDef  newTypeDef);


    /**
     * Change the identifiers for an AttributeTypeDef.
     *
     * @param sourceName - source of the request (used for logging).
     * @param originalAttributeTypeDefGUID - AttributeTypeDef's original unique identifier.
     * @param originalAttributeTypeDefName - AttributeTypeDef's original unique name.
     * @param newAttributeTypeDef - updated AttributeTypeDef with new identifiers
     */
    void reIdentifyAttributeTypeDef(String            sourceName,
                                    String            originalAttributeTypeDefGUID,
                                    String            originalAttributeTypeDefName,
                                    AttributeTypeDef  newAttributeTypeDef);


    /**
     * Return a boolean indicating that the type name matches the category.
     *
     * @param sourceName - source of the request (used for logging)
     * @param category - TypeDefCategory enum value to test
     * @param typeName - type name to test
     * @return - boolean flag indicating that the type name is of the specified category
     * @throws TypeErrorException - the type name is not a recognized type or the category is incorrect or there
     *                              is an error in the type definition (TypeDef) cached.
     */
    boolean    isValidTypeCategory(String            sourceName,
                                   TypeDefCategory   category,
                                   String            typeName) throws TypeErrorException;


    /**
     * Return boolean indicating if a classification type can be applied to a specified entity.  This
     * uses the list of valid entity types located in the ClassificationDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param classificationTypeName - name of the classification's type (ClassificationDef)
     * @param entityTypeName - name of the entity's type (EntityDef)
     * @return boolean indicating if the classification is valid for the entity.
     * @throws TypeErrorException - the type name is not a recognized type or the category is incorrect or there
     *                              is an error in the type definition (TypeDef) cached.
     */
    boolean    isValidClassificationForEntity(String  sourceName,
                                              String  classificationTypeName,
                                              String  entityTypeName) throws TypeErrorException;


    /**
     * Return identifiers for the TypeDef that matches the supplied type name.  If the type name is not recognized,
     * null is returned.
     *
     * @param sourceName - source of the request (used for logging)
     * @param category - category of type
     * @param typeName - String type name - the type name is not recognized or of the wrong category.
     * @return InstanceType object containing TypeDef unique identifier (guid), typeDef name and versionName
     * @throws TypeErrorException - the type name is not a recognized type or the category is incorrect or there
     *                              is an error in the type definition (TypeDef) cached.
     */
    InstanceType getInstanceType(String            sourceName,
                                 TypeDefCategory   category,
                                 String            typeName) throws TypeErrorException;


    /**
     * Return the initial status set up for the instance.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeName - name of the type to extract the initial status from.
     * @return InstanceStatus enum
     * @throws TypeErrorException - the type name is not recognized.
     */
    InstanceStatus getInitialStatus(String sourceName, String typeName) throws TypeErrorException;


    /**
     * Return the URL string to use for direct access to the metadata instance.  This can be used for
     * entities and relationships.  However, not all servers support direct access, in which case, this
     * URL is null.
     *
     * @param sourceName - source of the request (used for logging)
     * @param guid - unique identifier for the instance.
     * @return String URL with placeholder for variables such as userId.
     */
    String getInstanceURL(String sourceName, String guid);
}

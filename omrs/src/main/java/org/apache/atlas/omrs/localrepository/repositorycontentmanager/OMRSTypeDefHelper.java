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
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.AttributeTypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefGallery;


/**
 * OMRSTypeDefHelper provides methods for manipulating TypeDefs and creating metadata instances with the correct TypeDef
 * headers
 */
public interface OMRSTypeDefHelper
{
    /**
     * Return the list of typedefs active in the local repository.
     *
     * @return TypeDef gallery
     */
    TypeDefGallery   getActiveTypeDefGallery();


    /**
     * Return the list of typedefs known by the local repository.
     *
     * @return TypeDef gallery
     */
    TypeDefGallery   getKnownTypeDefGallery();


    /**
     * Return the TypeDef identified by the name supplied by the caller.  This is used in the connectors when
     * validating the actual types of the repository with the known open metadata types - looking specifically
     * for types of the same name but with different content.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefName - unique name for the TypeDef
     * @return TypeDef object or null if TypeDef is not known.
     */
    TypeDef  getTypeDefByName (String    sourceName,
                               String    typeDefName);


    /**
     * Return the AttributeTypeDef identified by the name supplied by the caller.  This is used in the connectors when
     * validating the actual types of the repository with the known open metadata types - looking specifically
     * for types of the same name but with different content.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefName - unique name for the TypeDef
     * @return AttributeTypeDef object or null if AttributeTypeDef is not known.
     */
    AttributeTypeDef getAttributeTypeDefByName (String    sourceName,
                                                String    attributeTypeDefName);


    /**
     * Return the TypeDefs identified by the name supplied by the caller.  The TypeDef name may have wild
     * card characters in it which is why the results are returned in a list.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefName - unique name for the TypeDef
     * @return TypeDef object or null if TypeDef is not known.
     */
    TypeDefGallery getActiveTypesByWildCardName (String    sourceName,
                                                 String    typeDefName);


    /**
     * Return the TypeDef identified by the guid supplied by the caller.  This call is used when
     * retrieving a type that only the guid is known.
     *
     * @param sourceName - source of the request (used for logging)
     * @param parameterName - name of parameter
     * @param typeDefGUID - unique identifier for the TypeDef
     * @param methodName - calling method
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    TypeDef  getTypeDef (String    sourceName,
                         String    parameterName,
                         String    typeDefGUID,
                         String    methodName) throws TypeErrorException;


    /**
     * Return the AttributeTypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that only the guid is known.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefGUID - unique identifier for the AttributeTypeDef
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    AttributeTypeDef  getAttributeTypeDef (String    sourceName,
                                           String    attributeTypeDefGUID,
                                           String    methodName) throws TypeErrorException;



    /**
     * Return the TypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that should exist.  For example, retrieving the type of a metadata instance.
     *
     * @param sourceName - source of the request (used for logging)
     * @param guidParameterName - name of guid parameter
     * @param nameParameterName - name of name parameter
     * @param typeDefGUID - unique identifier for the TypeDef
     * @param typeDefName - unique name for the TypeDef
     * @param methodName - calling method
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    TypeDef  getTypeDef (String    sourceName,
                         String    guidParameterName,
                         String    nameParameterName,
                         String    typeDefGUID,
                         String    typeDefName,
                         String    methodName) throws TypeErrorException;


    /**
     * Return the AttributeTypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that should exist.  For example, retrieving the type definition of a metadata instance's
     * property.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefGUID - unique identifier for the AttributeTypeDef
     * @param attributeTypeDefName - unique name for the AttributeTypeDef
     * @param methodName - calling method
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    AttributeTypeDef  getAttributeTypeDef (String    sourceName,
                                           String    attributeTypeDefGUID,
                                           String    attributeTypeDefName,
                                           String    methodName) throws TypeErrorException;
}

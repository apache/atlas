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
package org.apache.atlas.omrs.rest.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.AttributeTypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefGallery;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * TypeDefGalleryResponse provides the response structure for an OMRS REST API call that returns a TypeDefGallery.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class TypeDefGalleryResponse extends OMRSRESTAPIResponse
{
    private ArrayList<AttributeTypeDef> attributeTypeDefs = null;
    private ArrayList<TypeDef>          typeDefs          = null;


    /**
     * Default constructor
     */
    public TypeDefGalleryResponse()
    {
    }


    /**
     * Return the list of attribute type definitions from this gallery.
     *
     * @return list of AttributeTypeDefs
     */
    public List<AttributeTypeDef> getAttributeTypeDefs()
    {
        return attributeTypeDefs;
    }


    /**
     * Set up the list of attribute type definitions from this gallery.
     *
     * @param attributeTypeDefs - list of AttributeTypeDefs
     */
    public void setAttributeTypeDefs(List<AttributeTypeDef> attributeTypeDefs)
    {
        this.attributeTypeDefs = new ArrayList<>(attributeTypeDefs);
    }


    /**
     * Return the list of type definitions from this gallery
     *
     * @return list of TypeDefs
     */
    public List<TypeDef> getTypeDefs()
    {
        return typeDefs;
    }


    /**
     * Set up the list of type definitions for this gallery.
     *
     * @param typeDefs - list of type definitions
     */
    public void setTypeDefs(List<TypeDef> typeDefs)
    {
        this.typeDefs = new ArrayList<>(typeDefs);
    }


    @Override
    public String toString()
    {
        return "TypeDefGalleryResponse{" +
                "attributeTypeDefs=" + attributeTypeDefs +
                ", typeDefs=" + typeDefs +
                ", relatedHTTPCode=" + relatedHTTPCode +
                ", exceptionClassName='" + exceptionClassName + '\'' +
                ", exceptionErrorMessage='" + exceptionErrorMessage + '\'' +
                ", exceptionSystemAction='" + exceptionSystemAction + '\'' +
                ", exceptionUserAction='" + exceptionUserAction + '\'' +
                '}';
    }
}

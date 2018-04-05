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
package org.apache.atlas.omrs.archivemanager.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.AttributeTypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefPatch;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * OpenMetadataArchiveTypeStore defines the contents of the TypeStore in an open metadata archive.  The TypeStore
 * contains a list of types used for attributes, a list of type definition (TypeDef) patches to update existing types
 * and a list of TypeDefs for new types of classifications, entities and relationships.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class OpenMetadataArchiveTypeStore
{
    private ArrayList<AttributeTypeDef> attributeTypeDefs = null;
    private ArrayList<TypeDefPatch>     typeDefPatches    = null;
    private ArrayList<TypeDef>          newTypeDefs       = null;


    /**
     * Default constructor for OpenMetadataArchiveTypeStore relies on variables being initialized in their declaration.
     */
    public OpenMetadataArchiveTypeStore()
    {
    }


    /**
     * Return the list of attribute types used in this archive.
     *
     * @return list of AttributeTypeDef objects
     */
    public List<AttributeTypeDef> getAttributeTypeDefs()
    {
        if (attributeTypeDefs == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(attributeTypeDefs);
        }
    }


    /**
     * Set up the list of attribute types used in this archive.
     *
     * @param attributeTypeDefs - list of AttributeTypeDef objects
     */
    public void setAttributeTypeDefs(List<AttributeTypeDef> attributeTypeDefs)
    {
        if (attributeTypeDefs == null)
        {
            this.attributeTypeDefs = null;
        }
        else
        {
            this.attributeTypeDefs = new ArrayList<>(attributeTypeDefs);
        }
    }


    /**
     * Return the list of TypeDef patches from this archive.
     *
     * @return list of TypeDef objects
     */
    public List<TypeDefPatch> getTypeDefPatches()
    {
        if (typeDefPatches == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(typeDefPatches);
        }
    }


    /**
     * Set up the list of TypeDef patches from this archive.
     *
     * @param typeDefPatches - list of TypeDef objects
     */
    public void setTypeDefPatches(List<TypeDefPatch> typeDefPatches)
    {
        if (typeDefPatches == null)
        {
            this.typeDefPatches =  null;
        }
        else
        {
            this.typeDefPatches =  new ArrayList<>(typeDefPatches);
        }
    }


    /**
     * Return the list of new TypeDefs in this open metadata archive.
     *
     * @return list of TypeDef objects
     */
    public List<TypeDef> getNewTypeDefs()
    {
        if (newTypeDefs == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(newTypeDefs);
        }
    }


    /**
     * Set up the list of new TypeDefs in this open metadata archive.
     *
     * @param newTypeDefs - list of TypeDef objects
     */
    public void setNewTypeDefs(List<TypeDef> newTypeDefs)
    {
        if (newTypeDefs == null)
        {
            this.newTypeDefs = null;
        }
        else
        {
            this.newTypeDefs = new ArrayList<>(newTypeDefs);
        }
    }
}

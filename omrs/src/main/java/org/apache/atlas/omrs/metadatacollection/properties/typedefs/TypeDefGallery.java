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

package org.apache.atlas.omrs.metadatacollection.properties.typedefs;

import java.util.ArrayList;

/**
 * TypeDefGallery contains details of the AttributeTypeDefs and full TypeDefs supported by a rep
 */
public class TypeDefGallery
{
    private ArrayList<AttributeTypeDef> attributeTypeDefs = null;
    private ArrayList<TypeDef>          typeDefs          = null;


    /**
     * Default constructor
     */
    public TypeDefGallery()
    {
    }


    /**
     * Copy/clone constructor
     *
     * @param template - template to copy
     */
    public TypeDefGallery(TypeDefGallery    template)
    {
        if (template != null)
        {
            ArrayList<AttributeTypeDef> templateAttributeTypeDefs = template.getAttributeTypeDefs();
            ArrayList<TypeDef>          templateTypeDefs          = template.getTypeDefs();

            if (templateAttributeTypeDefs != null)
            {
                attributeTypeDefs = new ArrayList<>(templateAttributeTypeDefs);
            }

            if (templateTypeDefs != null)
            {
                typeDefs = new ArrayList<>(templateTypeDefs);
            }
        }
    }


    /**
     * Return the list of attribute type definitions from the gallery.
     *
     * @return list of attribute type definitions
     */
    public ArrayList<AttributeTypeDef> getAttributeTypeDefs()
    {
        if (attributeTypeDefs == null)
        {
            return attributeTypeDefs;
        }
        else
        {
            return new ArrayList<>(attributeTypeDefs);
        }
    }


    /**
     * Set up the list of attribute type definitions from the gallery.
     *
     * @param attributeTypeDefs - list of attribute type definitions
     */
    public void setAttributeTypeDefs(ArrayList<AttributeTypeDef> attributeTypeDefs)
    {
        this.attributeTypeDefs = attributeTypeDefs;
    }


    /**
     * Return the list of type definitions from the gallery.
     *
     * @return list of type definitions
     */
    public ArrayList<TypeDef> getTypeDefs()
    {
        if (typeDefs == null)
        {
            return typeDefs;
        }
        else
        {
            return new ArrayList<>(typeDefs);
        }
    }


    /**
     * Set up the list of type definitions from the gallery.
     *
     * @param typeDefs - list of type definitions
     */
    public void setTypeDefs(ArrayList<TypeDef> typeDefs)
    {
        this.typeDefs = typeDefs;
    }
}

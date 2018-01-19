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

package org.apache.atlas.ocf.properties;


/**
 * Many open metadata entities are referenceable.  It means that they have a qualified name and additional
 * properties.  In addition the Referenceable class adds support for the parent asset, guid, url and type
 * for the entity through extending ElementHeader.
 */
public class Referenceable extends ElementHeader
{
    /*
     * Attributes of a Referenceable
     */
    protected  String                 qualifiedName = null;
    protected  AdditionalProperties   additionalProperties = null;

    /*
     * Attached glossary terms
     */
    protected  Meanings               meanings = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object.
     * @param meanings - list of glossary terms (summary)
     */
    public Referenceable(AssetDescriptor      parentAsset,
                         ElementType          type,
                         String               guid,
                         String               url,
                         Classifications      classifications,
                         String               qualifiedName,
                         AdditionalProperties additionalProperties,
                         Meanings             meanings)
    {
        super(parentAsset, type, guid, url, classifications);

        this.qualifiedName = qualifiedName;
        this.additionalProperties = additionalProperties;
        this.meanings = meanings;
    }


    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateReferenceable - element to copy
     */
    public Referenceable(AssetDescriptor parentAsset, Referenceable templateReferenceable)
    {
        /*
         * Save the parent asset description.
         */
        super(parentAsset, templateReferenceable);

        if (templateReferenceable != null)
        {
            /*
             * Copy the qualified name from the supplied template.
             */
            qualifiedName = templateReferenceable.getQualifiedName();

            /*
             * Create a copy of the additional properties since the parent asset may have changed.
             */
            AdditionalProperties   templateAdditionalProperties = templateReferenceable.getAdditionalProperties();
            if (templateAdditionalProperties != null)
            {
                additionalProperties = new AdditionalProperties(parentAsset, templateAdditionalProperties);
            }

            /*
             * Create a copy of any glossary terms
             */
            Meanings  templateMeanings = templateReferenceable.getMeanings();
            if (templateMeanings != null)
            {
                meanings = templateMeanings.cloneIterator(parentAsset);
            }
        }
    }


    /**
     * Returns the stored qualified name property for the metadata entity.
     * If no qualified name is available then the empty string is returned.
     *
     * @return qualifiedName
     */
    public String getQualifiedName()
    {
        return qualifiedName;
    }


    /**
     * Return a copy of the additional properties.  Null means no additional properties are available.
     *
     * @return AdditionalProperties
     */
    public AdditionalProperties getAdditionalProperties()
    {
        if (additionalProperties == null)
        {
            return additionalProperties;
        }
        else
        {
            return new AdditionalProperties(super.getParentAsset(), additionalProperties);
        }
    }


    /**
     * Return a list of the glossary terms attached to this referenceable object.  Null means no terms available.
     *
     * @return list of glossary terms (summary)
     */
    public Meanings getMeanings()
    {
        if (meanings == null)
        {
            return meanings;
        }
        else
        {
            return meanings.cloneIterator(super.getParentAsset());
        }
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "Referenceable{" +
                "qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
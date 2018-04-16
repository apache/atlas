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

package org.apache.atlas.omas.connectedasset.properties;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Many open metadata entities are referenceable.  It means that they have a qualified name and additional
 * properties.  In addition the Referenceable class adds support for the parent asset, guid, url and type
 * for the entity through extending ElementHeader.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Referenceable extends ElementHeader
{
    protected String                 qualifiedName = null;
    protected AdditionalProperties   additionalProperties = null;
    protected List<Meaning>          meanings = null;


    /**
     * Default Constructor
     */
    public Referenceable()
    {
        super();
    }


    /**
     * Copy/clone constructor.
     *
     * @param templateReferenceable - element to copy
     */
    public Referenceable(Referenceable templateReferenceable)
    {
        /*
         * Save the parent asset description.
         */
        super(templateReferenceable);

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
                additionalProperties = new AdditionalProperties(templateAdditionalProperties);
            }


            /*
             * Create a copy of any glossary terms
             */
            List<Meaning>  templateMeanings = templateReferenceable.getMeanings();
            if (templateMeanings != null)
            {
                meanings = new ArrayList<>(templateMeanings);
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
     * Updates the qualified name property stored for the metadata entity.
     * If a null is supplied it means no qualified name is available.
     *
     * @param  newQualifiedName - unique name
     */
    public void setQualifiedName(String  newQualifiedName) { qualifiedName = newQualifiedName; }


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
            return new AdditionalProperties(additionalProperties);
        }
    }


    /**
     * Set up a new additional properties object.
     *
     * @param newAdditionalProperties - additional properties for the referenceable object.
     */
    public void setAdditionalProperties(AdditionalProperties newAdditionalProperties)
    {
        additionalProperties = newAdditionalProperties;
    }


    /**
     * Return a list of the glossary terms attached to this referenceable object.  Null means no terms available.
     *
     * @return list of glossary terms (summary)
     */
    public List<Meaning> getMeanings()
    {
        if (meanings == null)
        {
            return meanings;
        }
        else
        {
            return new ArrayList<>(meanings);
        }
    }

    /**
     * Set up a list of the glossary terms attached to this referenceable object.  Null means no terms available.
     *
     * @param meanings - list of glossary terms (summary)
     */
    public void setMeanings(List<Meaning> meanings) { this.meanings = meanings; }
}
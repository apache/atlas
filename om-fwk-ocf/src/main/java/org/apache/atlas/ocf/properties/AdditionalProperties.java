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

import java.util.*;


/**
 * The AdditionalProperties class provides support for arbitrary properties to be added to a properties object.
 * It wraps a java.util.Map map object built around HashMap.
 */
public class AdditionalProperties extends AssetPropertyBase
{
    protected Map<String,Object>  additionalProperties = new HashMap<>();


    /**
     * Constructor for a new set of additional properties that are not connected either directly or indirectly to an asset.
     *
     * @param additionalProperties - map of properties for the metadata element.
     */
    public AdditionalProperties(Map<String,Object>  additionalProperties)
    {
        this(null, additionalProperties);
    }


    /**
     * Constructor for a new set of additional properties that are connected either directly or indirectly to an asset.
     *
     * @param parentAsset - description of the asset that these additional properties are attached to.
     * @param additionalProperties - map of properties for the metadata element.
     */
    public AdditionalProperties(AssetDescriptor     parentAsset,
                                Map<String,Object>  additionalProperties)
    {
        super(parentAsset);

        if (additionalProperties != null)
        {
            this.additionalProperties = new HashMap<>(additionalProperties);
        }
    }


    /**
     * Copy/clone Constructor for additional properties that are connected to an asset.
     *
     * @param parentAsset - description of the asset that these additional properties are attached to.
     * @param templateProperties - template object to copy.
     */
    public AdditionalProperties(AssetDescriptor   parentAsset, AdditionalProperties templateProperties)
    {
        super(parentAsset, templateProperties);

        /*
         * An empty properties object is created in the private variable declaration so nothing to do.
         */
        if (templateProperties != null)
        {
            /*
             * Process templateProperties if they are not null
             */
            Iterator<String> propertyNames = templateProperties.getPropertyNames();

            if (propertyNames != null)
            {
                while (propertyNames.hasNext())
                {
                    String newPropertyName = propertyNames.next();
                    Object newPropertyValue = templateProperties.getProperty(newPropertyName);

                    additionalProperties.put(newPropertyName, newPropertyValue);
                }
            }
        }
    }


    /**
     * Returns a list of the additional stored properties for the element.
     * If no stored properties are present then null is returned.
     *
     * @return list of additional properties
     */
    public Iterator<String> getPropertyNames()
    {
        return additionalProperties.keySet().iterator();
    }


    /**
     * Returns the requested additional stored property for the element.
     * If no stored property with that name is present then null is returned.
     *
     * @param name - String name of the property to return.
     * @return requested property value.
     */
    public Object getProperty(String name)
    {
        return additionalProperties.get(name);
    }


    /**
     * Test whether the supplied object is equal to this object.
     *
     * @param testObject - object to test
     * @return boolean indicating if the supplied object represents the same content as this object.
     */
    @Override
    public boolean equals(Object testObject)
    {
        if (this == testObject)
        {
            return true;
        }
        if (testObject == null || getClass() != testObject.getClass())
        {
            return false;
        }

        AdditionalProperties that = (AdditionalProperties) testObject;

        return additionalProperties != null ? additionalProperties.equals(that.additionalProperties) : that.additionalProperties == null;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "AdditionalProperties{" +
                "additionalProperties=" + additionalProperties +
                '}';
    }
}
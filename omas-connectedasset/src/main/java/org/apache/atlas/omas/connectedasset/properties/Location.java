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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Location describes where the asset is located.  The model allows a very flexible definition of location
 * that can be set up at different levels of granularity.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Location extends Referenceable
{
    /*
     * Properties that make up the location of the asset.
     */
    private String displayName = null;
    private String description = null;


    /**
     * Default constructor
     */
    public Location()
    {
        super();
    }


    /**
     * Copy/clone constructor
     *
     * @param templateLocation - template object to copy.
     */
    public Location(Location   templateLocation)
    {
        super(templateLocation);
        if (templateLocation != null)
        {
            displayName = templateLocation.getDisplayName();
            description = templateLocation.getDescription();
        }
    }


    /**
     * Returns the stored display name property for the location.
     * If no display name is available then null is returned.
     *
     * @return displayName
     */
    public String getDisplayName()
    {
        return displayName;
    }


    /**
     * Updates the display name property stored for the location.
     * If a null is supplied it clears the display name.
     *
     * @param  newDisplayName - consumable name
     */
    public void setDisplayName(String  newDisplayName)
    {
        displayName = newDisplayName;
    }


    /**
     * Returns the stored description property for the location.
     * If no description is provided then null is returned.
     *
     * @return description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Updates the description property stored for the location.
     * If a null is supplied it clears any saved description.
     *
     * @param  newDescription - description
     */
    public void setDescription(String  newDescription) { description = newDescription; }
}
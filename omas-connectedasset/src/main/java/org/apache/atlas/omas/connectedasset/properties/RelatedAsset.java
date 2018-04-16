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
 * RelatedAsset describes assets that are related to this asset.  For example, if the asset is a data store, the
 * related assets could be its supported data sets.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class RelatedAsset extends Referenceable
{
    /*
     * Properties that make up the summary properties of the related asset.
     */
    private String displayName = null;
    private String description = null;
    private String owner = null;


    /**
     * Default constructor
     */
    public RelatedAsset()
    {
        super();
    }


    /**
     * Copy/clone constructor
     *
     * @param templateRelatedAsset - template object to copy.
     */
    public RelatedAsset(RelatedAsset templateRelatedAsset)
    {
        super(templateRelatedAsset);
        if (templateRelatedAsset != null)
        {
            displayName = templateRelatedAsset.getDisplayName();
            description = templateRelatedAsset.getDescription();
            owner = templateRelatedAsset.getOwner();
        }
    }


    /**
     * Returns the stored display name property for the related asset.
     * If no display name is available then null is returned.
     *
     * @return displayName
     */
    public String getDisplayName()
    {
        return displayName;
    }


    /**
     * Updates the display name property stored for the related asset.
     * If a null is supplied it clears the display name.
     *
     * @param  newDisplayName - consumable name
     */
    public void setDisplayName(String  newDisplayName)
    {
        displayName = newDisplayName;
    }


    /**
     * Returns the stored description property for the related asset.
     * If no description is provided then null is returned.
     *
     * @return description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Updates the description property stored for the related asset.
     * If a null is supplied it clears any saved description.
     *
     * @param  newDescription - description
     */
    public void setDescription(String  newDescription) { description = newDescription; }


    /**
     * Returns the details of the owner for this related asset.
     *
     * @return String owner
     */
    public String getOwner() { return owner; }


    /**
     * Set up the owner details for this related asset.  This could be the name of the owner, website, userid ...
     * If null is supplied, it clears any saved owner details.
     *
     * @param owner - String
     */
    public void setOwner(String owner) { this.owner = owner; }


    /**
     * Return the detailed properties for a related asset.
     *
     * @return RelatedAssetProperties
     */
    public RelatedAssetProperties getRelatedAssetProperties()
    {
        return new RelatedAssetProperties(this);
    }
}
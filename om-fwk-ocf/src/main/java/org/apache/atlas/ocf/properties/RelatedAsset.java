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

import org.apache.atlas.ocf.ffdc.PropertyServerException;

/**
 * RelatedAsset describes assets that are related to this asset.  For example, if the asset is a data store, the
 * related assets could be its supported data sets.
 */
public class RelatedAsset extends Referenceable
{
    /*
     * Properties that make up the summary properties of the related asset.
     */
    private String    displayName = null;
    private String    description = null;
    private String    owner = null;

    /*
     * The detailed properties that are retrieved from the server
     */
    private RelatedAssetProperties  relatedAssetProperties = null;


    /**
     * Typical constructor
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object.
     * @param meanings - list of glossary terms (summary)
     * @param displayName - consumable name
     * @param description - description property stored for the related asset.
     * @param owner - the owner details for this related asset.
     * @param relatedAssetProperties - detailed properties of the asset.
     */
    public RelatedAsset(AssetDescriptor         parentAsset,
                        ElementType             type,
                        String                  guid,
                        String                  url,
                        Classifications         classifications,
                        String                  qualifiedName,
                        AdditionalProperties    additionalProperties,
                        Meanings                meanings,
                        String                  displayName,
                        String                  description,
                        String                  owner,
                        RelatedAssetProperties  relatedAssetProperties)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.displayName = displayName;
        this.description = description;
        this.owner = owner;
        this.relatedAssetProperties = relatedAssetProperties;
    }


    /**
     * Copy/clone constructor
     *
     * @param parentAsset - description of the asset that this related asset is attached to.
     * @param templateRelatedAsset - template object to copy.
     */
    public RelatedAsset(AssetDescriptor  parentAsset, RelatedAsset templateRelatedAsset)
    {
        super(parentAsset, templateRelatedAsset);
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
     * Returns the details of the owner for this related asset.
     *
     * @return String owner
     */
    public String getOwner() { return owner; }


    /**
     * Return the detailed properties for a related asset.
     *
     * @return a refreshed version of the RelatedAssetProperties
     * @throws PropertyServerException - problems communicating with the property (metadata) server
     */
    public RelatedAssetProperties getRelatedAssetProperties() throws PropertyServerException
    {
        if (relatedAssetProperties != null)
        {
            relatedAssetProperties.refresh();
        }

        return relatedAssetProperties;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "RelatedAsset{" +
                "displayName='" + displayName + '\'' +
                ", description='" + description + '\'' +
                ", owner='" + owner + '\'' +
                ", relatedAssetProperties=" + relatedAssetProperties +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
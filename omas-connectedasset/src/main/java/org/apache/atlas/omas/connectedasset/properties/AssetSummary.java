/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
 * AssetSummary is a POJO that holds asset properties that are used for displaying details of
 * an asset in summary lists or hover text.  It includes the following properties:
 * <ul>
 *     <li>type - metadata type information for the asset properties</li>
 *     <li>guid - globally unique identifier for the asset</li>
 *     <li>url - external link for the asset</li>
 *     <li>qualifiedName - The official (unique) name for the asset. This is often defined by the IT systems
 *     management organization and should be used (when available) on audit logs and error messages.
 *     (qualifiedName from Referenceable - model 0010)</li>
 *     <li>displayName - A consumable name for the endpoint.  Often a shortened form of the assetQualifiedName
 *     for use on user interfaces and messages.   The assetDisplayName should be only be used for audit logs and error
 *     messages if the assetQualifiedName is not set. (Sourced from attribute name within Asset - model 0010)</li>
 *     <li>shortDescription - short description about the asset.
 *     (Sourced from assetSummary within ConnectionsToAsset - model 0205)</li>
 *     <li>description - full description of the asset.
 *     (Sourced from attribute description within Asset - model 0010)</li>
 *     <li>owner - name of the person or organization that owns the asset.
 *     (Sourced from attribute owner within Asset - model 0010)</li>
 *     <li>classifications - list of classifications assigned to the asset</li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class AssetSummary extends AssetDescriptor
{
    /*
     * Official type definition for this asset
     */
    private ElementType type = null;

    /*
     * Standard header for any metadata entity
     */
    private String guid = null;
    private String url = null;

    /*
     * Base attributes from Referenceable and Asset
     */
    private String qualifiedName = null;
    private String displayName = null;
    private String shortDescription = null;
    private String description = null;
    private String owner = null;

    /*
     * Attached classifications
     */
    private List<Classification> classifications = null;


    /**
     * Typical Constructor - the AssetSummary is empty
     */
    public AssetSummary()
    {
        /*
         * Initialize super class
         */
        super();
    }


    /**
     * Copy/clone constructor.  Note, this is a deep copy
     *
     * @param templateAssetSummary - template values for asset summary
     */
    public AssetSummary(AssetSummary   templateAssetSummary)
    {
        /*
         * Initialize super class
         */
        super(templateAssetSummary);

        /*
         * Copy relevant values from the template
         */
        if (templateAssetSummary != null)
        {
            type = templateAssetSummary.getType();
            guid = templateAssetSummary.getGUID();
            url = templateAssetSummary.getURL();
            qualifiedName = templateAssetSummary.getQualifiedName();
            displayName = templateAssetSummary.getDisplayName();
            shortDescription = templateAssetSummary.getShortDescription();
            description = templateAssetSummary.getDescription();
            owner = templateAssetSummary.getOwner();

            List<Classification>  templateClassifications = templateAssetSummary.getClassifications();
            if (templateClassifications != null)
            {
                classifications = new ArrayList<>(templateClassifications);
            }

        }
    }


    /**
     * Return the element type properties for this asset.  These values are set up by the metadata repository
     * and define details to the metadata entity used to represent this element.
     *
     * @return ElementType - type information.
     */
    public ElementType getType()
    {
        return type;
    }


    /**
     * Set up the type properties for this element.
     *
     * @param type - details of the metadata type for this asset
     */
    public void setType(ElementType type)
    {
        if (type == null)
        {
            this.type = new ElementType();
        }
        else
        {
            this.type = type;
            super.setAssetTypeName(type.getElementTypeName());
        }
    }


    /**
     * Return the unique id for this element.
     *
     * @return guid - unique id
     */
    public String getGUID() {
        return guid;
    }


    /**
     * Updates the guid property stored for the element.
     * If a null is supplied it is saved as a null.
     *
     * @param  newGUID - guid property
     */
    public void setGUID(String  newGUID)
    {
        if (newGUID != null)
        {
            guid = newGUID;
        }
        else
        {
            guid = "";
        }
    }


    /**
     * Returns the URL for this element in the metadata repository.
     * If no URL is known for the element then null is returned.
     *
     * @return element URL
     */
    public String getURL() {
        return url;
    }


    /**
     * Updates the URL for the element used to access its properties in the metadata repository.
     * If a null is supplied it is saved as a null.
     *
     * @param url - element URL
     */
    public void setURL(String url) {
        this.url = url;
    }


    /**
     * Returns the stored qualified name property for the asset.
     * If no qualified name is provided then null is returned.
     *
     * @return qualifiedName
     */
    public String getQualifiedName() {
        return qualifiedName;
    }


    /**
     * Updates the qualified name property stored for the asset.
     * If a null is supplied it is saved as a null.
     *
     * @param  newQualifiedName - unique name
     */
    public void setQualifiedName(String  newQualifiedName)
    {
        if (newQualifiedName != null)
        {
            qualifiedName = newQualifiedName;

            /*
             * Use the qualified name as the asset name if it is not the empty string.
             */
            if (!qualifiedName.equals(""))
            {
                super.setAssetName(qualifiedName);
            }
        }
        else
        {
            qualifiedName = "";
        }
    }


    /**
     * Returns the stored display name property for the asset.
     * If no display name is available then null is returned.
     *
     * @return displayName
     */
    public String getDisplayName()
    {
        return displayName;
    }


    /**
     * Updates the display name property stored for the asset.
     * If a null is supplied it clears the display name.
     *
     * @param  newDisplayName - consumable name
     */
    public void setDisplayName(String  newDisplayName)
    {
        displayName = newDisplayName;

        if (displayName != null && (!displayName.equals("")))
        {
            /*
             * Use the display name for the asset name if qualified name is not set up.
             */
            if (qualifiedName == null || qualifiedName.equals(""))
            {
                super.setAssetName(displayName);
            }
        }
    }


    /**
     * Returns the short description of the asset from relationship with Connection.
     *
     * @return shortDescription
     */
    public String getShortDescription()
    {
        return shortDescription;
    }


    /**
     * Sets the short description for the asset.
     *
     * @param assetShortDescription - short description from relationship with Connection
     */
    public void setShortDescription(String assetShortDescription)
    {
        this.shortDescription = assetShortDescription;
    }


    /**
     * Returns the stored description property for the asset.
     * If no description is provided then null is returned.
     *
     * @return description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Updates the description property stored for the asset.
     * If a null is supplied it clears any saved description.
     *
     * @param  newDescription - description
     */
    public void setDescription(String  newDescription) { description = newDescription; }


    /**
     * Returns the name of the owner for this asset.
     *
     * @return owner
     */
    public String getOwner() {
        return owner;
    }


    /**
     * Updates the name of the owner for this asset.
     *
     * @param newOwner - owner name
     */
    public void setOwner(String newOwner) { owner = newOwner; }


    /**
     * Return the list of classifications associated with the asset.   This is an enumeration and the
     * pointers are set to the start of the list of classifications
     *
     * @return Classifications - list of classifications
     */
    public List<Classification> getClassifications()
    {
        if (classifications == null)
        {
            return classifications;
        }
        else
        {
            return new ArrayList<>(classifications);
        }
    }


    /**
     * Set up the list of classifications for this asset.
     *
     * @param classifications - list of classifications
     */
    public void setClassifications(List<Classification> classifications)
    {
        if (classifications == null)
        {
            this.classifications = classifications;
        }
        else
        {
            this.classifications = new ArrayList<>(classifications);
        }
    }
}
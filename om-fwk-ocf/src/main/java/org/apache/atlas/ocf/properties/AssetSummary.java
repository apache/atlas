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

package org.apache.atlas.ocf.properties;


/**
 * AssetSummary holds asset properties that are used for displaying details of
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
public class AssetSummary extends AssetDescriptor
{
    private ElementType     type = null;
    private String          qualifiedName = null;
    private String          displayName = null;
    private String          shortDescription = null;
    private String          description = null;
    private String          owner = null;
    private Classifications classifications = null;


    /**
     * Typical constructor with parameters to fill properties.
     *
     * @param type - details of the metadata type for this asset
     * @param guid - guid property
     * @param url - element URL used to access its properties in the metadata repository.
     * @param qualifiedName - unique name
     * @param displayName - consumable name
     * @param description - description of the asset
     * @param shortDescription - short description from relationship with Connection
     * @param owner - owner name
     * @param classifications - enumeration of classifications
     */
    public AssetSummary(ElementType     type,
                        String          guid,
                        String          url,
                        String          qualifiedName,
                        String          displayName,
                        String          shortDescription,
                        String          description,
                        String          owner,
                        Classifications classifications)
    {
        super(guid, url);

        this.type = type;
        if (type != null)
        {
            super.setAssetTypeName(type.getElementTypeName());
        }

        this.qualifiedName = qualifiedName;
        this.displayName = displayName;

        /*
         * Use the qualified name as the asset name if it is not null or the empty string.
         * Otherwise use display name (unless it is null or the empty string).
         */
        if ((qualifiedName == null) || (qualifiedName.equals("")))
        {
            if ((displayName != null) && (!displayName.equals("")))
            {
                /*
                 * Good display name
                 */
                super.setAssetName(displayName);
            }
        }
        else /* good qualified name */
        {
            super.setAssetName(qualifiedName);
        }

        this.shortDescription = shortDescription;
        this.description = description;
        this.owner = owner;
        this.classifications = classifications;
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
            qualifiedName = templateAssetSummary.getQualifiedName();
            displayName = templateAssetSummary.getDisplayName();
            shortDescription = templateAssetSummary.getShortDescription();
            description = templateAssetSummary.getDescription();
            owner = templateAssetSummary.getOwner();

            Classifications  templateClassifications = templateAssetSummary.getClassifications();
            if (templateClassifications != null)
            {
                classifications = templateClassifications.cloneIterator(this);
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
     * Returns the stored qualified name property for the asset.
     * If no qualified name is provided then null is returned.
     *
     * @return qualifiedName
     */
    public String getQualifiedName() {
        return qualifiedName;
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
     * Returns the short description of the asset from relationship with Connection.
     *
     * @return shortDescription String
     */
    public String getShortDescription()
    {
        return shortDescription;
    }


    /**
     * Returns the stored description property for the asset.
     * If no description is provided then null is returned.
     *
     * @return description String
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Returns the name of the owner for this asset.
     *
     * @return owner String
     */
    public String getOwner() {
        return owner;
    }


    /**
     * Return the list of classifications associated with the asset.   This is an enumeration and the
     * pointers are set to the start of the list of classifications
     *
     * @return Classifications - enumeration of classifications
     */
    public Classifications getClassifications()
    {
        if (classifications == null)
        {
            return classifications;
        }
        else
        {
            return classifications.cloneIterator(this);
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
        return "AssetSummary{" +
                "type=" + type +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", displayName='" + displayName + '\'' +
                ", shortDescription='" + shortDescription + '\'' +
                ", description='" + description + '\'' +
                ", owner='" + owner + '\'' +
                ", classifications=" + classifications +
                '}';
    }
}
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

import java.util.ArrayList;
import java.util.List;

/**
 * RelatedMediaReference stores information about an link to an external media file that
 * is relevant to this asset.
 */
public class RelatedMediaReference extends Referenceable
{
    /*
     * Attributes of a related media reference
     */
    private String                       mediaId = null;
    private String                       linkDescription = null;
    private String                       displayName = null;
    private String                       uri = null;
    private String                       resourceDescription = null;
    private String                       version = null;
    private String                       organization = null;
    private RelatedMediaType             mediaType = null;
    private ArrayList<RelatedMediaUsage> mediaUsageList = null;


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
     * @param mediaId the reference identifier for this asset's related media.
     * @param linkDescription - description of the reference (with respect to this asset).
     * @param displayName - consumable name
     * @param uri - the URI used to retrieve the resource for this media reference.
     * @param resourceDescription - the description of this external media.
     * @param version - the version of the resource that this external reference represents.
     * @param organization - the name of the organization that owns the resource that this external reference represents.
     * @param mediaType - the type of media referenced
     * @param mediaUsageList - List of ways the media may be used
     */
    public RelatedMediaReference(AssetDescriptor              parentAsset,
                                 ElementType                  type,
                                 String                       guid,
                                 String                       url,
                                 Classifications              classifications,
                                 String                       qualifiedName,
                                 AdditionalProperties         additionalProperties,
                                 Meanings                     meanings,
                                 String                       mediaId,
                                 String                       linkDescription,
                                 String                       displayName,
                                 String                       uri,
                                 String                       resourceDescription,
                                 String                       version,
                                 String                       organization,
                                 RelatedMediaType             mediaType,
                                 ArrayList<RelatedMediaUsage> mediaUsageList)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);
        this.mediaId = mediaId;
        this.linkDescription = linkDescription;
        this.displayName = displayName;
        this.uri = uri;
        this.resourceDescription = resourceDescription;
        this.version = version;
        this.organization = organization;
        this.mediaType = mediaType;
        this.mediaUsageList = mediaUsageList;
    }

    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateRelatedMediaReference - element to copy
     */
    public RelatedMediaReference(AssetDescriptor       parentAsset,
                                 RelatedMediaReference templateRelatedMediaReference)
    {
        /*
         * Initialize the super class.
         */
        super(parentAsset, templateRelatedMediaReference);

        if (templateRelatedMediaReference != null)
        {
            /*
             * Copy the values from the supplied template.
             */
            mediaId = templateRelatedMediaReference.getMediaId();
            linkDescription = templateRelatedMediaReference.getLinkDescription();
            displayName = templateRelatedMediaReference.getDisplayName();
            uri = templateRelatedMediaReference.getURI();
            resourceDescription = templateRelatedMediaReference.getResourceDescription();
            version = templateRelatedMediaReference.getVersion();
            organization = templateRelatedMediaReference.getOrganization();
            mediaType = templateRelatedMediaReference.getMediaType();

            List<RelatedMediaUsage> templateMediaUsageList = templateRelatedMediaReference.getMediaUsageList();
            if (templateMediaUsageList != null)
            {
                mediaUsageList = new ArrayList<RelatedMediaUsage>(templateMediaUsageList);
            }
        }
    }


    /**
     * Return the identifier given to this reference (with respect to this asset).
     *
     * @return String mediaId
     */
    public String getMediaId() { return mediaId; }


    /**
     * Return the description of the reference (with respect to this asset).
     *
     * @return String link description.
     */
    public String getLinkDescription() { return linkDescription; }


    /**
     * Return the display name of this media reference.
     *
     * @return String display name.
     */
    public String getDisplayName() { return displayName; }


    /**
     * Return the URI used to retrieve the resource for this media reference.
     *
     * @return String URI
     */
    public String getURI() { return uri; }


    /**
     * Return the description of this external media.
     *
     * @return String resource description
     */
    public String getResourceDescription() { return resourceDescription; }


    /**
     * Return the version of the resource that this media reference represents.
     *
     * @return String version
     */
    public String getVersion() { return version; }


    /**
     * Return the name of the organization that owns the resource that this external reference represents.
     *
     * @return String organization name
     */
    public String getOrganization() { return organization; }


    /**
     * Return the type of media referenced.
     *
     * @return RelatedMediaType
     */
    public RelatedMediaType getMediaType() { return mediaType; }


    /**
     * Return the list of recommended usage for the related media.  Null means no usage guidance is available.
     *
     * @return List of RelatedMediaUsage
     */
    public ArrayList<RelatedMediaUsage> getMediaUsageList()
    {
        if (mediaUsageList != null)
        {
            return mediaUsageList;
        }
        else
        {
            return new ArrayList<RelatedMediaUsage>(mediaUsageList);
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
        return "RelatedMediaReference{" +
                "mediaId='" + mediaId + '\'' +
                ", linkDescription='" + linkDescription + '\'' +
                ", displayName='" + displayName + '\'' +
                ", uri='" + uri + '\'' +
                ", resourceDescription='" + resourceDescription + '\'' +
                ", version='" + version + '\'' +
                ", organization='" + organization + '\'' +
                ", mediaType=" + mediaType +
                ", mediaUsageList=" + mediaUsageList +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
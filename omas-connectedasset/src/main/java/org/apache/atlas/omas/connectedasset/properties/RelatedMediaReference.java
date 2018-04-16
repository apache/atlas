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
 * RelatedMediaReference stores information about an link to an external media file that
 * is relevant to this asset.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
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
    private List<RelatedMediaUsage> mediaUsageList = null;


    /**
     * Default Constructor
     */
    public RelatedMediaReference()
    {
        super();
    }


    /**
     * Copy/clone constructor.
     *
     * @param templateRelatedMediaReference - element to copy
     */
    public RelatedMediaReference(RelatedMediaReference templateRelatedMediaReference)
    {
        /*
         * Initialize the super class.
         */
        super(templateRelatedMediaReference);

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
     * Set up the reference identifier for this asset's related media.
     *
     * @param mediaId String
     */
    public void setReferenceId(String mediaId) { this.mediaId = mediaId; }


    /**
     * Return the description of the reference (with respect to this asset).
     *
     * @return String link description.
     */
    public String getLinkDescription() { return linkDescription; }


    /**
     * Set up the description of the reference (with respect to this asset).
     *
     * @param linkDescription - String
     */
    public void setLinkDescription(String linkDescription) { this.linkDescription = linkDescription; }


    /**
     * Return the display name of this media reference.
     *
     * @return String display name.
     */
    public String getDisplayName() { return displayName; }


    /**
     * Set up the display name for this media reference.
     *
     * @param displayName - String
     */
    public void setDisplayName(String displayName) { this.displayName = displayName; }


    /**
     * Return the URI used to retrieve the resource for this media reference.
     *
     * @return String URI
     */
    public String getURI() { return uri; }


    /**
     * Set up the URI used to retrieve the resource for this media reference.
     *
     * @param uri - String
     */
    public void setURI(String uri) { this.uri = uri; }


    /**
     * Return the description of this external media.
     *
     * @return String resource description
     */
    public String getResourceDescription() { return resourceDescription; }


    /**
     * Set up the description of this external media.
     *
     * @param resourceDescription String
     */
    public void setResourceDescription(String resourceDescription) { this.resourceDescription = resourceDescription; }


    /**
     * Return the version of the resource that this media reference represents.
     *
     * @return String version
     */
    public String getVersion() { return version; }


    /**
     * Set up the version of the resource that this external reference represents.
     *
     * @param version - String
     */
    public void setVersion(String version) { this.version = version; }


    /**
     * Return the name of the organization that owns the resource that this external reference represents.
     *
     * @return String organization name
     */
    public String getOrganization() { return organization; }


    /**
     * Set up the name of the organization that owns the resource that this external reference represents.
     *
     * @param organization - String
     */
    public void setOrganization(String organization) { this.organization = organization; }


    /**
     * Return the type of media referenced.
     *
     * @return RelatedMediaType
     */
    public RelatedMediaType getMediaType() { return mediaType; }


    /**
     * Set up the media type.
     *
     * @param mediaType - RelatedMediaType
     */
    public void setMediaType(RelatedMediaType mediaType) { this.mediaType = mediaType; }


    /**
     * Return the list of recommended usage for the related media.  Null means no usage guidance is available.
     *
     * @return List of RelatedMediaUsage
     */
    public List<RelatedMediaUsage> getMediaUsageList()
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
     * Set up the media usage list.
     *
     * @param mediaUsageList - List of RelatedMediaUsage
     */
    public void setMediaUsageList(List<RelatedMediaUsage> mediaUsageList)
    {
        this.mediaUsageList = new ArrayList<RelatedMediaUsage>(mediaUsageList);
    }
}
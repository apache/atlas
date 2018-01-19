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
 * ExternalReference stores information about an link to an external resource that is relevant to this asset.
 */
public class ExternalReference extends Referenceable
{
    /*
     * Attributes of an external reference
     */
    private String            referenceId = null;
    private String            linkDescription = null;
    private String            displayName = null;
    private String            uri = null;
    private String            resourceDescription = null;
    private String            version = null;
    private String            organization = null;


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
     * @param referenceId the reference identifier for this asset's reference.
     * @param linkDescription - description of the reference (with respect to this asset).
     * @param displayName - display name for this external reference.
     * @param uri - the URI used to retrieve the resource that this external reference represents.
     * @param resourceDescription - the description of the resource that this external reference represents.
     * @param version - the version of the resource that this external reference represents.
     * @param organization - the name of the organization that owns the resource that this external reference represents.
     */
    public ExternalReference(AssetDescriptor      parentAsset,
                             ElementType          type,
                             String               guid,
                             String               url,
                             Classifications      classifications,
                             String               qualifiedName,
                             AdditionalProperties additionalProperties,
                             Meanings             meanings,
                             String               referenceId,
                             String               linkDescription,
                             String               displayName,
                             String               uri,
                             String               resourceDescription,
                             String               version,
                             String               organization)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);
        this.referenceId = referenceId;
        this.linkDescription = linkDescription;
        this.displayName = displayName;
        this.uri = uri;
        this.resourceDescription = resourceDescription;
        this.version = version;
        this.organization = organization;
    }


    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateExternalReference - element to copy
     */
    public ExternalReference(AssetDescriptor parentAsset, ExternalReference templateExternalReference)
    {
        /*
         * Initialize the super class.
         */
        super(parentAsset, templateExternalReference);

        if (templateExternalReference != null)
        {
            /*
             * Copy the values from the supplied template.
             */
            referenceId = templateExternalReference.getReferenceId();
            linkDescription = templateExternalReference.getLinkDescription();
            displayName = templateExternalReference.getDisplayName();
            uri = templateExternalReference.getURI();
            resourceDescription = templateExternalReference.getResourceDescription();
            version = templateExternalReference.getVersion();
            organization = templateExternalReference.getOrganization();
        }
    }


    /**
     * Return the identifier given to this reference (with respect to this asset).
     *
     * @return String referenceId
     */
    public String getReferenceId() { return referenceId; }


    /**
     * Return the description of the reference (with respect to this asset).
     *
     * @return String link description.
     */
    public String getLinkDescription() { return linkDescription; }


    /**
     * Return the display name of this external reference.
     *
     * @return String display name.
     */
    public String getDisplayName() { return displayName; }


    /**
     * Return the URI used to retrieve the resource that this external reference represents.
     *
     * @return String URI
     */
    public String getURI() { return uri; }


    /**
     * Return the description of the resource that this external reference represents.
     *
     * @return String resource description
     */
    public String getResourceDescription() { return resourceDescription; }


    /**
     * Return the version of the resource that this external reference represents.
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
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "ExternalReference{" +
                "referenceId='" + referenceId + '\'' +
                ", linkDescription='" + linkDescription + '\'' +
                ", displayName='" + displayName + '\'' +
                ", uri='" + uri + '\'' +
                ", resourceDescription='" + resourceDescription + '\'' +
                ", version='" + version + '\'' +
                ", organization='" + organization + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
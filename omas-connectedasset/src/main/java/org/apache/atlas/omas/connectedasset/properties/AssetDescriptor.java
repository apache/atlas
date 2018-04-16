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
 * This is the base class for a connected asset.  It is passed to all of the embedded property objects so the name
 * and type can be used for error messages and other diagnostics.  It also carries the URL of the
 * metadata repository where this is known to enable properties to be retrieved on request.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class AssetDescriptor extends PropertyBase
{
    /*
     * Derived name and type for use by nested property object for messages/debug.  If these default values
     * are seen it is a sign that the asset properties are not being populated from the metadata repository.
     */
    private String assetName = "<Unknown>";
    private String assetTypeName = "<Unknown>";

    /*
     * URL where the metadata about the asset is located.  It remains null if no repository is known.
     */
    private String assetRepositoryURL = null;


    /**
     * Typical constructor - the asset descriptor is effectively empty - and the set methods need to be called to
     * add useful content to it.
     */
    public AssetDescriptor()
    {
        /*
         * Nothing to do except call superclass
         */
        super();
    }


    /**
     * Explicit constructor - the asset descriptor is explicitly given the name and type for the asset.  This
     * constructor is used to override the hard coded defaults.
     *
     * @param assetName - name of asset
     * @param assetTypeName - name of asset type
     * @param assetRepositoryURL - URL for the metadata repository
     */
    public AssetDescriptor(String assetName, String assetTypeName, String  assetRepositoryURL)
    {
        super();

        this.assetName = assetName;
        this.assetTypeName = assetTypeName;
        this.assetRepositoryURL = assetRepositoryURL;
    }


    /**
     * Copy/clone Constructor - used to copy the asset descriptor for a new consumer.
     *
     * @param templateAssetDescriptor - template asset descriptor to copy.
     */
    public AssetDescriptor(AssetDescriptor   templateAssetDescriptor)
    {
        super();

        this.assetName = templateAssetDescriptor.getAssetName();
        this.assetTypeName = templateAssetDescriptor.getAssetTypeName();
        this.assetRepositoryURL = templateAssetDescriptor.getAssetRepositoryURL();
    }


    /**
     * Method to enable a subclass to set up the asset name.
     *
     * @param assetName - String - name of asset for messages etc
     */
    protected void setAssetName(String     assetName)
    {
        this.assetName = assetName;
    }


    /**
     * Method to enable a subclass to set up the asset type name.
     *
     * @param assetTypeName - String - new type name
     */
    protected void setAssetTypeName(String    assetTypeName)
    {
        this.assetTypeName = assetTypeName;
    }


    /**
     * Set up the URL of the metadata repository where the asset properties are held.
     *
     * @param assetRepositoryURL - String - URL
     */
    protected void setAssetRepositoryURL(String assetRepositoryURL) { this.assetRepositoryURL = assetRepositoryURL; }


    /**
     * Return the name of the asset - for use in messages and other diagnostics.
     *
     * @return String - asset name
     */
    public String getAssetName()
    {
        return assetName;
    }


    /**
     * Return the name of the asset's type - for use in messages and other diagnostics.
     *
     * @return String - asset type name
     */
    public String getAssetTypeName()
    {
        return assetTypeName;
    }


    /**
     * Return the URL of the metadata repository where the asset properties are held.
     *
     * @return String - URL
     */
    public String getAssetRepositoryURL() { return assetRepositoryURL; }
}
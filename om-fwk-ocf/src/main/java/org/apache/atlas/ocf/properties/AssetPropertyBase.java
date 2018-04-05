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
 * The AssetPropertyBase class is a base class for all properties that link off of the connected asset.
 * It manages the information about the parent asset.
 */
public abstract class AssetPropertyBase extends PropertyBase
{
    private AssetDescriptor     parentAsset = null;

    /**
     * Typical constructor that sets the link to the connected asset to null
     *
     * @param parentAsset - descriptor of asset that his property relates to.
     */
    protected AssetPropertyBase(AssetDescriptor     parentAsset)
    {
        /*
         * Initialize superclass and save the parent asset.
         */
        super();
        this.parentAsset = parentAsset;
    }


    /**
     * Copy/clone constructor - sets up details of the parent asset from the template
     *
     * @param parentAsset - descriptor of asset that his property relates to.
     * @param  template - AssetPropertyBase to copy
     */
    protected AssetPropertyBase(AssetDescriptor     parentAsset, AssetPropertyBase  template)
    {
        /*
         * Initialize superclass and save the parentAsset
         */
        super(template);
        this.parentAsset = parentAsset;
    }


    /**
     * Return the asset descriptor of the parent asset.
     *
     * @return AssetDescriptor
     */
    protected  AssetDescriptor  getParentAsset() { return parentAsset; }


    /**
     * Return the name of the connected asset that this property is connected to.
     *
     * @return String name of the connected asset
     */
    protected String getParentAssetName()
    {
        String  parentAssetName = "<Unknown>";

        if (parentAsset != null)
        {
            parentAssetName = parentAsset.getAssetName();
        }

        return parentAssetName;
    }


    /**
     * Return the type of the connected asset that this property relates to.
     *
     * @return String name of the connected asset's type.
     */
    protected String getParentAssetTypeName()
    {
        String  parentAssetTypeName = "<Unknown>";

        if (parentAsset != null)
        {
            parentAssetTypeName = parentAsset.getAssetTypeName();
        }

        return parentAssetTypeName;
    }


    /**
     * An equals() method for subclasses to check they are connected to the same parent asset.
     *
     * @param testObject - object to test
     * @return boolean indicating whether this object is connected to equivalent parent assets.
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

        AssetPropertyBase that = (AssetPropertyBase) testObject;

        return parentAsset != null ? parentAsset.equals(that.parentAsset) : that.parentAsset == null;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "AssetPropertyBase{" +
                "parentAsset=" + parentAsset +
                '}';
    }
}
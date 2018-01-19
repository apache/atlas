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

import java.util.ArrayList;

public abstract class AssetPropertyIteratorBase extends AssetPropertyBase
{
    protected PagingIterator pagingIterator = null;

    /**
     * Typical Constructor creates an iterator with the supplied list of comments.
     *
     * @param parentAsset - descriptor of parent asset
     * @param totalElementCount - the total number of elements to process.  A negative value is converted to 0.
     * @param maxCacheSize - maximum number of elements that should be retrieved from the property server and
     *                     cached in the element list at any one time.  If a number less than one is supplied, 1 is used.
     */
    protected AssetPropertyIteratorBase(AssetDescriptor              parentAsset,
                                        int                          totalElementCount,
                                        int                          maxCacheSize)
    {
        /*
         * Initialize superclass.
         */
        super(parentAsset);

        pagingIterator = new PagingIterator(parentAsset, this, totalElementCount, maxCacheSize);
    }


    /**
     * Copy/clone constructor - sets up details of the parent asset from the template
     *
     * @param parentAsset - descriptor of asset that his property relates to.
     * @param  template - AssetPropertyBaseImpl to copy
     */
    protected AssetPropertyIteratorBase(AssetDescriptor     parentAsset, AssetPropertyIteratorBase  template)
    {
        /*
         * Initialize superclass.
         */
        super(parentAsset, template);

        if (template != null)
        {
            pagingIterator = new PagingIterator(parentAsset, this, template.pagingIterator);
        }
    }


    /**
     * Return the number of elements in the list.
     *
     * @return elementCount
     */
    public int getElementCount()
    {
        if (pagingIterator == null)
        {
            return 0;
        }
        else
        {
            return pagingIterator.getElementCount();
        }
    }


    /**
     * Method implemented by a subclass that ensures the cloning process is a deep clone.
     *
     * @param parentAsset - descriptor of parent asset
     * @param template - object to clone
     * @return new cloned object.
     */
    protected abstract AssetPropertyBase  cloneElement(AssetDescriptor  parentAsset, AssetPropertyBase   template);


    /**
     * Method implemented by subclass to retrieve the next cached list of elements.
     *
     * @param cacheStartPointer - where to start the cache.
     * @param maximumSize - maximum number of elements in the cache.
     * @return list of elements corresponding to the supplied cache pointers.
     * @throws PropertyServerException - there is a problem retrieving elements from the property (metadata) server.
     */
    protected abstract ArrayList<AssetPropertyBase> getCachedList(int  cacheStartPointer,
                                                                  int  maximumSize) throws PropertyServerException;
}

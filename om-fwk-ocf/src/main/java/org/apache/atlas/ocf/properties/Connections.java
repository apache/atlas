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

import java.util.Iterator;

/**
 * Connections supports an iterator over a list of connections.  Callers can use it to step through the list
 * just once.  If they want to parse the list again, they could use the copy/clone constructor to create
 * a new iterator.
 */
public abstract class Connections extends AssetPropertyIteratorBase implements Iterator<Connection>
{
    /**
     * Typical Constructor creates an iterator with the supplied list of elements.
     *
     * @param parentAsset - descriptor of parent asset
     * @param totalElementCount - the total number of elements to process.  A negative value is converted to 0.
     * @param maxCacheSize - maximum number of elements that should be retrieved from the property server and
     *                     cached in the element list at any one time.  If a number less than one is supplied, 1 is used.
     */
    public Connections(AssetDescriptor              parentAsset,
                       int                          totalElementCount,
                       int                          maxCacheSize)
    {
        super(parentAsset, totalElementCount, maxCacheSize);
    }


    /**
     * Copy/clone constructor.  Used to reset iterator element pointer to 0;
     *
     * @param parentAsset - descriptor of parent asset
     * @param template - type-specific iterator to copy; null to create an empty iterator
     */
    public Connections(AssetDescriptor   parentAsset, Connections    template)
    {
        super(parentAsset, template);
    }


    /**
     * Provides a concrete implementation of cloneElement for the specific iterator type.
     *
     * @param parentAsset - descriptor of parent asset
     * @param template - object to clone
     * @return new cloned object.
     */
    protected  AssetPropertyBase  cloneElement(AssetDescriptor  parentAsset, AssetPropertyBase   template)
    {
        return new Connection(parentAsset, (Connection)template);
    }


    /**
     * Clones this iterator.
     *
     * @param parentAsset - descriptor of parent asset
     * @return new cloned object.
     */
    protected  abstract Connections  cloneIterator(AssetDescriptor  parentAsset);


    /**
     * The iterator can only be used once to step through the elements.  This method returns
     * a boolean to indicate if it has got to the end of the list yet.
     *
     * @return boolean indicating whether there are more elements.
     */
    @Override
    public boolean hasNext()
    {
        return super.pagingIterator.hasNext();
    }


    /**
     * Return the next element in the iteration.
     *
     * @return Connection - next element object that has been cloned.
     */
    @Override
    public Connection next()
    {
        return (Connection)super.pagingIterator.next();
    }


    /**
     * Remove the current element in the iterator. (Null implementation since this iterator works off of cached
     * elements from the property (metadata) server.)
     */
    @Override
    public void remove()
    {
        super.pagingIterator.remove();
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "Connections{" +
                "pagingIterator=" + pagingIterator +
                '}';
    }
}
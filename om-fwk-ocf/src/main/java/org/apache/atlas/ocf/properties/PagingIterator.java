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

import org.apache.atlas.ocf.ffdc.OCFErrorCode;
import org.apache.atlas.ocf.ffdc.OCFRuntimeException;
import org.apache.atlas.ocf.ffdc.PropertyServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * PagingIterator supports an iterator over a list of objects that extend AssetPropertyBase.
 * Callers can use it to step through the list just once.  If they want to parse the list again,
 * they could use the copy/clone constructor to create a new iterator.
 *
 * PagingIterator provides paging support to enable the list in the iterator to be much bigger than
 * can be stored in memory.  It maintains pointers for the full list, and a potentially smaller
 * cached list that is used to batch up elements being retrieved from the property (metadata) server.
 * Thus is combines the ability to process very large lists whilst minimising the chatter with the property server.
 *
 * The class uses a number of pointers and counts.  Imagine a list of 4000 columns in a table.
 * We can not retrieve all 4000 columns on one call. So we retrieve subsets over a number of REST calls as the
 * caller walks through the full list.
 * <ul>
 *     <li>
 *         totalElement is 4000 - ie how many elements there are in total.
 *     </li>
 *     <li>
 *         maxCacheSize is the maximum elements we can retrieve in one call
 *     </li>
 *     <li>
 *         cachedElementList.size() is how many elements we have in memory at this time.
 *     </li>
 * </ul>
 *
 * cachedElementList.size() is set to maxCacheSize() for all retrieves except for processing the last cache of elements.
 * For example, if the totalElement is 25 and the maxCacheSize() is 10 then we would retrieve 3 caches -
 * the first two would have 10 elements in them and the third will have 5 elements.
 * In the first 2 retrieves, maxCacheSize and cachedElementList.size() are set to 10.
 * In the last one, maxCacheSize==10 and cachedElementList.size()==5.
 */
public class PagingIterator extends AssetPropertyBase implements Iterator<AssetPropertyBase>
{
    private int                          maxCacheSize         = 1;

    private int                          totalElementCount    = 0;
    private int                          cachedElementStart   = 0;
    private ArrayList<AssetPropertyBase> cachedElementList    = new ArrayList<>();
    private int                          cachedElementPointer = 0;

    private AssetPropertyIteratorBase    iterator             = null;

    private static final Logger log = LoggerFactory.getLogger(PagingIterator.class);



    /**
     * Typical Constructor creates an iterator with the supplied list of comments.
     *
     * @param parentAsset - descriptor of parent asset.
     * @param iterator - type-specific iterator that wraps this paging iterator.
     * @param totalElementCount - the total number of elements to process.  A negative value is converted to 0.
     * @param maxCacheSize - maximum number of elements that should be retrieved from the property server and
     *                     cached in the element list at any one time.  If a number less than one is supplied, 1 is used.
     */
    public PagingIterator(AssetDescriptor              parentAsset,
                          AssetPropertyIteratorBase    iterator,
                          int                          totalElementCount,
                          int                          maxCacheSize)
    {
        super(parentAsset);

        if (log.isDebugEnabled())
        {
            log.debug("New PagingIterator:");
            log.debug("==> totalElementCount: " + totalElementCount);
            log.debug("==> maxCacheSize: " + maxCacheSize);
        }


        if (totalElementCount > 0)
        {
            this.totalElementCount = totalElementCount;
        }

        if (maxCacheSize > 0)
        {
            this.maxCacheSize = maxCacheSize;
        }

        if (iterator != null)
        {
            this.iterator = iterator;
        }
        else
        {
            /*
             * Throw runtime exception to show the caller they are not using the list correctly.
             */
            OCFErrorCode errorCode = OCFErrorCode.NO_ITERATOR;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(this.getClass().getSimpleName(),
                                                                            super.getParentAssetName(),
                                                                            super.getParentAssetTypeName());

            throw new OCFRuntimeException(errorCode.getHTTPErrorCode(),
                                          this.getClass().getName(),
                                          "next",
                                          errorMessage,
                                          errorCode.getSystemAction(),
                                          errorCode.getUserAction());
        }
    }


    /**
     * Copy/clone constructor.  Used to reset iterator element pointer to 0;
     *
     * @param parentAsset - descriptor of parent asset
     * @param iterator - type-specific iterator that wraps this paging iterator.
     * @param templateIterator - template to copy; null to create an empty iterator
     */
    public PagingIterator(AssetDescriptor           parentAsset,
                          AssetPropertyIteratorBase iterator,
                          PagingIterator            templateIterator)
    {
        super(parentAsset, templateIterator);

        if (templateIterator != null)
        {
            this.cachedElementStart = 0;
            this.cachedElementPointer = 0;
            this.cachedElementList = new ArrayList<>();

            if (templateIterator.totalElementCount > 0)
            {
                this.totalElementCount = templateIterator.totalElementCount;
            }

            if (templateIterator.maxCacheSize > 0)
            {
                this.maxCacheSize = templateIterator.maxCacheSize;
            }

            if (iterator != null)
            {
                this.iterator = iterator;
            }
            else
            {
            /*
             * Throw runtime exception to show the caller they are not using the list correctly.
             */
                OCFErrorCode errorCode = OCFErrorCode.NO_ITERATOR;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(this.getClass().getSimpleName(),
                                                                                super.getParentAssetName(),
                                                                                super.getParentAssetTypeName());

                throw new OCFRuntimeException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              "next",
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
            }

            if (templateIterator.cachedElementStart == 0)
            {
                /*
                 * The template's cache starts at the beginning of the total list so ok to copy it.
                 */
                for (AssetPropertyBase templateElement : templateIterator.cachedElementList)
                {
                    this.cachedElementList.add(iterator.cloneElement(parentAsset, templateElement));
                }
            }
        }
    }


    /**
     * The iterator can only be used once to step through the elements.  This method returns
     * a boolean to indicate if it has got to the end of the list yet.
     *
     * @return boolean indicating whether there are more elements.
     */
    @Override
    public boolean hasNext()
    {
        return (cachedElementStart < totalElementCount);
    }


    /**
     * Return the next element in the list
     *
     * @return AssetPropertyBase - next element.
     * @throws OCFRuntimeException - if there are no more elements in the list or there are problems retrieving
     *                             elements from the property (metadata) server.
     */
    @Override
    public AssetPropertyBase next()
    {
        /*
         * Check more elements available
         */
        if (this.hasNext())
        {
            AssetPropertyBase retrievedElement = null;

            /*
             * If the pointer is at the end of the cache then retrieve more content from the property (metadata)
             * server.
             */
            if (cachedElementPointer == cachedElementList.size())
            {
                try
                {
                    cachedElementList = iterator.getCachedList(cachedElementStart, maxCacheSize);
                    cachedElementPointer = 0;
                }
                catch (PropertyServerException   error)
                {
                    /*
                     * Problem retrieving next cache.  The exception includes a detailed error message,
                     */
                    OCFErrorCode errorCode = OCFErrorCode.PROPERTIES_NOT_AVAILABLE;
                    String        errorMessage = errorCode.getErrorMessageId()
                                               + errorCode.getFormattedErrorMessage(error.getErrorMessage(),
                                                                                    this.toString());

                    throw new OCFRuntimeException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  "next",
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction(),
                                                  error);
                }
            }

            retrievedElement = iterator.cloneElement(getParentAsset(), cachedElementList.get(cachedElementPointer));
            cachedElementPointer++;
            cachedElementStart++;

            if (log.isDebugEnabled())
            {
                log.debug("Returning next element:");
                log.debug("==> totalElementCount: " + totalElementCount);
                log.debug("==> cachedElementPointer: " + cachedElementPointer);
                log.debug("==> cachedElementStart:" + cachedElementStart);
                log.debug("==> maxCacheSize:" + maxCacheSize);
            }


            return retrievedElement;
        }
        else
        {
            /*
             * Throw runtime exception to show the caller they are not using the list correctly.
             */
            OCFErrorCode errorCode = OCFErrorCode.NO_MORE_ELEMENTS;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(this.getClass().getSimpleName(),
                                                                            super.getParentAssetName(),
                                                                            super.getParentAssetTypeName());

            throw new OCFRuntimeException(errorCode.getHTTPErrorCode(),
                                          this.getClass().getName(),
                                          "next",
                                          errorMessage,
                                          errorCode.getSystemAction(),
                                          errorCode.getUserAction());
        }
    }


    /**
     * Return the number of elements in the list.
     *
     * @return elementCount
     */
    public int getElementCount()
    {
        return totalElementCount;
    }


    /**
     * Remove the current element in the iterator.
     */
    @Override
    public void remove()
    {
        /*
         * Nothing to do since the removed element can never be revisited through this instance.
         */
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "PagingIterator{" +
                "maxCacheSize=" + maxCacheSize +
                ", totalElementCount=" + totalElementCount +
                ", cachedElementStart=" + cachedElementStart +
                ", cachedElementList=" + cachedElementList +
                ", cachedElementPointer=" + cachedElementPointer +
                '}';
    }
}
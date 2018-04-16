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
package org.apache.atlas.omrs.rest.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * OMRSRESTAPIPagedResponse provides the base definition for a paged response.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class OMRSRESTAPIPagedResponse extends OMRSRESTAPIResponse
{
    protected String  nextPageURL = null;
    protected int     offset      = 0;
    protected int     pageSize    = 0;


    /**
     * Default constructor
     */
    public OMRSRESTAPIPagedResponse()
    {
    }


    /**
     * Return the url that can be used to retrieve the next page.
     *
     * @return url string
     */
    public String getNextPageURL()
    {
        return nextPageURL;
    }


    /**
     * Set up the url that can be used to retrieve the next page.
     *
     * @param nextPageURL - url string
     */
    public void setNextPageURL(String nextPageURL)
    {
        this.nextPageURL = nextPageURL;
    }


    /**
     * Return the starting element number for this set of results.  This is used when retrieving elements
     * beyond the first page of results. Zero means the results start from the first element.
     *
     * @return - offset number
     */
    public int getOffset()
    {
        return offset;
    }


    /**
     * Set up the starting element number for this set of results.  This is used when retrieving elements
     * beyond the first page of results. Zero means the results start from the first element.
     *
     * @param offset - offset number
     */
    public void setOffset(int offset)
    {
        this.offset = offset;
    }


    /**
     * Return the maximum number of elements that can be returned on this request.
     *
     * @return page size
     */
    public int getPageSize()
    {
        return pageSize;
    }


    /**
     * Set up the maximum number of elements that can be returned on this request.
     *
     * @param pageSize - integer number
     */
    public void setPageSize(int pageSize)
    {
        this.pageSize = pageSize;
    }


    @Override
    public String toString()
    {
        return "OMRSRESTAPIPagedResponse{" +
                "nextPageURL='" + nextPageURL + '\'' +
                ", offset=" + offset +
                ", pageSize=" + pageSize +
                ", relatedHTTPCode=" + relatedHTTPCode +
                ", exceptionClassName='" + exceptionClassName + '\'' +
                ", exceptionErrorMessage='" + exceptionErrorMessage + '\'' +
                ", exceptionSystemAction='" + exceptionSystemAction + '\'' +
                ", exceptionUserAction='" + exceptionUserAction + '\'' +
                '}';
    }
}

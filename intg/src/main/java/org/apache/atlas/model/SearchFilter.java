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
package org.apache.atlas.model;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Generic filter, to specify search criteria using name/value pairs.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class SearchFilter {
    public static final String PARAM_TYPE = "type";
    public static final String PARAM_SUPERTYPE = "supertype";
    public static final String PARAM_NOT_SUPERTYPE = "notsupertype";

    /**
     * to specify whether the result should be sorted? If yes, whether asc or desc.
     */
    public enum SortType { NONE, ASC, DESC };

    private Map<String, String> params     = null;
    private long                startIndex = 0;
    private long                maxRows    = Long.MAX_VALUE;
    private boolean             getCount   = true;
    private String              sortBy     = null;
    private SortType            sortType   = null;

    public SearchFilter() {
        setParams(null);
    }

    public SearchFilter(Map<String, String> params) {
        setParams(params);
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public String getParam(String name) {
        String ret = null;

        if (name != null && params != null) {
            ret = params.get(name);
        }

        return ret;
    }

    public void setParam(String name, String value) {
        if (name != null) {
            if (params == null) {
                params = new HashMap<String, String>();
            }

            params.put(name, value);
        }
    }

    public long getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(long startIndex) {
        this.startIndex = startIndex;
    }

    public long getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(long maxRows) {
        this.maxRows = maxRows;
    }

    public boolean isGetCount() {
        return getCount;
    }

    public void setGetCount(boolean getCount) {
        this.getCount = getCount;
    }

    public String getSortBy() {
        return sortBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public SortType getSortType() {
        return sortType;
    }

    public void setSortType(SortType sortType) {
        this.sortType = sortType;
    }
}

/**
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

package org.apache.atlas.catalog;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Base user API request.
 */
public abstract class BaseRequest implements Request {
    private final Map<String, Object> queryProperties = new HashMap<>();
    private final Map<String, Object> updateProperties = new HashMap<>();
    private final String queryString;
    private final Collection<String> additionalSelectProperties = new HashSet<>();

    protected BaseRequest(Map<String, Object> queryProperties, String queryString) {
        this(queryProperties, queryString, null);
    }

    protected BaseRequest(Map<String, Object> queryProperties, String queryString, Map<String, Object> updateProperties) {
        if (queryProperties != null) {
            this.queryProperties.putAll(queryProperties);
        }

        if (updateProperties != null) {
            this.updateProperties.putAll(updateProperties);
        }

        this.queryString = queryString;
    }


    public Map<String, Object> getQueryProperties() {
        return queryProperties;
    }

    public Map<String, Object> getUpdateProperties() {
        return updateProperties;
    }

    public <T> T getProperty(String name) {
        return (T) queryProperties.get(name);
    }

    public String getQueryString() {
        return queryString;
    }

    @Override
    public void addAdditionalSelectProperties(Collection<String> resultProperties) {
        additionalSelectProperties.addAll(resultProperties);
    }

    @Override
    public Collection<String> getAdditionalSelectProperties() {
        return additionalSelectProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BaseRequest that = (BaseRequest) o;

        return queryProperties.equals(that.queryProperties) &&
               updateProperties.equals(that.updateProperties) &&
               additionalSelectProperties.equals(that.additionalSelectProperties) &&
               queryString == null ? that.queryString == null : queryString.equals(that.queryString);
    }

    @Override
    public int hashCode() {
        int result = queryProperties.hashCode();
        result = 31 * result + updateProperties.hashCode();
        result = 31 * result + (queryString != null ? queryString.hashCode() : 0);
        result = 31 * result + additionalSelectProperties.hashCode();
        return result;
    }
}

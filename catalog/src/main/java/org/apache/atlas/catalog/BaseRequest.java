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
    private final Map<String, Object> properties = new HashMap<>();
    private final String queryString;
    private final Collection<String> additionalSelectProperties = new HashSet<>();

    protected BaseRequest(Map<String, Object> properties, String queryString) {
        if (properties != null) {
            this.properties.putAll((properties));
        }
        this.queryString = queryString;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public <T> T getProperty(String name) {
        return (T)properties.get(name);
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
}

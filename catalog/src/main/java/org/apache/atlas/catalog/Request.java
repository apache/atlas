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
import java.util.Map;

/**
 * Represents a user request.
 */
public interface Request {
    /**
     * Request cardinality enum.
     */
    enum Cardinality {INSTANCE, COLLECTION}

    /**
     * Get request properties.
     *
     * @return request property map
     */
    Map<String, Object> getProperties();

    /**
     * Get the value of a specified property.
     *
     * @param name  property name
     * @param <T>   value type
     *
     * @return value for the requested property or null if property not in map
     */
    <T> T getProperty(String name);

    /**
     * Get the query string.
     *
     * @return the user specified query string or null
     */
    String getQueryString();

    /**
     * Get the cardinality of the request.
     *
     * @return the request cardinality
     */
    Cardinality getCardinality();

    /**
     * Add additional property names which should be returned in the result.
     *
     * @param resultProperties  collection of property names
     */
    void addAdditionalSelectProperties(Collection<String> resultProperties);

    /**
     * Get any additional property names which should be included in the result.
     *
     * @return collection of added property names or an empty collection
     */
    Collection<String> getAdditionalSelectProperties();
}

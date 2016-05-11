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

package org.apache.atlas.repository.graphdb;

import java.util.Collection;
import java.util.Set;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Represents a graph element. 
 *
 */
public interface AtlasElement {

    /**
     * Gets the id of this element
     * @return
     */
    Object getId();

    /**
     * Gets the names of the properties associated with this element.
     * @return
     */
    Collection<? extends String> getPropertyKeys();

    /**
     * Gets the value of the element property with the given name
     * 
     * @param propertyName
     * @return
     */
    <T> T getProperty(String propertyName);
    
    /**
     * Removes a property from the vertex.
     */
    void removeProperty(String propertyName);

    /**
     * Sets a single-valued property to the given value.
     * 
     * @param propertyName
     * @param value
     */
    <T> void setProperty(String propertyName, T value);     
    
    
    /**
     * Creates a Jettison JSONObject from this Element
     *
     * @param propertyKeys The property keys at the root of the element to serialize.  If null, then all keys are serialized.
     */
    JSONObject toJson(Set<String> propertyKeys) throws JSONException;   
    
}

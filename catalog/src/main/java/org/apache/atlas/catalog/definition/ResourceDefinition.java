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

package org.apache.atlas.catalog.definition;

import org.apache.atlas.catalog.PropertyMapper;
import org.apache.atlas.catalog.PropertyValueFormatter;
import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.exception.InvalidPayloadException;
import org.apache.atlas.catalog.projection.Projection;
import org.apache.atlas.catalog.projection.Relation;
import org.apache.atlas.typesystem.types.AttributeDefinition;

import java.util.Collection;
import java.util.Map;

/**
 * Resource definition.
 */
public interface ResourceDefinition {
    /**
     * The type name of the resource.
     *
     * @return the resources type name
     */
    String getTypeName();
    /**
     * Validate a user request.
     *
     * @param request  user request
     *
     * @throws InvalidPayloadException if the request payload is invalid in any way
     */
    void validate(Request request) throws InvalidPayloadException;

    /**
     * Get the name of the resources id property.
     *
     * @return the id property name
     */
    String getIdPropertyName();

    /**
     * Get the property definitions for the resource.
     *
     * @return resource property definitions
     */
    //todo: abstract usage of AttributeDefinition
    Collection<AttributeDefinition> getPropertyDefinitions();

    /**
     * Filter out properties which shouldn't be returned in the result.
     * The passed in map is directly modified as well as returned.
     *
     * @param request      user request
     * @param propertyMap  property map to filter
     *
     * @return the filtered property map
     */
    Map<String, Object> filterProperties(Request request, Map<String, Object> propertyMap);

    /**
     * Generate an href for the resource from the provided resource property map.
     *
     * @param properties  resource property map
     *
     * @return a URL to be used as an href property value for the resource
     */
    String resolveHref(Map<String, Object> properties);

    /**
     * Get map of resource projections.
     *
     * @return map of resource projections
     */
    Map<String, Projection> getProjections();

    /**
     * Get map of resource relations.
     *
     * @return map of resource relations
     */
    Map<String, Relation> getRelations();

    /**
     * Get the property mapper associated with the resource.
     *
     * @return associated property mapper
     */
    PropertyMapper getPropertyMapper();

    /**
     * Get the registered property value formatters.
     * @return map of property name to property value formatter
     */
    Map<String, PropertyValueFormatter> getPropertyValueFormatters();
}

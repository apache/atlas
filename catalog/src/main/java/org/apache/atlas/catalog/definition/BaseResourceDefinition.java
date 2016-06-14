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

import org.apache.atlas.AtlasException;
import org.apache.atlas.catalog.*;
import org.apache.atlas.catalog.exception.CatalogRuntimeException;
import org.apache.atlas.catalog.exception.InvalidPayloadException;
import org.apache.atlas.catalog.projection.Projection;
import org.apache.atlas.catalog.projection.Relation;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;

import java.util.*;

/**
 * Base class for resource definitions.
 */
public abstract class BaseResourceDefinition implements ResourceDefinition {
    protected static final TypeSystem typeSystem = TypeSystem.getInstance();

    protected final Set<String> instanceProperties = new HashSet<>();
    protected final Set<String> collectionProperties = new HashSet<>();
    protected Map<String, AttributeDefinition> propertyDefs = new HashMap<>();
    protected Map<String, AttributeInfo> properties = new HashMap<>();

    protected final Map<String, Projection> projections = new HashMap<>();
    protected final Map<String, Relation> relations = new HashMap<>();

    protected final PropertyMapper propertyMapper;
    protected final Map<String, PropertyValueFormatter> propertyValueFormatters = new HashMap<>();


    public BaseResourceDefinition() {
        DefaultDateFormatter defaultDateFormatter = new DefaultDateFormatter();
        registerPropertyValueFormatter("creation_time", defaultDateFormatter);
        registerPropertyValueFormatter("modified_time", defaultDateFormatter);

        this.propertyMapper = createPropertyMapper();
    }

    @Override
    public void validateCreatePayload(Request request) throws InvalidPayloadException {
        Collection<String> propKeys = new HashSet<>(request.getQueryProperties().keySet());
        Collection<String> missingProperties = new HashSet<>();
        for (AttributeInfo property : properties.values()) {
            String name = property.name;
            if (property.multiplicity == Multiplicity.REQUIRED) {
                if (request.getProperty(name) == null) {
                    missingProperties.add(name);
                }
            }
            propKeys.remove(name);
        }
        if (! missingProperties.isEmpty() || ! propKeys.isEmpty()) {
            throw new InvalidPayloadException(missingProperties, propKeys);
        }
        //todo: property type validation
    }

    @Override
    public void validateUpdatePayload(Request request) throws InvalidPayloadException {
        Collection<String> updateKeys = new HashSet<>(request.getUpdateProperties().keySet());
        Collection<String> validProperties = new HashSet<>(properties.keySet());
        // currently updating 'name' property for any resource is unsupported
        validProperties.remove("name");
        updateKeys.removeAll(validProperties);

        if (! updateKeys.isEmpty()) {
            throw new InvalidPayloadException(Collections.<String>emptySet(), updateKeys);
        }
    }

    @Override
    public Collection<AttributeDefinition> getPropertyDefinitions() {
        return propertyDefs.values();
    }

    @Override
    public  Map<String, Object> filterProperties(Request request, Map<String, Object> propertyMap) {
        Request.Cardinality cardinality = request.getCardinality();
        Collection<String> requestProperties = request.getAdditionalSelectProperties();
        Iterator<Map.Entry<String, Object>> propIter = propertyMap.entrySet().iterator();
        while(propIter.hasNext()) {
            Map.Entry<String, Object> propEntry = propIter.next();
            String prop = propEntry.getKey();
            if (! requestProperties.contains(prop)) {
                if (cardinality == Request.Cardinality.COLLECTION) {
                    if (! collectionProperties.contains(prop)) {
                        propIter.remove();
                    }
                } else {
                    if (! instanceProperties.isEmpty() && ! instanceProperties.contains(prop)) {
                        propIter.remove();
                    }
                }
            }
        }
        return propertyMap;
    }

    @Override
    public Map<String, Projection> getProjections() {
        return projections;
    }

    @Override
    public Map<String, Relation> getRelations() {
        return relations;
    }


    @Override
    public synchronized PropertyMapper getPropertyMapper() {
        return propertyMapper;
    }

    @Override
    public Map<String, PropertyValueFormatter> getPropertyValueFormatters() {
        return propertyValueFormatters;
    }

    protected void registerProperty(AttributeDefinition propertyDefinition) {
        try {
            propertyDefs.put(propertyDefinition.name, propertyDefinition);
            properties.put(propertyDefinition.name, new AttributeInfo(typeSystem, propertyDefinition, null));
        } catch (AtlasException e) {
            throw new CatalogRuntimeException("Unable to create attribute: " + propertyDefinition.name, e);
        }
    }

    protected void registerPropertyValueFormatter(String property, PropertyValueFormatter valueFormatter) {
        propertyValueFormatters.put(property, valueFormatter);
    }

    /**
     * Create a new property mapper instance.
     * Should be overridden in children where the default implementation isn't sufficient.
     *
     * @return a new property mapper instance
     */
    protected PropertyMapper createPropertyMapper() {
        return new DefaultPropertyMapper();
    }
}

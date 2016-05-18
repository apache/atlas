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

import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.repository.Constants;

import java.util.*;

/**
 * Wrapper for vertices which provides additional information.
 */
public class VertexWrapper {
    private final Vertex vertex;
    private final String vertexType;
    private final Set<String> removedProperties = new HashSet<>();
    private final PropertyMapper propertyMapper;
    private final Map<String, PropertyValueFormatter> propertyValueFormatters;
    protected ResourceComparator resourceComparator = new ResourceComparator();

    public VertexWrapper(Vertex v, ResourceDefinition resourceDefinition) {
        this(v, resourceDefinition.getPropertyMapper(), resourceDefinition.getPropertyValueFormatters());
    }

    public VertexWrapper(Vertex v,
                         PropertyMapper mapper,
                         Map<String, PropertyValueFormatter> formatters) {
        vertex = v;
        vertexType = getVertexType(v);
        propertyMapper = mapper;
        propertyValueFormatters = formatters;
    }

    public Vertex getVertex() {
        return vertex;
    }

    public <T> T getProperty(String name) {
        T val;
        if (removedProperties.contains(name)) {
            val = null;
        } else {
            val = vertex.getProperty(propertyMapper.toFullyQualifiedName(name, vertexType));
            if (propertyValueFormatters.containsKey(name)) {
                //todo: fix typing of property mapper
                val = (T) propertyValueFormatters.get(name).format(val);
            }
        }
        return val;
    }

    public Collection<String> getPropertyKeys() {
        Collection<String> propertyKeys = new TreeSet<>(resourceComparator);

        for (String p : vertex.getPropertyKeys()) {
            String cleanName = propertyMapper.toCleanName(p, vertexType);
            if (! removedProperties.contains(cleanName)) {
                propertyKeys.add(cleanName);
            }
        }
        return propertyKeys;
    }

    public Map<String, Object> getPropertyMap() {
        Map<String, Object> props = new TreeMap<>(resourceComparator);
        for (String p : vertex.getPropertyKeys()) {
            String cleanName = propertyMapper.toCleanName(p, vertexType);
            if (! removedProperties.contains(cleanName)) {
                Object val = vertex.getProperty(p);
                if (propertyValueFormatters.containsKey(cleanName)) {
                    val = propertyValueFormatters.get(cleanName).format(val);
                }
                props.put(cleanName, val);
            }
        }
        return props;
    }

    public void removeProperty(String name) {
        removedProperties.add(name);
    }

    public boolean isPropertyRemoved(String name) {
        return removedProperties.contains(name);
    }

    public String toString() {
        return String.format("VertexWrapper[name=%s]", getProperty("name"));
    }

    private String getVertexType(Vertex v) {
        return v.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
    }
}

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
package org.apache.atlas.repository.graphdb.janus;

import java.util.*;



import org.apache.atlas.RequestContext;

import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.graphson.AtlasGraphSONMode;
import org.apache.atlas.repository.graphdb.janus.graphson.AtlasGraphSONUtility;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.JanusGraphElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.Constants.LEAN_GRAPH_ENABLED;
import static org.apache.atlas.type.Constants.GUID_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.INTERNAL_PROPERTY_KEY_PREFIX;

/**
 * Janus implementation of AtlasElement.
 *
 * @param <T> the implementation class of the wrapped Janus element
 * that is stored.
 */
public class AtlasJanusElement<T extends Element> implements AtlasElement {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusElement.class);

    private T element;
    protected AtlasJanusGraph graph;
    protected final static Set<String> VERTEX_CORE_PROPERTIES = new HashSet<>();

    //excludeProperties: Getting key related issue while Migration mode when fetching few attributes from graph
    //This is dirty fix to ignore getting such attributes value from graph & return null explicitly
    private static final Set<String> excludeProperties = new HashSet<>();
    static {
        excludeProperties.add("replicatedTo");
        excludeProperties.add("replicatedFrom");

        VERTEX_CORE_PROPERTIES.add("__guid");
        VERTEX_CORE_PROPERTIES.add("__state");
        VERTEX_CORE_PROPERTIES.add("__typeName");
        VERTEX_CORE_PROPERTIES.add("qualifiedName");
        VERTEX_CORE_PROPERTIES.add("__u_qualifiedName");
    }

    public AtlasJanusElement(AtlasJanusGraph graph, T element) {
        this.element = element;
        this.graph = graph;
    }

    @Override
    public <T> T getProperty(String propertyName, Class<T> clazz) {
        if (excludeProperties.contains(propertyName)) {
            return null;
        }

        if (LEAN_GRAPH_ENABLED && isAssetVertex()) {
            AtlasJanusVertex vertex = (AtlasJanusVertex) this;

            if (vertex.getDynamicVertex().hasProperties() && vertex.getDynamicVertex().hasProperty(propertyName)) {
                return (T) vertex.getDynamicVertex().getProperty(propertyName, clazz);
            } else {
                return null;
            }
        }

        //add explicit logic to return null if the property does not exist
        //This is the behavior Atlas expects.  Janus throws an exception
        //in this scenario.

        Property p = getWrappedElement().property(propertyName);
        if (p.isPresent()) {
            Object propertyValue= p.value();
            if (propertyValue == null) {
                return null;
            }
            if (AtlasEdge.class == clazz) {
                return (T) graph.getEdge(propertyValue.toString());
            }
            if (AtlasVertex.class == clazz) {
                return (T) graph.getVertex(propertyValue.toString());
            }
            return (T) propertyValue;

        }
        return null;
    }

    /**
     * Gets all of the values of the given property.
     * @param propertyName
     * @return
     */
    @Override
    public <T> Collection<T> getPropertyValues(String propertyName, Class<T> type) {
        return Collections.singleton(getProperty(propertyName, type));
    }

    @Override
    public Set<String> getPropertyKeys() {
        if (LEAN_GRAPH_ENABLED && isAssetVertex()) {
            AtlasJanusVertex vertex = (AtlasJanusVertex) this;
            return vertex.getDynamicVertex().getPropertyKeys();
        } else {
            return getWrappedElement().keys();
        }
    }

    @Override
    public void removeProperty(String propertyName) {
        if (LEAN_GRAPH_ENABLED && isAssetVertex()) {
            AtlasJanusVertex vertex = (AtlasJanusVertex) this;
            vertex.getDynamicVertex().removeProperty(propertyName);
            return;
        }

        Iterator<? extends Property<String>> it = getWrappedElement().properties(propertyName);
        while(it.hasNext()) {
            Property<String> property = it.next();
            property.remove();
            recordInternalAttribute(propertyName, null);
        }
    }

    @Override
    public void removePropertyValue(String propertyName, Object propertyValue) {
        List<Object> finalValues = new ArrayList<>();

        if (LEAN_GRAPH_ENABLED && isAssetVertex()) {
            AtlasJanusVertex vertex = (AtlasJanusVertex) this;
            if ( vertex.getDynamicVertex().hasProperty(propertyName)) {
                Collection allValues = vertex.getDynamicVertex().getProperty(propertyName, Collection.class);
                if (CollectionUtils.isNotEmpty(allValues)) {
                    allValues.remove(propertyValue);
                    finalValues = new ArrayList<>(allValues);
                }
            }

        } else {
            Iterator<? extends Property<Object>> it = getWrappedElement().properties(propertyName);

            boolean removedFirst = false;

            while (it.hasNext()) {
                Property currentProperty = it.next();
                Object currentPropertyValue = currentProperty.value();

                if (!removedFirst && Objects.equals(currentPropertyValue, propertyValue)) {
                    currentProperty.remove();
                    removedFirst = true;
                } else {
                    finalValues.add(currentPropertyValue);
                }
            }
        }

        recordInternalAttribute(propertyName, finalValues);
    }

    @Override
    public void removeAllPropertyValue(String propertyName, Object propertyValue) {
        List<Object> finalValues = new ArrayList<>();

        if (LEAN_GRAPH_ENABLED && isAssetVertex()) {
            AtlasJanusVertex vertex = (AtlasJanusVertex) this;
            if ( vertex.getDynamicVertex().hasProperty(propertyName)) {
                Collection allValues = vertex.getDynamicVertex().getProperty(propertyName, Collection.class);
                if (CollectionUtils.isNotEmpty(allValues)) {
                    allValues.removeIf(value -> Objects.equals(value, propertyValue));
                    finalValues = new ArrayList<>(allValues);
                }
            }

        } else {
            Iterator<? extends Property<Object>> it = getWrappedElement().properties(propertyName);

            while (it.hasNext()) {
                Property currentProperty      = it.next();
                Object   currentPropertyValue = currentProperty.value();

                if (Objects.equals(currentPropertyValue, propertyValue)) {
                    currentProperty.remove();
                } else {
                    finalValues.add(currentPropertyValue);
                }
            }
        }

        recordInternalAttribute(propertyName, finalValues);
    }

    @Override
    public void setProperty(String propertyName, Object value) {
        try {
            if (value == null) {
                Object existingVal = getProperty(propertyName, Object.class);
                if (existingVal != null) {
                    removeProperty(propertyName);
                }
            } else {
                if (LEAN_GRAPH_ENABLED && isAssetVertex()) {
                    AtlasJanusVertex vertex = (AtlasJanusVertex) this;
                    vertex.getDynamicVertex().setProperty(propertyName, value);

                    if (VERTEX_CORE_PROPERTIES.contains(propertyName)) {
                        getWrappedElement().property(propertyName, value);
                    }
                } else {
                    // Might be an edge
                    getWrappedElement().property(propertyName, value);
                    recordInternalAttribute(propertyName, value);
                 }
            }
        } catch(SchemaViolationException e) {
            throw new AtlasSchemaViolationException(e);
        }
    }

    public boolean isAssetVertex() {
        return this instanceof AtlasVertex && this.element.label().equals(Constants.ASSET_VERTEX_LABEL);
    }

    @Override
    public Object getId() {
        return element.id();
    }

    @Override
    public T getWrappedElement() {
        return element;
    }


    @Override
    public JSONObject toJson(Set<String> propertyKeys) throws JSONException {

        return AtlasGraphSONUtility.jsonFromElement(this, propertyKeys, AtlasGraphSONMode.NORMAL);
    }

    @Override
    public int hashCode() {
        int result = 37;
        result = 17*result + getClass().hashCode();
        result = 17*result + getWrappedElement().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object other) {

        if (other == null) {
            return false;
        }

        if (other.getClass() != getClass()) {
            return false;
        }
        AtlasJanusElement otherElement = (AtlasJanusElement) other;
        return getWrappedElement().equals(otherElement.getWrappedElement());
    }

    @Override
    public List<String> getListProperty(String propertyName) {
        List<String> value =  getProperty(propertyName, List.class);
        return value;
    }

    @Override
    public <V> List<V> getMultiValuedProperty(String propertyName, Class<V> elementType) {
        if (LEAN_GRAPH_ENABLED && isAssetVertex()) {
            Object val = getProperty(propertyName, elementType);
            if (val == null) {
                return new ArrayList<>(0);
            } else {
                return (List<V>) val;
            }
        }

        Iterator<? extends Property<Object>> it = getWrappedElement().properties(propertyName);

        List<V> value = new ArrayList<>();
        while (it.hasNext()) {
            Property currentProperty      = it.next();
            Object   currentPropertyValue = currentProperty.value();
            value.add((V) currentPropertyValue);
        }
        return value;
    }

    @Override
    public <V> Set<V> getMultiValuedSetProperty(String propertyName, Class<V> elementType) {
        if (LEAN_GRAPH_ENABLED && isAssetVertex()) {
            Set<V> value = new HashSet<>();
            Object prop = getProperty(propertyName, elementType);
            if (prop == null) {
                return value;
            } else if (prop instanceof Collection) {
                value.addAll((Collection<V>) prop);
            } else {
                value.add((V) prop);
            }
            return value;
        }

        Set<V> value = new HashSet<>();
        Iterator<? extends Property<Object>> it = getWrappedElement().properties(propertyName);

        while (it.hasNext()) {
            Property currentProperty      = it.next();
            Object   currentPropertyValue = currentProperty.value();
            value.add((V) currentPropertyValue);
        }
        return value;
    }

    @Override
    public void setListProperty(String propertyName, List<String> values) {
        setProperty(propertyName, values);

    }

    @Override
    public boolean exists() {
        try {
            return !((JanusGraphElement)element).isRemoved();
        } catch(IllegalStateException e) {
            return false;
        }

    }

    @Override
    public <T> void setJsonProperty(String propertyName, T value) {
        setProperty(propertyName, value);
    }

    @Override
    public <T> T getJsonProperty(String propertyName) {
        return (T)getProperty(propertyName, String.class);
    }

    @Override
    public String getIdForDisplay() {
        return getId().toString();
    }

    @Override
    public <V> List<V> getListProperty(String propertyName, Class<V> elementType) {

        List<String> value = getListProperty(propertyName);

        if (value == null || value.isEmpty()) {
            return (List<V>)value;
        }

        if (AtlasEdge.class.isAssignableFrom(elementType)) {


            return (List<V>)Lists.transform(value, new Function<String, AtlasEdge>(){

                @Override
                public AtlasEdge apply(String input) {
                    return graph.getEdge(input);
                }
            });
        }

        if (AtlasVertex.class.isAssignableFrom(elementType)) {

            return (List<V>)Lists.transform(value, new Function<String, AtlasVertex>(){

                @Override
                public AtlasVertex apply(String input) {
                    return graph.getVertex(input);
                }
            });
        }

        return (List<V>)value;
    }


    @Override
    public void setPropertyFromElementsIds(String propertyName, List<AtlasElement> values) {
        List<String> propertyValue = new ArrayList<>(values.size());
        for(AtlasElement value: values) {
            propertyValue.add(value.getId().toString());
        }
        setProperty(propertyName, propertyValue);
    }


    @Override
    public void setPropertyFromElementId(String propertyName, AtlasElement value) {
        setProperty(propertyName, value.getId().toString());
    }


    @Override
    public boolean isIdAssigned() {
        return true;
    }

    protected void recordInternalAttribute(String propertyName, Object finalValue) {
        if (propertyName.startsWith(INTERNAL_PROPERTY_KEY_PREFIX)) {
            RequestContext context = RequestContext.get();
            String entityGuid = this.getProperty(GUID_PROPERTY_KEY, String.class);

            if (StringUtils.isNotEmpty(entityGuid)) {
                if (context.getAllInternalAttributesMap().get(entityGuid) != null) {
                    context.getAllInternalAttributesMap().get(entityGuid).put(propertyName, finalValue);
                } else {
                    Map<String, Object> map = new HashMap<>();
                    map.put(propertyName, finalValue);
                    context.getAllInternalAttributesMap().put(entityGuid, map);
                }
            }
        }
    }

    protected void recordInternalAttributeIncrementalAdd(String propertyName, Class cardinality) {
        if (propertyName.startsWith(INTERNAL_PROPERTY_KEY_PREFIX)) {
            String entityGuid = this.getProperty(GUID_PROPERTY_KEY, String.class);

            if (StringUtils.isNotEmpty(entityGuid)) {
                Collection<Object> currentValues = null;

                if (cardinality == List.class) {
                    currentValues = getMultiValuedProperty(propertyName, Object.class);
                } else {
                    currentValues = getMultiValuedSetProperty(propertyName, Object.class);
                }

                // Assumption: This method is being called after setting the property on element,
                // hence assuming currentValues is the final expected state and not adding `value` to `currentValues`

                if (StringUtils.isNotEmpty(entityGuid)) {
                    RequestContext context = RequestContext.get();

                    if (context.getAllInternalAttributesMap().get(entityGuid) != null) {
                        context.getAllInternalAttributesMap().get(entityGuid).put(propertyName, currentValues);
                    } else {
                        Map<String, Object> map = new HashMap<>();
                        map.put(propertyName, currentValues);
                        context.getAllInternalAttributesMap().put(entityGuid, map);
                    }
                }
            }
        }
    }
}

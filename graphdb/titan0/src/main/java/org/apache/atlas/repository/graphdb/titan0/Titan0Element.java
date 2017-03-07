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
package org.apache.atlas.repository.graphdb.titan0;

import java.lang.Override;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanElement;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;

/**
 * Titan 0.5.4 implementation of AtlasElement.
 */
public class Titan0Element<T extends Element> implements AtlasElement {

    protected Titan0Graph graph;
    protected T wrappedElement;

    public Titan0Element(Titan0Graph graph, T element) {
        wrappedElement = element;
        this.graph = graph;
    }

    @Override
    public Object getId() {
        return wrappedElement.getId();
    }

    @Override
    public Set<String> getPropertyKeys() {
        return wrappedElement.getPropertyKeys();
    }

    @Override
    public <U> void setProperty(String propertyName, U value) {
        try {
            wrappedElement.setProperty(propertyName, value);
        } catch (SchemaViolationException e) {
            throw new AtlasSchemaViolationException(e);
        }
    }

    @Override
    public <U> U getProperty(String propertyName, Class<U> clazz) {

        Object rawValue = wrappedElement.getProperty(propertyName);

        if (rawValue == null) {
            return null;
        }
        if (AtlasEdge.class == clazz) {
            return (U)graph.getEdge(rawValue.toString());
        }
        if (AtlasVertex.class == clazz) {
            return (U)graph.getVertex(rawValue.toString());
        }
        return (U)rawValue;

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
    public void removeProperty(String propertyName) {
        wrappedElement.removeProperty(propertyName);

    }

    @Override
    public JSONObject toJson(Set<String> propertyKeys) throws JSONException {
        return GraphSONUtility.jsonFromElement(wrappedElement, propertyKeys, GraphSONMode.NORMAL);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.atlas.repository.graphdb.AtlasElement#getListProperty(java.
     * lang.String)
     */
    @Override
    public List<String> getListProperty(String propertyName) {
        return getProperty(propertyName, List.class);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.atlas.repository.graphdb.AtlasElement#setListProperty(java.
     * lang.String, java.util.List)
     */
    @Override
    public void setListProperty(String propertyName, List<String> values) {
        setProperty(propertyName, values);

    }

    @Override
    public T getWrappedElement() {
        return wrappedElement;
    }

    @Override
    public int hashCode() {
        int result = 37;
        result = 17 * result + getClass().hashCode();
        result = 17 * result + getWrappedElement().hashCode();
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
        Titan0Element otherElement = (Titan0Element) other;
        return getWrappedElement().equals(otherElement.getWrappedElement());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.atlas.repository.graphdb.AtlasElement#exists()
     */
    @Override
    public boolean exists() {
        try {
            return !((TitanElement)wrappedElement).isRemoved();
        } catch(IllegalStateException e) {
            return false;
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.atlas.repository.graphdb.AtlasElement#setJsonProperty(java.
     * lang.String, java.lang.Object)
     */
    @Override
    public <T> void setJsonProperty(String propertyName, T value) {
        setProperty(propertyName, value);

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.atlas.repository.graphdb.AtlasElement#getJsonProperty(java.
     * lang.String)
     */
    @Override
    public <T> T getJsonProperty(String propertyName) {
        return (T) getProperty(propertyName, String.class);
    }

    @Override
    public String getIdForDisplay() {
        return getId().toString();
    }



    @Override
    public <V> List<V> getListProperty(String propertyName, Class<V> elementType) {

        List<String> value = getListProperty(propertyName);

        if (value == null) {
            return null;
        }

        if (AtlasEdge.class == elementType) {

            return (List<V>)Lists.transform(value, new Function<String, AtlasEdge>(){

                @Override
                public AtlasEdge apply(String input) {
                    return graph.getEdge(input);
                }
            });
        }

        if (AtlasVertex.class == elementType) {

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
        for(AtlasElement element: values) {
            propertyValue.add(element.getId().toString());
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
}

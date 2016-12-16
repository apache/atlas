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
package org.apache.atlas.repository.store.graph.v1;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;

/**
 * Utility methods for Graph.
 */
public class AtlasGraphUtilsV1 {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasGraphUtilsV1.class);

    public static final String PROPERTY_PREFIX      = Constants.INTERNAL_PROPERTY_KEY_PREFIX + "type.";
    public static final String SUPERTYPE_EDGE_LABEL = PROPERTY_PREFIX + ".supertype";
    public static final String VERTEX_TYPE          = "typeSystem";

    public static final BiMap<String, String> RESERVED_CHARS_ENCODE_MAP =
            HashBiMap.create(new HashMap<String, String>() {{
                put("{", "_o");
                put("}", "_c");
                put("\"", "_q");
                put("$", "_d");
                put("%", "_p");
            }});


    public static String getPropertyKey(AtlasBaseTypeDef typeDef) {
        return getPropertyKey(typeDef.getName());
    }

    public static String getPropertyKey(AtlasBaseTypeDef typeDef, String child) {
        return getPropertyKey(typeDef.getName(), child);
    }

    public static String getPropertyKey(String typeName) {
        return PROPERTY_PREFIX + typeName;
    }

    public static String getPropertyKey(String typeName, String child) {
        return PROPERTY_PREFIX + typeName + "." + child;
    }

    public static String getIdFromVertex(AtlasVertex vertex) {
        return vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
    }

    public static String getTypeName(AtlasVertex instanceVertex) {
        return instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class);
    }

    public static String getEdgeLabel(String fromNode, String toNode) {
        return PROPERTY_PREFIX + "edge." + fromNode + "." + toNode;
    }

    public static String encodePropertyKey(String key) {
        String ret = key;

        if (StringUtils.isNotBlank(key)) {
            for (String str : RESERVED_CHARS_ENCODE_MAP.keySet()) {
                ret = ret.replace(str, RESERVED_CHARS_ENCODE_MAP.get(str));
            }
        }

        return ret;
    }

    public static String decodePropertyKey(String key) {
        String ret = key;

        if (StringUtils.isNotBlank(key)) {
            for (String encodedStr : RESERVED_CHARS_ENCODE_MAP.values()) {
                ret = ret.replace(encodedStr, RESERVED_CHARS_ENCODE_MAP.inverse().get(encodedStr));
            }
        }

        return ret;
    }

    public static <T extends AtlasElement> void setProperty(T element, String propertyName, Object value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> setProperty({}, {}, {})", toString(element), propertyName, value);
        }

        propertyName = encodePropertyKey(propertyName);

        Object existingValue = element.getProperty(propertyName, Object.class);

        if (value == null || (value instanceof Collection && ((Collection)value).isEmpty())) {
            if (existingValue != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Removing property {} from {}", propertyName, toString(element));
                }

                element.removeProperty(propertyName);
            }
        } else {
            if (!value.equals(existingValue)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Setting property {} in {}", propertyName, toString(element));
                }

                element.setProperty(propertyName, value);
            }
        }
    }

    public static <T extends AtlasElement, O> O getProperty(T element, String propertyName, Class<O> returnType) {
        Object property = element.getProperty(encodePropertyKey(propertyName), returnType);

        if (LOG.isDebugEnabled()) {
            LOG.debug("getProperty({}, {}) ==> {}", toString(element), propertyName, returnType.cast(property));
        }

        return returnType.cast(property);
    }

    private static String toString(AtlasElement element) {
        if (element instanceof AtlasVertex) {
            return toString((AtlasVertex) element);
        } else if (element instanceof AtlasEdge) {
            return toString((AtlasEdge)element);
        }

        return element.toString();
    }

    public static String toString(AtlasVertex vertex) {
        if(vertex == null) {
            return "vertex[null]";
        } else {
            if (LOG.isDebugEnabled()) {
                return getVertexDetails(vertex);
            } else {
                return String.format("vertex[id=%s]", vertex.getId().toString());
            }
        }
    }


    public static String toString(AtlasEdge edge) {
        if(edge == null) {
            return "edge[null]";
        } else {
            if (LOG.isDebugEnabled()) {
                return getEdgeDetails(edge);
            } else {
                return String.format("edge[id=%s]", edge.getId().toString());
            }
        }
    }

    public static String getVertexDetails(AtlasVertex vertex) {
        return String.format("vertex[id=%s type=%s guid=%s]",
                vertex.getId().toString(), getTypeName(vertex), getIdFromVertex(vertex));
    }

    public static String getEdgeDetails(AtlasEdge edge) {
        return String.format("edge[id=%s label=%s from %s -> to %s]", edge.getId(), edge.getLabel(),
                toString(edge.getOutVertex()), toString(edge.getInVertex()));
    }
}

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

package org.apache.atlas.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.ElementPropertyConfig;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class AtlasGraphSONUtility {

    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

    private final GraphSONMode mode;
    private final List<String> vertexPropertyKeys;
    private final List<String> edgePropertyKeys;
    private final ElementPropertyConfig.ElementPropertiesRule vertexPropertiesRule;
    private final ElementPropertyConfig.ElementPropertiesRule edgePropertiesRule;
    private final boolean normalized;

    /**
     * A AtlasGraphSONUtility that includes the specified properties.
     */
    public AtlasGraphSONUtility(GraphSONMode mode,
                                Set<String> vertexPropertyKeys, Set<String> edgePropertyKeys) {
        this(mode, ElementPropertyConfig.includeProperties(vertexPropertyKeys, edgePropertyKeys));
    }

    public AtlasGraphSONUtility(GraphSONMode mode, ElementPropertyConfig config) {
        this.vertexPropertyKeys = config.getVertexPropertyKeys();
        this.edgePropertyKeys = config.getEdgePropertyKeys();
        this.vertexPropertiesRule = config.getVertexPropertiesRule();
        this.edgePropertiesRule = config.getEdgePropertiesRule();
        this.normalized = config.isNormalized();

        this.mode = mode;
    }

    /**
     * Creates GraphSON for a single graph element.
     */
    public ObjectNode objectNodeFromElement(Element element) {
        final boolean isEdge = element instanceof Edge;
        final boolean showTypes = mode == GraphSONMode.EXTENDED;
        final List<String> propertyKeys = isEdge ? this.edgePropertyKeys : this.vertexPropertyKeys;
        final ElementPropertyConfig.ElementPropertiesRule elementPropertyConfig = isEdge ? this.edgePropertiesRule : this.vertexPropertiesRule;

        final ObjectNode jsonElement = createJSONMap(createPropertyMap(element, propertyKeys, elementPropertyConfig, normalized), propertyKeys, showTypes);

        putObject(jsonElement, GraphSONTokens._ID, element.getId());

        // it's important to keep the order of these straight.  check Edge first and then Vertex because there
        // are graph implementations that have Edge extend from Vertex
        if (element instanceof Edge) {
            final Edge edge = (Edge) element;

            putObject(jsonElement, GraphSONTokens._ID, element.getId());
            jsonElement.put(GraphSONTokens._TYPE, GraphSONTokens.EDGE);
            putObject(jsonElement, GraphSONTokens._OUT_V, edge.getVertex(Direction.OUT).getId());
            putObject(jsonElement, GraphSONTokens._IN_V, edge.getVertex(Direction.IN).getId());
            jsonElement.put(GraphSONTokens._LABEL, edge.getLabel());
        } else if (element instanceof Vertex) {
            putObject(jsonElement, GraphSONTokens._ID, element.getId());
            jsonElement.put(GraphSONTokens._TYPE, GraphSONTokens.VERTEX);
        }

        return jsonElement;
    }

    private static ObjectNode objectNodeFromElement(Element element, List<String> propertyKeys, GraphSONMode mode) {
        final AtlasGraphSONUtility graphson = element instanceof Edge ?
                new AtlasGraphSONUtility(mode, null, new HashSet<String>(propertyKeys))
                : new AtlasGraphSONUtility(mode, new HashSet<String>(propertyKeys), null);
        return graphson.objectNodeFromElement(element);
    }


    private static ArrayNode createJSONList(List list, List<String> propertyKeys, boolean showTypes) {
        final ArrayNode jsonList = jsonNodeFactory.arrayNode();
        for (Object item : list) {
            if (item instanceof Element) {
                jsonList.add(objectNodeFromElement((Element) item, propertyKeys,
                        showTypes ? GraphSONMode.EXTENDED : GraphSONMode.NORMAL));
            } else if (item instanceof List) {
                jsonList.add(createJSONList((List) item, propertyKeys, showTypes));
            } else if (item instanceof Map) {
                jsonList.add(createJSONMap((Map) item, propertyKeys, showTypes));
            } else if (item != null && item.getClass().isArray()) {
                jsonList.add(createJSONList(convertArrayToList(item), propertyKeys, showTypes));
            } else {
                addObject(jsonList, item);
            }
        }
        return jsonList;
    }

    private static ObjectNode createJSONMap(Map map, List<String> propertyKeys, boolean showTypes) {
        final ObjectNode jsonMap = jsonNodeFactory.objectNode();
        for (Object key : map.keySet()) {
            Object value = map.get(key);
            if (value != null) {
                if (value instanceof List) {
                    value = createJSONList((List) value, propertyKeys, showTypes);
                } else if (value instanceof Map) {
                    value = createJSONMap((Map) value, propertyKeys, showTypes);
                } else if (value instanceof Element) {
                    value = objectNodeFromElement((Element) value, propertyKeys,
                            showTypes ? GraphSONMode.EXTENDED : GraphSONMode.NORMAL);
                } else if (value.getClass().isArray()) {
                    value = createJSONList(convertArrayToList(value), propertyKeys, showTypes);
                }
            }

            putObject(jsonMap, key.toString(), getValue(value, showTypes));
        }

        return jsonMap;
    }

    private static void addObject(ArrayNode jsonList, Object value) {
        if (value == null) {
            jsonList.add((JsonNode) null);
        } else if (value.getClass() == Boolean.class) {
            jsonList.add((Boolean) value);
        } else if (value.getClass() == Long.class) {
            jsonList.add((Long) value);
        } else if (value.getClass() == Integer.class) {
            jsonList.add((Integer) value);
        } else if (value.getClass() == Float.class) {
            jsonList.add((Float) value);
        } else if (value.getClass() == Double.class) {
            jsonList.add((Double) value);
        } else if (value.getClass() == Byte.class) {
            jsonList.add((Byte) value);
        } else if (value.getClass() == Short.class) {
            jsonList.add((Short) value);
        } else if (value.getClass() == String.class) {
            jsonList.add((String) value);
        } else if (value.getClass() == BigDecimal.class) {
            jsonList.add((BigDecimal) value);
        } else if (value.getClass() == BigInteger.class) {
            jsonList.add(((BigInteger) value).longValue());
        } else if (value instanceof ObjectNode) {
            jsonList.add((ObjectNode) value);
        } else if (value instanceof ArrayNode) {
            jsonList.add((ArrayNode) value);
        } else {
            jsonList.add(value.toString());
        }
    }

    private static void putObject(ObjectNode jsonMap, String key, Object value) {
        if (value == null) {
            jsonMap.put(key, (JsonNode) null);
        } else if (value.getClass() == Boolean.class) {
            jsonMap.put(key, (Boolean) value);
        } else if (value.getClass() == Long.class) {
            jsonMap.put(key, (Long) value);
        } else if (value.getClass() == Integer.class) {
            jsonMap.put(key, (Integer) value);
        } else if (value.getClass() == Float.class) {
            jsonMap.put(key, (Float) value);
        } else if (value.getClass() == Double.class) {
            jsonMap.put(key, (Double) value);
        } else if (value.getClass() == Short.class) {
            jsonMap.put(key, (Short) value);
        } else if (value.getClass() == Byte.class) {
            jsonMap.put(key, (Byte) value);
        } else if (value.getClass() == String.class) {
            jsonMap.put(key, (String) value);
        } else if(value.getClass() == BigDecimal.class) {
            jsonMap.put(key, (BigDecimal) value);
        } else if(value.getClass() == BigInteger.class) {
            jsonMap.put(key, ((BigInteger) value).longValue());
        } else if (value instanceof ObjectNode) {
            jsonMap.put(key, (ObjectNode) value);
        } else if (value instanceof ArrayNode) {
            jsonMap.put(key, (ArrayNode) value);
        } else {
            jsonMap.put(key, value.toString());
        }
    }

    private static Map createPropertyMap(Element element, List<String> propertyKeys,
                                         ElementPropertyConfig.ElementPropertiesRule rule, boolean normalized) {
        final Map map = new HashMap<String, Object>();
        final List<String> propertyKeyList;
        if (normalized) {
            final List<String> sorted = new ArrayList<String>(element.getPropertyKeys());
            Collections.sort(sorted);
            propertyKeyList = sorted;
        } else {
            propertyKeyList = new ArrayList<String>(element.getPropertyKeys());
        }

        if (propertyKeys == null) {
            for (String key : propertyKeyList) {
                final Object valToPutInMap = element.getProperty(key);
                if (valToPutInMap != null) {
                    map.put(key, valToPutInMap);
                }
            }
        } else {
            if (rule == ElementPropertyConfig.ElementPropertiesRule.INCLUDE) {
                for (String key : propertyKeys) {
                    final Object valToPutInMap = element.getProperty(key);
                    if (valToPutInMap != null) {
                        map.put(key, valToPutInMap);
                    }
                }
            } else {
                for (String key : propertyKeyList) {
                    if (!propertyKeys.contains(key)) {
                        final Object valToPutInMap = element.getProperty(key);
                        if (valToPutInMap != null) {
                            map.put(key, valToPutInMap);
                        }
                    }
                }
            }
        }

        return map;
    }

    private static Object getValue(Object value, boolean includeType) {

        Object returnValue = value;

        // if the includeType is set to true then show the data types of the properties
        if (includeType) {

            // type will be one of: map, list, string, long, int, double, float.
            // in the event of a complex object it will call a toString and store as a
            // string
            String type = determineType(value);

            ObjectNode valueAndType = jsonNodeFactory.objectNode();
            valueAndType.put(GraphSONTokens.TYPE, type);

            if (type.equals(GraphSONTokens.TYPE_LIST)) {

                // values of lists must be accumulated as ObjectNode objects under the value key.
                // will return as a ArrayNode. called recursively to traverse the entire
                // object graph of each item in the array.
                ArrayNode list = (ArrayNode) value;

                // there is a set of values that must be accumulated as an array under a key
                ArrayNode valueArray = valueAndType.putArray(GraphSONTokens.VALUE);
                for (int ix = 0; ix < list.size(); ix++) {
                    // the value of each item in the array is a node object from an ArrayNode...must
                    // get the value of it.
                    addObject(valueArray, getValue(getTypedValueFromJsonNode(list.get(ix)), includeType));
                }

            } else if (type.equals(GraphSONTokens.TYPE_MAP)) {

                // maps are converted to a ObjectNode.  called recursively to traverse
                // the entire object graph within the map.
                ObjectNode convertedMap = jsonNodeFactory.objectNode();
                ObjectNode jsonObject = (ObjectNode) value;
                Iterator keyIterator = jsonObject.fieldNames();
                while (keyIterator.hasNext()) {
                    Object key = keyIterator.next();

                    // no need to getValue() here as this is already a ObjectNode and should have type info
                    convertedMap.put(key.toString(), jsonObject.get(key.toString()));
                }

                valueAndType.put(GraphSONTokens.VALUE, convertedMap);
            } else {

                // this must be a primitive value or a complex object.  if a complex
                // object it will be handled by a call to toString and stored as a
                // string value
                putObject(valueAndType, GraphSONTokens.VALUE, value);
            }

            // this goes back as a JSONObject with data type and value
            returnValue = valueAndType;
        }

        return returnValue;
    }

    static Object getTypedValueFromJsonNode(JsonNode node) {
        Object theValue = null;

        if (node != null && !node.isNull()) {
            if (node.isBoolean()) {
                theValue = node.booleanValue();
            } else if (node.isDouble()) {
                theValue = node.doubleValue();
            } else if (node.isFloatingPointNumber()) {
                theValue = node.floatValue();
            } else if (node.isInt()) {
                theValue = node.intValue();
            } else if (node.isLong()) {
                theValue = node.longValue();
            } else if (node.isBigDecimal()) {
                theValue = node.bigIntegerValue();
            } else if (node.isBigInteger()) {
                theValue = node.bigIntegerValue();
            } else if (node.isTextual()) {
                theValue = node.textValue();
            } else if (node.isArray()) {
                // this is an array so just send it back so that it can be
                // reprocessed to its primitive components
                theValue = node;
            } else if (node.isObject()) {
                // this is an object so just send it back so that it can be
                // reprocessed to its primitive components
                theValue = node;
            } else {
                theValue = node.textValue();
            }
        }

        return theValue;
    }

    private static List convertArrayToList(final Object value) {
        final ArrayList<Object> list = new ArrayList<Object>();
        int arrlength = Array.getLength(value);
        for (int i = 0; i < arrlength; i++) {
            Object object = Array.get(value, i);
            list.add(object);
        }
        return list;
    }

    private static String determineType(Object value) {
        String type = GraphSONTokens.TYPE_STRING;
        if (value == null) {
            type = GraphSONTokens.TYPE_UNKNOWN;
        } else if (value.getClass() == Double.class) {
            type = GraphSONTokens.TYPE_DOUBLE;
        } else if (value.getClass() == Float.class) {
            type = GraphSONTokens.TYPE_FLOAT;
        } else if (value.getClass() == Byte.class) {
            type = GraphSONTokens.TYPE_BYTE;
        } else if (value.getClass() == Short.class) {
            type = GraphSONTokens.TYPE_SHORT;
        } else if (value.getClass() == Integer.class) {
            type = GraphSONTokens.TYPE_INTEGER;
        } else if (value.getClass() == Long.class) {
            type = GraphSONTokens.TYPE_LONG;
        } else if (value.getClass() == Boolean.class) {
            type = GraphSONTokens.TYPE_BOOLEAN;
        } else if(value.getClass() == BigInteger.class) {
            type = GraphSONTokens.TYPE_BIG_INTEGER;
        } else if(value.getClass() == BigDecimal.class) {
            type = GraphSONTokens.TYPE_BIG_DECIMAL;
        } else if (value instanceof ArrayNode) {
            type = GraphSONTokens.TYPE_LIST;
        } else if (value instanceof ObjectNode) {
            type = GraphSONTokens.TYPE_MAP;
        }

        return type;
    }
}

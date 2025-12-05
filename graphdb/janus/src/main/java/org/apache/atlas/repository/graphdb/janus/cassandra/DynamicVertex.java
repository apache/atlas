package org.apache.atlas.repository.graphdb.janus.cassandra;


import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a dynamic vertex with arbitrary properties.
 */
public class DynamicVertex {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicVertex.class);

    private final Map<String, Object> properties = new HashMap<>();

    /**
     * Default constructor.
     */
    public DynamicVertex() {
    }

    /**
     * Constructor with initial properties.
     *
     * @param properties The initial properties to set
     */
    public DynamicVertex(Map<String, Object> properties) {
        if (properties != null) {
            this.properties.putAll(properties);
        }
    }

    public boolean hasProperties() {
        return !MapUtils.isEmpty(properties);
    }

    /**
     * Gets a property value as a specific type.
     *
     * @param key   The property key
     * @param clazz The expected class type
     * @return The property value cast to the specified type, or null if not found or not compatible
     */
    @SuppressWarnings("unchecked")
    public <T> T getProperty(String key, Class<T> clazz) {
        Object val = properties.get(key);
        if (val != null ) {
            try {
                if (clazz.equals(Long.class) && ! (val instanceof Long)) {
                    return (T) Long.valueOf((String) val);

                } else if (clazz.equals(Float.class) && !(val instanceof Float)) {
                    return (T) Float.valueOf((String) val);

                } else if (clazz.equals(Double.class) && !(val instanceof Double)) {
                    return (T) Double.valueOf((String) val);

                } else if (clazz.equals(Integer.class) && !(val instanceof Integer)) {
                    return (T) Integer.valueOf((String) val);

                } else if (clazz.equals(Map.class) && !(val instanceof Map)) {
                    return (T) AtlasType.fromJson((String) val, Map.class);
                }
            } catch (ClassCastException cce) {
                String errorMessage = String.format("Can not cast property %s from %s to %s", key, val.getClass().getName(), clazz.getName());
                LOG.error(errorMessage);
                throw cce;
            }


            return (T) val;
        }
        return null;
    }


    /**
     * Sets a property value.
     *
     * @param key   The property key
     * @param value The property value
     * @return This DynamicVertex instance for method chaining
     */
    public DynamicVertex setProperty(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    public void addSetProperty(String key, Object value) {
        Object currentValue = properties.getOrDefault(key, null);

        Set<Object> values;
        try {

            if (currentValue == null) {
                values = new HashSet<>(1);
            } else if (currentValue instanceof List) {
                values = new HashSet<>((List) currentValue);
            } else {
                values = (Set) currentValue;
            }

            if (!values.contains(value)) {
                values.add(value);
                properties.put(key, values);
            }
        } catch (ClassCastException cce) {
            throw new RuntimeException(cce);
        }
    }

    public void addListProperty(String key, Object value) {
        try {
            List<Object> values = (List<Object>) properties.getOrDefault(key, new ArrayList<>(1));
            values.add(value);
            properties.put(key, values);

        } catch (ClassCastException cce) {
            throw new RuntimeException(cce);
        }
    }

    /**
     * Gets all property keys.
     *
     * @return A set of all property keys
     */
    public Set<String> getPropertyKeys() {
        return Collections.unmodifiableSet(properties.keySet());
    }

    /**
     * Gets all properties as a map.
     *
     * @return An unmodifiable map of all properties
     */
    public Map<String, Object> getAllProperties() {
        return Collections.unmodifiableMap(properties);
    }

    /**
     * Checks if a property exists.
     *
     * @param key The property key
     * @return true if the property exists, false otherwise
     */
    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }

    /**
     * Removes a property.
     *
     * @param key The property key
     * @return The previous value, or null if no mapping existed
     */
    public Object removeProperty(String key) {
        return properties.remove(key);
    }

    @Override
    public String toString() {
        return "DynamicVertex{properties=" + properties + '}';
    }
}
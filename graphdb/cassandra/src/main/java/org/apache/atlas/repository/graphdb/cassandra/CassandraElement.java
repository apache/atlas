package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.type.AtlasType;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.*;

public abstract class CassandraElement implements AtlasElement {

    protected String id;  // non-final: eager deterministic ID rewrite may change it before commit
    protected final Map<String, Object> properties;
    protected final CassandraGraph graph;
    protected boolean isNew;
    protected boolean isDirty;
    protected boolean isDeleted;

    protected CassandraElement(String id, CassandraGraph graph) {
        this.id         = id;
        this.graph      = graph;
        this.properties = new LinkedHashMap<>();
        this.isNew      = true;
        this.isDirty    = false;
        this.isDeleted  = false;
    }

    protected CassandraElement(String id, Map<String, Object> properties, CassandraGraph graph) {
        this.id         = id;
        this.graph      = graph;
        this.properties = properties != null ? new LinkedHashMap<>(properties) : new LinkedHashMap<>();
        this.isNew      = false;
        this.isDirty    = false;
        this.isDeleted  = false;
    }

    @Override
    public Object getId() {
        return id;
    }

    public String getIdString() {
        return id;
    }

    @Override
    public Collection<? extends String> getPropertyKeys() {
        return Collections.unmodifiableSet(properties.keySet());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProperty(String propertyName, Class<T> clazz) {
        Object value = properties.get(propertyName);

        if (value == null) {
            return null;
        }

        if (clazz.isInstance(value)) {
            return (T) value;
        }

        // Handle type conversions
        if (clazz == String.class) {
            return (T) String.valueOf(value);
        } else if (clazz == Long.class || clazz == long.class) {
            if (value instanceof Number) {
                return (T) Long.valueOf(((Number) value).longValue());
            }
            return (T) Long.valueOf(String.valueOf(value));
        } else if (clazz == Integer.class || clazz == int.class) {
            if (value instanceof Number) {
                return (T) Integer.valueOf(((Number) value).intValue());
            }
            return (T) Integer.valueOf(String.valueOf(value));
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            return (T) Boolean.valueOf(String.valueOf(value));
        } else if (clazz == Double.class || clazz == double.class) {
            if (value instanceof Number) {
                return (T) Double.valueOf(((Number) value).doubleValue());
            }
            return (T) Double.valueOf(String.valueOf(value));
        } else if (clazz == Float.class || clazz == float.class) {
            if (value instanceof Number) {
                return (T) Float.valueOf(((Number) value).floatValue());
            }
            return (T) Float.valueOf(String.valueOf(value));
        } else if (clazz == Short.class || clazz == short.class) {
            if (value instanceof Number) {
                return (T) Short.valueOf(((Number) value).shortValue());
            }
            return (T) Short.valueOf(String.valueOf(value));
        } else if (clazz == Byte.class || clazz == byte.class) {
            if (value instanceof Number) {
                return (T) Byte.valueOf(((Number) value).byteValue());
            }
            return (T) Byte.valueOf(String.valueOf(value));
        } else if (clazz == List.class) {
            // Handle String→List JSON deserialization (e.g., TypeDef enum values stored as JSON string)
            if (value instanceof String) {
                List<?> parsed = AtlasType.fromJson((String) value, List.class);
                return (T) parsed;
            }
        }

        return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Collection<T> getPropertyValues(String propertyName, Class<T> type) {
        Object value = properties.get(propertyName);

        if (value == null) {
            return Collections.emptyList();
        }

        if (value instanceof Collection) {
            // After Cassandra JSON round-trip, Sets become ArrayLists and nested
            // collections can appear if addProperty was called before the fix.
            // Flatten any nested collections to ensure all elements are scalars.
            Collection<?> coll = (Collection<?>) value;
            List<T> result = new ArrayList<>();
            for (Object elem : coll) {
                if (elem instanceof Collection) {
                    for (Object nested : (Collection<?>) elem) {
                        result.add((T) nested);
                    }
                } else {
                    result.add((T) elem);
                }
            }
            return result;
        }

        return Collections.singletonList((T) value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> getListProperty(String propertyName) {
        Object value = properties.get(propertyName);

        if (value == null) {
            return null;
        }

        if (value instanceof List) {
            return (List<String>) value;
        }

        if (value instanceof String) {
            return AtlasType.fromJson((String) value, List.class);
        }

        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> List<V> getMultiValuedProperty(String propertyName, Class<V> elementType) {
        Object value = properties.get(propertyName);

        if (value == null) {
            return new ArrayList<>();
        }

        if (value instanceof List) {
            return new ArrayList<>((List<V>) value);
        }

        // addProperty() stores multi-valued attributes as LinkedHashSet.
        // Before Cassandra round-trip (e.g. mutation response), the value
        // is still a Set, not a List. Handle any Collection type to avoid
        // wrapping the entire Set as a single element (double-nesting).
        if (value instanceof Collection) {
            return new ArrayList<>((Collection<V>) value);
        }

        List<V> result = new ArrayList<>();
        result.add((V) value);
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> Set<V> getMultiValuedSetProperty(String propertyName, Class<V> elementType) {
        Object value = properties.get(propertyName);

        if (value == null) {
            return new HashSet<>();
        }

        if (value instanceof Set) {
            return new HashSet<>((Set<V>) value);
        }

        if (value instanceof Collection) {
            return new HashSet<>((Collection<V>) value);
        }

        Set<V> result = new HashSet<>();
        result.add((V) value);
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> List<V> getListProperty(String propertyName, Class<V> elementType) {
        return getMultiValuedProperty(propertyName, elementType);
    }

    @Override
    public void setListProperty(String propertyName, List<String> values) {
        if (values == null) {
            properties.remove(propertyName);
        } else {
            properties.put(propertyName, new ArrayList<>(values));
        }
        markDirty();
    }

    @Override
    public void setPropertyFromElementsIds(String propertyName, List<AtlasElement> values) {
        if (values == null || values.isEmpty()) {
            properties.remove(propertyName);
        } else {
            List<String> ids = new ArrayList<>(values.size());
            for (AtlasElement element : values) {
                ids.add(element.getId().toString());
            }
            properties.put(propertyName, ids);
        }
        markDirty();
    }

    @Override
    public void setPropertyFromElementId(String propertyName, AtlasElement value) {
        if (value == null) {
            properties.remove(propertyName);
        } else {
            properties.put(propertyName, value.getId().toString());
        }
        markDirty();
    }

    @Override
    public void removeProperty(String propertyName) {
        properties.remove(propertyName);
        markDirty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void removePropertyValue(String propertyName, Object propertyValue) {
        Object current = properties.get(propertyName);
        if (current instanceof Collection) {
            ((Collection) current).remove(propertyValue);
        } else if (current != null && current.equals(propertyValue)) {
            properties.remove(propertyName);
        }
        markDirty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void removeAllPropertyValue(String propertyName, Object propertyValue) {
        Object current = properties.get(propertyName);
        if (current instanceof Collection) {
            ((Collection) current).removeAll(Collections.singleton(propertyValue));
        } else if (current != null && current.equals(propertyValue)) {
            properties.remove(propertyName);
        }
        markDirty();
    }

    @Override
    public <T> void setProperty(String propertyName, T value) {
        if (value == null) {
            properties.remove(propertyName);
        } else {
            properties.put(propertyName, value);
        }
        markDirty();
    }

    @Override
    public JSONObject toJson(Set<String> propertyKeys) throws JSONException {
        JSONObject json = new JSONObject();
        json.put("id", id);

        Set<String> keys = (propertyKeys != null) ? propertyKeys : properties.keySet();
        for (String key : keys) {
            Object value = properties.get(key);
            if (value != null) {
                json.put(key, value);
            }
        }
        return json;
    }

    @Override
    public boolean exists() {
        return !isDeleted;
    }

    @Override
    public <T> void setJsonProperty(String propertyName, T value) {
        if (value == null) {
            properties.remove(propertyName);
        } else {
            properties.put(propertyName, AtlasType.toJson(value));
        }
        markDirty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getJsonProperty(String propertyName) {
        Object value = properties.get(propertyName);
        if (value instanceof String) {
            return (T) AtlasType.fromJson((String) value, Object.class);
        }
        return (T) value;
    }

    @Override
    public String getIdForDisplay() {
        return id;
    }

    @Override
    public boolean isIdAssigned() {
        return id != null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getWrappedElement() {
        return (T) this;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public boolean isNew() {
        return isNew;
    }

    public boolean isDirty() {
        return isDirty;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void markDirty() {
        isDirty = true;
        // Notify the transaction buffer so existing (non-new) elements
        // are tracked as dirty and flushed to Cassandra on commit
        if (!isNew && this instanceof CassandraVertex) {
            graph.notifyVertexDirty((CassandraVertex) this);
        } else if (!isNew && this instanceof CassandraEdge) {
            graph.notifyEdgeDirty((CassandraEdge) this);
        }
    }

    public void markDeleted() {
        isDeleted = true;
        isDirty   = true;
    }

    public void markPersisted() {
        isNew   = false;
        isDirty = false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraElement that = (CassandraElement) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id='" + id + "'}";
    }
}

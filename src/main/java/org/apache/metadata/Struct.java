package org.apache.metadata;

import java.util.HashMap;
import java.util.Map;

public class Struct implements IStruct {

    public final String typeName;
    private final Map<String, Object> values;

    public Struct(String typeName) {
        this.typeName = typeName;
        values = new HashMap<String, Object>();
    }

    /**
        @nopublic
    */
    public Struct(String typeName, Map<String, Object> values) {
        this(typeName);
        this.values.putAll(values);
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    @Override
    public Object get(String attrName) {
        return values.get(attrName);
    }

    @Override
    public void set(String attrName, Object value) {
        values.put(attrName, value);
    }

    /**
     * @nopublic
     * @return
     */
    public Map<String, Object> getValuesMap() {
        return values;
    }
}

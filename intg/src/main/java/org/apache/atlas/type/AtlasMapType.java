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
package org.apache.atlas.type;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * class that implements behaviour of a map-type.
 */
public class AtlasMapType extends AtlasType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityType.class);

    private final String keyTypeName;
    private final String valueTypeName;

    private AtlasType keyType;
    private AtlasType valueType;

    public AtlasMapType(String keyTypeName, String valueTypeName) {
        super(AtlasBaseTypeDef.getMapTypeName(keyTypeName, valueTypeName), TypeCategory.MAP);

        this.keyTypeName   = keyTypeName;
        this.valueTypeName = valueTypeName;
    }

    public AtlasMapType(AtlasType keyType, AtlasType valueType) {
        super(AtlasBaseTypeDef.getMapTypeName(keyType.getTypeName(), valueType.getTypeName()), TypeCategory.MAP);

        this.keyTypeName   = keyType.getTypeName();
        this.valueTypeName = valueType.getTypeName();
        this.keyType       = keyType;
        this.valueType     = valueType;
    }

    public AtlasMapType(String keyTypeName, String valueTypeName, AtlasTypeRegistry typeRegistry)
        throws AtlasBaseException {
        super(AtlasBaseTypeDef.getMapTypeName(keyTypeName, valueTypeName), TypeCategory.MAP);

        this.keyTypeName   = keyTypeName;
        this.valueTypeName = valueTypeName;

        resolveReferences(typeRegistry);
    }

    public String getKeyTypeName() {
        return keyTypeName;
    }

    public String getValueTypeName() {
        return valueTypeName;
    }

    public AtlasType getKeyType() {
        return keyType;
    }

    public AtlasType getValueType() {
        return valueType;
    }

    public void setKeyType(AtlasType keyType) {
        this.keyType = keyType;
    }

    @Override
    public void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        this.keyType   = typeRegistry.getType(keyTypeName);
        this.valueType = typeRegistry.getType(valueTypeName);
    }

    @Override
    public Map<Object, Object>  createDefaultValue() {
        Map<Object, Object> ret = new HashMap<>();

        Object key = keyType.createDefaultValue();

        if ( key != null) {
            ret.put(key, valueType.createDefaultValue());
        }

        return ret;
    }

    @Override
    public boolean isValidValue(Object obj) {
        if (obj != null) {
            if (obj instanceof Map) {
                Map<Object, Objects> map = (Map<Object, Objects>) obj;

                for (Map.Entry e : map.entrySet()) {
                    if (!keyType.isValidValue(e.getKey()) || !valueType.isValidValue(e.getValue())) {
                        return false; // invalid key/value
                    }
                }
            } else {
                return false; // invalid type
            }
        }

        return true;
    }

    @Override
    public boolean isValidValueForUpdate(Object obj) {
        if (obj != null) {
            if (obj instanceof Map) {
                Map<Object, Objects> map = (Map<Object, Objects>) obj;

                for (Map.Entry e : map.entrySet()) {
                    if (!keyType.isValidValueForUpdate(e.getKey()) || !valueType.isValidValueForUpdate(e.getValue())) {
                        return false; // invalid key/value
                    }
                }
            } else {
                return false; // invalid type
            }
        }

        return true;
    }

    @Override
    public Map<Object, Object> getNormalizedValue(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof Map) {
            Map<Object, Object> ret = new HashMap<>();

            Map<Object, Objects> map = (Map<Object, Objects>) obj;

            for (Map.Entry e : map.entrySet()) {
                Object normalizedKey = keyType.getNormalizedValue(e.getKey());

                if (normalizedKey != null) {
                    Object value = e.getValue();

                    if (value != null) {
                        Object normalizedValue = valueType.getNormalizedValue(e.getValue());

                        if (normalizedValue != null) {
                            ret.put(normalizedKey, normalizedValue);
                        } else {
                            return null; // invalid value
                        }
                    } else {
                        ret.put(normalizedKey, value);
                    }
                } else {
                    return null; // invalid key
                }
            }

            return ret;
        }

        return null;
    }

    @Override
    public Map<Object, Object> getNormalizedValueForUpdate(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof Map) {
            Map<Object, Object> ret = new HashMap<>();

            Map<Object, Objects> map = (Map<Object, Objects>) obj;

            for (Map.Entry e : map.entrySet()) {
                Object normalizedKey = keyType.getNormalizedValueForUpdate(e.getKey());

                if (normalizedKey != null) {
                    Object value = e.getValue();

                    if (value != null) {
                        Object normalizedValue = valueType.getNormalizedValueForUpdate(e.getValue());

                        if (normalizedValue != null) {
                            ret.put(normalizedKey, normalizedValue);
                        } else {
                            return null; // invalid value
                        }
                    } else {
                        ret.put(normalizedKey, value);
                    }
                } else {
                    return null; // invalid key
                }
            }

            return ret;
        }

        return null;
    }

    @Override
    public boolean validateValue(Object obj, String objName, List<String> messages) {
        boolean ret = true;

        if (obj != null) {
            if (obj instanceof Map) {
                Map<Object, Objects> map = (Map<Object, Objects>) obj;

                for (Map.Entry e : map.entrySet()) {
                    Object key = e.getKey();

                    if (!keyType.isValidValue(key)) {
                        ret = false;

                        messages.add(objName + "." + key + ": invalid key for type " + getTypeName());
                    } else {
                        Object value = e.getValue();

                        ret = valueType.validateValue(value, objName + "." + key, messages) && ret;
                    }
                }
            } else {
                ret = false;

                messages.add(objName + "=" + obj + ": invalid value for type " + getTypeName());
            }
        }

        return ret;
    }

    @Override
    public boolean validateValueForUpdate(Object obj, String objName, List<String> messages) {
        boolean ret = true;

        if (obj != null) {
            if (obj instanceof Map) {
                Map<Object, Objects> map = (Map<Object, Objects>) obj;

                for (Map.Entry e : map.entrySet()) {
                    Object key = e.getKey();

                    if (!keyType.isValidValueForUpdate(key)) {
                        ret = false;

                        messages.add(objName + "." + key + ": invalid key for type " + getTypeName());
                    } else {
                        Object value = e.getValue();

                        ret = valueType.validateValueForUpdate(value, objName + "." + key, messages) && ret;
                    }
                }
            } else {
                ret = false;

                messages.add(objName + "=" + obj + ": invalid value for type " + getTypeName());
            }
        }

        return ret;
    }

    @Override
    public AtlasType getTypeForAttribute() {
        AtlasType keyAttributeType   = keyType.getTypeForAttribute();
        AtlasType valueAttributeType = valueType.getTypeForAttribute();

        if (keyAttributeType == keyType && valueAttributeType == valueType) {
            return this;
        } else {
            AtlasType attributeType = new AtlasMapType(keyAttributeType, valueAttributeType);

            if (LOG.isDebugEnabled()) {
                LOG.debug("getTypeForAttribute(): {} ==> {}", getTypeName(), attributeType.getTypeName());
            }

            return attributeType;
        }
    }
}

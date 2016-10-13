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

import java.util.*;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * class that implements behaviour of a struct-type.
 */
public class AtlasStructType extends AtlasType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasStructType.class);

    private final AtlasStructDef structDef;

    private Map<String, AtlasType> attrTypes = Collections.emptyMap();


    public AtlasStructType(AtlasStructDef structDef) {
        super(structDef.getName());

        this.structDef = structDef;
    }

    public AtlasStructType(AtlasStructDef structDef, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super(structDef.getName());

        this.structDef = structDef;

        this.resolveReferences(typeRegistry);
    }

    public AtlasType getAttributeType(String attributeName) { return attrTypes.get(attributeName); }

    public AtlasAttributeDef getAttributeDef(String attributeName) { return structDef.getAttribute(attributeName); }

    @Override
    public void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        Map<String, AtlasType> a = new HashMap<String, AtlasType>();

        for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
            AtlasType attrType = typeRegistry.getType(attributeDef.getTypeName());

            if (attrType == null) {
                throw new AtlasBaseException(attributeDef.getTypeName() + ": unknown type for attribute "
                                             + structDef.getName() + "." + attributeDef.getName());
            }

            Cardinality cardinality = attributeDef.getCardinality();

            if (cardinality == Cardinality.LIST || cardinality == Cardinality.SET) {
                attrType = new AtlasArrayType(attrType,
                                              attributeDef.getValuesMinCount(),
                                              attributeDef.getValuesMaxCount());
            }

            a.put(attributeDef.getName(), attrType);
        }

        this.attrTypes = Collections.unmodifiableMap(a);
    }

    @Override
    public AtlasStruct createDefaultValue() {
        AtlasStruct ret = new AtlasStruct(structDef.getName());

        populateDefaultValues(ret);

        return  ret;
    }

    @Override
    public boolean isValidValue(Object obj) {
        if (obj != null) {
            if (obj instanceof AtlasStruct) {
                AtlasStruct structObj = (AtlasStruct) obj;

                for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                    if (!isAssignableValue(structObj.getAttribute(attributeDef.getName()), attributeDef)) {
                        return false;
                    }
                }
            } else if (obj instanceof Map) {
                Map map = (Map) obj;

                for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                    if (!isAssignableValue(map.get(attributeDef.getName()), attributeDef)) {
                        return false; // no value for non-optinal attribute
                    }
                }
            } else {
                return false; // invalid type
            }
        }

        return true;
    }

    @Override
    public Object getNormalizedValue(Object obj) {
        Object ret = null;

        if (obj != null) {
            if (isValidValue(obj)) {
                if (obj instanceof AtlasStruct) {
                    normalizeAttributeValues((AtlasStruct) obj);
                    ret = obj;
                } else if (obj instanceof Map) {
                    normalizeAttributeValues((Map) obj);
                    ret = obj;
                }
            }
        }

        return ret;
    }

    @Override
    public boolean validateValue(Object obj, String objName, List<String> messages) {
        boolean ret = true;

        if (obj != null) {
            if (obj instanceof AtlasStruct) {
                AtlasStruct structObj = (AtlasStruct) obj;

                for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                    String    attrName = attributeDef.getName();
                    AtlasType dataType = attrTypes.get(attributeDef.getName());

                    if (dataType != null) {
                        Object value     = structObj.getAttribute(attrName);
                        String fieldName = objName + "." + attrName;

                        if (value != null) {
                            ret = dataType.validateValue(value, fieldName, messages) && ret;
                        } else if (!attributeDef.isOptional()) {
                            ret = false;

                            messages.add(fieldName + ": mandatory attribute value missing in type " + getTypeName());
                        }
                    }
                }
            } else if (obj instanceof Map) {
                Map map = (Map) obj;

                for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                    String    attrName = attributeDef.getName();
                    AtlasType dataType = attrTypes.get(attributeDef.getName());

                    if (dataType != null) {
                        Object value     = map.get(attrName);
                        String fieldName = objName + "." + attrName;

                        if (value != null) {
                            ret = dataType.validateValue(value, fieldName, messages) && ret;
                        } else if (!attributeDef.isOptional()) {
                            ret = false;

                            messages.add(fieldName + ": mandatory attribute value missing in type " + getTypeName());
                        }
                    }
                }
            } else {
                ret = false;

                messages.add(objName + "=" + obj + ": invalid value for type " + getTypeName());
            }
        }

        return ret;
    }

    public void normalizeAttributeValues(AtlasStruct obj) {
        if (obj != null) {
            for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                String attributeName = attributeDef.getName();

                if (obj.hasAttribute(attributeName)) {
                    Object attributeValue = getNormalizedValue(obj.getAttribute(attributeName), attributeDef);

                    obj.setAttribute(attributeName, attributeValue);
                } else if (!attributeDef.isOptional()) {
                    obj.setAttribute(attributeName, createDefaultValue(attributeDef));
                }
            }
        }
    }

    public void normalizeAttributeValues(Map<String, Object> obj) {
        if (obj != null) {
            for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                String attributeName = attributeDef.getName();

                if (obj.containsKey(attributeName)) {
                    Object attributeValue = getNormalizedValue(obj.get(attributeName), attributeDef);

                    obj.put(attributeName, attributeValue);
                } else if (!attributeDef.isOptional()) {
                    obj.put(attributeName, createDefaultValue(attributeDef));
                }
            }
        }
    }

    public void populateDefaultValues(AtlasStruct obj) {
        if (obj != null) {
            Map<String, Object> attributes = obj.getAttributes();

            if (attributes == null) {
                attributes = new HashMap<String, Object>();
            }

            for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                if (!attributeDef.isOptional()) {
                    attributes.put(attributeDef.getName(), createDefaultValue(attributeDef));
                }
            }

            obj.setAttributes(attributes);
        }
    }

    private Object createDefaultValue(AtlasAttributeDef attributeDef) {
        Object ret = null;

        if (attributeDef != null) {
            AtlasType dataType = attrTypes.get(attributeDef.getName());

            if (dataType != null) {
                ret = dataType.createDefaultValue();
            }
        }

        return ret;
    }

    private boolean isAssignableValue(Object value, AtlasAttributeDef attributeDef) {
        boolean ret = true;

        if (value != null) {
            AtlasType attrType = attrTypes.get(attributeDef.getName());

            if (attrType != null) {
                if (!attrType.isValidValue(value)) {
                    ret = false; // invalid value
                }
            }
        } else if (!attributeDef.isOptional()) {
            ret = false; // mandatory attribute not present
        }

        return ret;
    }

    private Object getNormalizedValue(Object value, AtlasAttributeDef attributeDef) {
        AtlasType attrType = attrTypes.get(attributeDef.getName());

        if (attrType != null) {
            if (value == null) {
                if (!attributeDef.isOptional()) {
                    return attrType.createDefaultValue();
                }
            } else {
                return attrType.getNormalizedValue(value);
            }
        }

        return null;
    }
}

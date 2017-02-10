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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.COUNT_NOT_SET;

/**
 * class that implements behaviour of an array-type.
 */
public class AtlasArrayType extends AtlasType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasArrayType.class);

    private final String elementTypeName;
    private int          minCount;
    private int          maxCount;

    private AtlasType elementType;

    public AtlasArrayType(AtlasType elementType) {
        this(elementType, COUNT_NOT_SET, COUNT_NOT_SET);
    }

    public AtlasArrayType(AtlasType elementType, int minCount, int maxCount) {
        super(AtlasBaseTypeDef.getArrayTypeName(elementType.getTypeName()), TypeCategory.ARRAY);

        this.elementTypeName = elementType.getTypeName();
        this.minCount        = minCount;
        this.maxCount        = maxCount;
        this.elementType     = elementType;
    }

    public AtlasArrayType(String elementTypeName) {
        this(elementTypeName, COUNT_NOT_SET, COUNT_NOT_SET);
    }

    public AtlasArrayType(String elementTypeName, int minCount, int maxCount) {
        super(AtlasBaseTypeDef.getArrayTypeName(elementTypeName), TypeCategory.ARRAY);

        this.elementTypeName = elementTypeName;
        this.minCount        = minCount;
        this.maxCount        = maxCount;
        this.elementType     = null;
    }

    public AtlasArrayType(String elementTypeName, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        this(elementTypeName, COUNT_NOT_SET, COUNT_NOT_SET, typeRegistry);
    }

    public AtlasArrayType(String elementTypeName, int minCount, int maxCount, AtlasTypeRegistry typeRegistry)
        throws  AtlasBaseException {
        super(AtlasBaseTypeDef.getArrayTypeName(elementTypeName), TypeCategory.ARRAY);

        this.elementTypeName = elementTypeName;
        this.minCount        = minCount;
        this.maxCount        = maxCount;

        this.resolveReferences(typeRegistry);
    }

    public String getElementTypeName() {
        return elementTypeName;
    }

    public void setMinCount(int minCount) { this.minCount = minCount; }

    public int getMinCount() {
        return minCount;
    }

    public void setMaxCount(int maxCount) { this.maxCount = maxCount; }

    public int getMaxCount() {
        return maxCount;
    }

    public AtlasType getElementType() {
        return elementType;
    }

    @Override
    public void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        elementType = typeRegistry.getType(elementTypeName);
    }

    @Override
    public Collection<?> createDefaultValue() {
        Collection<Object> ret = new ArrayList<>();

        ret.add(elementType.createDefaultValue());

        if (minCount != COUNT_NOT_SET) {
            for (int i = 1; i < minCount; i++) {
                ret.add(elementType.createDefaultValue());
            }
        }

        return ret;
    }

    @Override
    public boolean isValidValue(Object obj) {
        if (obj != null) {
            if (obj instanceof List || obj instanceof Set) {
                Collection objList = (Collection) obj;

                if (!isValidElementCount(objList.size())) {
                    return false;
                }

                for (Object element : objList) {
                    if (!elementType.isValidValue(element)) {
                        return false;
                    }
                }
            } else if (obj.getClass().isArray()) {
                int arrayLen = Array.getLength(obj);

                if (!isValidElementCount(arrayLen)) {
                    return false;
                }

                for (int i = 0; i < arrayLen; i++) {
                    if (!elementType.isValidValue(Array.get(obj, i))) {
                        return false;
                    }
                }
            } else {
                return false; // invalid type
            }
        }

        return true;
    }

    @Override
    public Collection<?> getNormalizedValue(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof List || obj instanceof Set) {
            List<Object> ret = new ArrayList<>();

            Collection objList = (Collection) obj;

            if (!isValidElementCount(objList.size())) {
                return null;
            }

            for (Object element : objList) {
                if (element != null) {
                    Object normalizedValue = elementType.getNormalizedValue(element);

                    if (normalizedValue != null) {
                        ret.add(normalizedValue);
                    } else {
                        return null; // invalid element value
                    }
                } else {
                    ret.add(element);
                }
            }

            return ret;
        } else if (obj.getClass().isArray()) {
            List<Object> ret = new ArrayList<>();

            int arrayLen = Array.getLength(obj);

            if (!isValidElementCount(arrayLen)) {
                return null;
            }

            for (int i = 0; i < arrayLen; i++) {
                Object element = Array.get(obj, i);

                if (element != null) {
                    Object normalizedValue = elementType.getNormalizedValue(element);

                    if (normalizedValue != null) {
                        ret.add(normalizedValue);
                    } else {
                        return null; // invalid element value
                    }
                } else {
                    ret.add(element);
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
            if (obj instanceof List || obj instanceof Set) {
                Collection objList = (Collection) obj;

                if (!isValidElementCount(objList.size())) {
                    ret = false;

                    messages.add(objName + ": incorrect number of values. found=" + objList.size()
                            + "; expected: minCount=" + minCount + ", maxCount=" + maxCount);
                }

                int idx = 0;
                for (Object element : objList) {
                    ret = elementType.validateValue(element, objName + "[" + idx + "]", messages) && ret;
                    idx++;
                }
            } else if (obj.getClass().isArray()) {
                int arrayLen = Array.getLength(obj);

                if (!isValidElementCount(arrayLen)) {
                    ret = false;

                    messages.add(objName + ": incorrect number of values. found=" + arrayLen
                            + "; expected: minCount=" + minCount + ", maxCount=" + maxCount);
                }

                for (int i = 0; i < arrayLen; i++) {
                    ret = elementType.validateValue(Array.get(obj, i), objName + "[" + i + "]", messages) && ret;
                }
            } else {
                ret = false;

                messages.add(objName + "=" + obj + ": invalid value for type " + getTypeName());
            }
        }

        return ret;
    }

    private boolean isValidElementCount(int count) {
        if (minCount != COUNT_NOT_SET) {
            if (count < minCount) {
                return false;
            }
        }

        if (maxCount != COUNT_NOT_SET) {
            if (count > maxCount) {
                return false;
            }
        }

        return true;
    }
}

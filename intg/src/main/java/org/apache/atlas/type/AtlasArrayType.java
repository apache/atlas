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
    void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
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
    public boolean areEqualValues(Object val1, Object val2) {
        boolean ret = true;

        if (val1 == null) {
            ret = isEmptyArrayValue(val2);
        } else if (val2 == null) {
            ret = isEmptyArrayValue(val1);
        } else {
            if (val1.getClass().isArray() && val2.getClass().isArray()) {
                int len = Array.getLength(val1);

                if (len != Array.getLength(val2)) {
                    ret = false;
                } else {
                    for (int i = 0; i < len; i++) {
                        if (!elementType.areEqualValues(Array.get(val1, i), Array.get(val2, i))) {
                            ret = false;

                            break;
                        }
                    }
                }
            } else if ((val1 instanceof Set) && (val2 instanceof Set)) {
                Set set1 = (Set) val1;
                Set set2 = (Set) val2;

                if (set1.size() != set2.size()) {
                    ret = false;
                } else {
                    for (Object elem1 : set1) {
                        boolean foundInSet2 = false;

                        for (Object elem2 : set2) {
                            if (elementType.areEqualValues(elem1, elem2)) {
                                foundInSet2 = true;

                                break;
                            }
                        }

                        if (!foundInSet2) {
                            ret = false;

                            break;
                        }
                    }
                }
            } else {
                List list1 = getListFromValue(val1);

                if (list1 == null) {
                    ret = false;
                } else {
                    List list2 = getListFromValue(val2);

                    if (list2 == null) {
                        ret = false;
                    } else {
                        int len = list1.size();

                        if (len != list2.size()) {
                            ret = false;
                        } else {
                            for (int i = 0; i < len; i++) {
                                if (!elementType.areEqualValues(list1.get(i), list2.get(i))) {
                                    ret = false;

                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        return ret;
    }

    @Override
    public boolean isValidValueForUpdate(Object obj) {
        if (obj != null) {
            if (obj instanceof List || obj instanceof Set) {
                Collection objList = (Collection) obj;

                if (!isValidElementCount(objList.size())) {
                    return false;
                }

                for (Object element : objList) {
                    if (!elementType.isValidValueForUpdate(element)) {
                        return false;
                    }
                }
            } else if (obj.getClass().isArray()) {
                int arrayLen = Array.getLength(obj);

                if (!isValidElementCount(arrayLen)) {
                    return false;
                }

                for (int i = 0; i < arrayLen; i++) {
                    if (!elementType.isValidValueForUpdate(Array.get(obj, i))) {
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

        if (obj instanceof String){
             obj = AtlasType.fromJson(obj.toString(), List.class);
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
    public Collection<?> getNormalizedValueForUpdate(Object obj) {
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
                    Object normalizedValue = elementType.getNormalizedValueForUpdate(element);

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
                    Object normalizedValue = elementType.getNormalizedValueForUpdate(element);

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

    @Override
    public boolean validateValueForUpdate(Object obj, String objName, List<String> messages) {
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
                    ret = elementType.validateValueForUpdate(element, objName + "[" + idx + "]", messages) && ret;
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
                    ret = elementType.validateValueForUpdate(Array.get(obj, i), objName + "[" + i + "]", messages) && ret;
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
        AtlasType elementAttributeType = elementType.getTypeForAttribute();

        if (elementAttributeType == elementType) {
            return this;
        } else {
            AtlasType attributeType = new AtlasArrayType(elementAttributeType, minCount, maxCount);

            if (LOG.isDebugEnabled()) {
                LOG.debug("getTypeForAttribute(): {} ==> {}", getTypeName(), attributeType.getTypeName());
            }

            return attributeType;
        }
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

    private boolean isEmptyArrayValue(Object val) {
        if (val == null) {
            return true;
        } else if (val instanceof Collection) {
            return ((Collection) val).isEmpty();
        } else if (val.getClass().isArray()) {
            return Array.getLength(val) == 0;
        } else if (val instanceof String){
            List list = AtlasType.fromJson(val.toString(), List.class);

            return list == null || list.isEmpty();
        }

        return false;
    }

    private List getListFromValue(Object val) {
        final List ret;

        if (val instanceof List) {
            ret = (List) val;
        } else if (val instanceof Collection) {
            int len = ((Collection) val).size();

            ret = new ArrayList<>(len);

            for (Object elem : ((Collection) val)) {
                ret.add(elem);
            }
        } else if (val.getClass().isArray()) {
            int len = Array.getLength(val);

            ret = new ArrayList<>(len);

            for (int i = 0; i < len; i++) {
                ret.add(Array.get(val, i));
            }
        } else if (val instanceof String){
            ret = AtlasType.fromJson(val.toString(), List.class);
        } else {
            ret = null;
        }

        return ret;
    }
}

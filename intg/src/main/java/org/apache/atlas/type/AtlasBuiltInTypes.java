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


import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;


/**
 * Built-in types in Atlas.
 */
public class AtlasBuiltInTypes {

    /**
     * class that implements behaviour of boolean type.
     */
    public static class AtlasBooleanType extends AtlasType {
        private static final Boolean DEFAULT_VALUE = Boolean.FALSE;

        public AtlasBooleanType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_BOOLEAN, TypeCategory.PRIMITIVE);
        }

        @Override
        public Boolean createDefaultValue() {
            return DEFAULT_VALUE;
        }

        @Override
        public boolean isValidValue(Object obj) {
            return true;
        }

        @Override
        public Boolean getNormalizedValue(Object obj) {
            if (obj != null) {
                if (obj instanceof Boolean) {
                    return (Boolean)obj;
                } else {
                    return Boolean.valueOf(obj.toString());
                }
            }

            return null;
        }
    }

    /**
     * class that implements behaviour of byte type.
     */
    public static class AtlasByteType extends AtlasType {
        private static final Byte DEFAULT_VALUE = (byte) 0;

        public AtlasByteType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_BYTE, TypeCategory.PRIMITIVE);
        }

        @Override
        public Byte createDefaultValue() {
            return DEFAULT_VALUE;
        }

        @Override
        public boolean isValidValue(Object obj) {
            if (obj == null || obj instanceof Number) {
                return true;
            }

            return getNormalizedValue(obj) != null;
        }

        @Override
        public Byte getNormalizedValue(Object obj) {
            if (obj != null) {
                if (obj instanceof Byte) {
                    return (Byte) obj;
                } else if (obj instanceof Number) {
                    return ((Number) obj).byteValue();
                } else {
                    String strValue = obj.toString();

                    if (StringUtils.isNotEmpty(strValue)) {
                        return Byte.valueOf(strValue);
                    }
                }
            }

            return null;
        }
    }

    /**
     * class that implements behaviour of short type.
     */
    public static class AtlasShortType extends AtlasType {
        private static final Short DEFAULT_VALUE = (short) 0;

        public AtlasShortType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_SHORT, TypeCategory.PRIMITIVE);
        }

        @Override
        public Short createDefaultValue() {
            return DEFAULT_VALUE;
        }

        @Override
        public boolean isValidValue(Object obj) {
            if (obj == null || obj instanceof Number) {
                return true;
            }

            return getNormalizedValue(obj) != null;
        }

        @Override
        public Short getNormalizedValue(Object obj) {
            if (obj != null) {
                if (obj instanceof Short) {
                    return (Short)obj;
                } else if (obj instanceof Number) {
                    return ((Number) obj).shortValue();
                } else {
                    try {
                        return Short.valueOf(obj.toString());
                    } catch(NumberFormatException excp) {
                        // ignore
                    }
                }
            }

            return null;
        }
    }

    /**
     * class that implements behaviour of integer type.
     */
    public static class AtlasIntType extends AtlasType {
        private static final Integer DEFAULT_VALUE = 0;

        public AtlasIntType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_INT, TypeCategory.PRIMITIVE);
        }

        @Override
        public Integer createDefaultValue() {
            return DEFAULT_VALUE;
        }

        @Override
        public boolean isValidValue(Object obj) {
            if (obj == null || obj instanceof Number) {
                return true;
            }

            return getNormalizedValue(obj) != null;
        }

        @Override
        public Integer getNormalizedValue(Object obj) {
            if (obj != null) {
                if (obj instanceof Integer) {
                    return (Integer) obj;
                } else if (obj instanceof Number) {
                    return ((Number) obj).intValue();
                } else {
                    try {
                        return Integer.valueOf(obj.toString());
                    } catch (NumberFormatException excp) {
                        // ignore
                    }
                }
            }

            return null;
        }
    }

    /**
     * class that implements behaviour of long type.
     */
    public static class AtlasLongType extends AtlasType {
        private static final Long DEFAULT_VALUE = 0L;

        public AtlasLongType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_LONG, TypeCategory.PRIMITIVE);
        }

        @Override
        public Long createDefaultValue() {
            return DEFAULT_VALUE;
        }

        @Override
        public boolean isValidValue(Object obj) {
            if (obj == null || obj instanceof Number) {
                return true;
            }

            return getNormalizedValue(obj) != null;
        }

        @Override
        public Long getNormalizedValue(Object obj) {
            if (obj != null) {
                if (obj instanceof Long) {
                    return (Long) obj;
                } else if (obj instanceof Number) {
                    return ((Number) obj).longValue();
                } else {
                    try {
                        return Long.valueOf(obj.toString());
                    } catch (NumberFormatException excp) {
                        // ignore
                    }
                }
            }

            return null;
        }
    }

    /**
     * class that implements behaviour of float type.
     */
    public static class AtlasFloatType extends AtlasType {
        private static final Float DEFAULT_VALUE = 0f;

        public AtlasFloatType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_FLOAT, TypeCategory.PRIMITIVE);
        }

        @Override
        public Float createDefaultValue() {
            return DEFAULT_VALUE;
        }

        @Override
        public boolean isValidValue(Object obj) {
            if (obj == null || obj instanceof Number) {
                return true;
            }

            return getNormalizedValue(obj) != null;
        }

        @Override
        public Float getNormalizedValue(Object obj) {
            if (obj != null) {
                if (obj instanceof Float) {
                    return (Float) obj;
                } else if (obj instanceof Number) {
                    return ((Number) obj).floatValue();
                } else {
                    try {
                        return Float.valueOf(obj.toString());
                    } catch (NumberFormatException excp) {
                        // ignore
                    }
                }
            }

            return null;
        }
    }

    /**
     * class that implements behaviour of double type.
     */
    public static class AtlasDoubleType extends AtlasType {
        private static final Double DEFAULT_VALUE = 0d;

        public AtlasDoubleType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_DOUBLE, TypeCategory.PRIMITIVE);
        }

        @Override
        public Double createDefaultValue() {
            return DEFAULT_VALUE;
        }

        @Override
        public boolean isValidValue(Object obj) {
            if (obj == null || obj instanceof Number) {
                return true;
            }

            return getNormalizedValue(obj) != null;
        }

        @Override
        public Double getNormalizedValue(Object obj) {
            Double ret;

            if (obj != null) {
                if (obj instanceof Double) {
                    return (Double) obj;
                } else if (obj instanceof Number) {
                    return ((Number) obj).doubleValue();
                } else {
                    try {
                        return Double.valueOf(obj.toString());
                    } catch (NumberFormatException excp) {
                        // ignore
                    }
                }
            }

            return null;
        }
    }

    /**
     * class that implements behaviour of Java BigInteger type.
     */
    public static class AtlasBigIntegerType extends AtlasType {
        private static final BigInteger DEFAULT_VALUE = BigInteger.ZERO;

        public AtlasBigIntegerType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_BIGINTEGER, TypeCategory.PRIMITIVE);
        }

        @Override
        public BigInteger createDefaultValue() {
            return DEFAULT_VALUE;
        }

        @Override
        public boolean isValidValue(Object obj) {
            if (obj == null || obj instanceof Number) {
                return true;
            }

            return getNormalizedValue(obj) != null;
        }

        @Override
        public BigInteger getNormalizedValue(Object obj) {
            if (obj != null) {
                if (obj instanceof BigInteger) {
                    return (BigInteger) obj;
                } else if (obj instanceof BigDecimal) {
                    return ((BigDecimal) obj).toBigInteger();
                } else if (obj instanceof Number) {
                    return BigInteger.valueOf(((Number) obj).longValue());
                } else {
                    try {
                        return new BigDecimal(obj.toString()).toBigInteger();
                    } catch (NumberFormatException excp) {
                        // ignore
                    }
                }
            }

            return null;
        }
    }

    /**
     * class that implements behaviour of Java BigDecimal type.
     */
    public static class AtlasBigDecimalType extends AtlasType {
        private static final BigDecimal DEFAULT_VALUE = BigDecimal.ZERO;

        public AtlasBigDecimalType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_BIGDECIMAL, TypeCategory.PRIMITIVE);
        }

        @Override
        public BigDecimal createDefaultValue() {
            return DEFAULT_VALUE;
        }

        @Override
        public boolean isValidValue(Object obj) {
            if (obj == null || obj instanceof Number) {
                return true;
            }

            return getNormalizedValue(obj) != null;
        }

        @Override
        public BigDecimal getNormalizedValue(Object obj) {
            if (obj != null) {
                if (obj instanceof BigDecimal) {
                    return (BigDecimal) obj;
                } else if (obj instanceof BigInteger) {
                    return new BigDecimal((BigInteger) obj);
                } else if (obj instanceof Number) {
                    return obj.equals(0) ? BigDecimal.ZERO : BigDecimal.valueOf(((Number) obj).doubleValue());
                } else {
                    try {
                        return new BigDecimal(obj.toString());
                    } catch (NumberFormatException excp) {
                        // ignore
                    }
                }
            }

            return null;
        }
    }

    /**
     * class that implements behaviour of Date type.
     */
    public static class AtlasDateType extends AtlasType {
        private static final Date DEFAULT_VALUE = new Date(0);

        public AtlasDateType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_DATE, TypeCategory.PRIMITIVE);
        }

        @Override
        public Date createDefaultValue() {
            return DEFAULT_VALUE;
        }

        @Override
        public boolean isValidValue(Object obj) {
            if (obj == null || obj instanceof Date || obj instanceof Number) {
                return true;
            }

            if (obj instanceof String && StringUtils.isEmpty((String) obj)) {
                return true;
            }

            return getNormalizedValue(obj) != null;
        }

        @Override
        public Date getNormalizedValue(Object obj) {
            if (obj != null) {
                if (obj instanceof Date) {
                    return (Date) obj;
                } else if (obj instanceof Number) {
                    return new Date(((Number) obj).longValue());
                } else {
                    try {
                        return AtlasBaseTypeDef.DATE_FORMATTER.parse(obj.toString());
                    } catch (ParseException excp) {
                        try { // try to read it as a number
                            long longDate = Long.valueOf(obj.toString());
                            return new Date(longDate);
                        } catch (NumberFormatException e) {
                            // ignore
                        }
                    }
                }
            }

            return null;
        }
    }

    /**
     * class that implements behaviour of String type.
     */
    public static class AtlasStringType extends AtlasType {
        private static final String DEFAULT_VALUE = "";
        private static final String OPTIONAL_DEFAULT_VALUE = null;

        public AtlasStringType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_STRING, TypeCategory.PRIMITIVE);
        }

        @Override
        public String createDefaultValue() {
            return DEFAULT_VALUE;
        }

        @Override
        public Object createOptionalDefaultValue() {
            return OPTIONAL_DEFAULT_VALUE;
        }

        @Override
        public boolean isValidValue(Object obj) {
            return true;
        }

        @Override
        public String getNormalizedValue(Object obj) {
            if (obj != null) {
                return obj.toString();
            }

            return null;
        }
    }

    /**
     * class that implements behaviour of Atlas object-id type.
     */
    public static class AtlasObjectIdType extends AtlasType {
        public static final String DEFAULT_UNASSIGNED_GUID = "-1";

        private final String objectType;

        public AtlasObjectIdType() {
            super(AtlasBaseTypeDef.ATLAS_TYPE_OBJECT_ID, TypeCategory.OBJECT_ID_TYPE);

            objectType = AtlasBaseTypeDef.ATLAS_TYPE_ASSET;
        }

        public AtlasObjectIdType(String objectType) {
            super(AtlasBaseTypeDef.ATLAS_TYPE_OBJECT_ID, TypeCategory.OBJECT_ID_TYPE);

            this.objectType = objectType;
        }

        public String getObjectType() { return objectType; }

        @Override
        public AtlasObjectId createDefaultValue() {
            return new AtlasObjectId(DEFAULT_UNASSIGNED_GUID, objectType);
        }

        @Override
        public boolean isValidValue(Object obj) {
            if (obj == null || obj instanceof AtlasObjectId) {
                return true;
            } else if (obj instanceof Map) {
                return isValidMap((Map)obj);
            }

            return getNormalizedValue(obj) != null;
        }

        @Override
        public AtlasObjectId getNormalizedValue(Object obj) {
            if (obj != null) {
                if (obj instanceof AtlasObjectId) {
                    return (AtlasObjectId) obj;
                } else if (obj instanceof Map) {
                    Map map = (Map) obj;

                    if (isValidMap(map)) {
                        return new AtlasObjectId(map);
                    }
                }
            }

            return null;
        }

        private boolean isValidMap(Map map) {
            Object guid = map.get(AtlasObjectId.KEY_GUID);

            if (guid != null && StringUtils.isNotEmpty(guid.toString())) {
                return true;
            } else {
                Object typeName = map.get(AtlasObjectId.KEY_TYPENAME);
                if (typeName != null && StringUtils.isNotEmpty(typeName.toString())) {
                    Object uniqueAttributes = map.get(AtlasObjectId.KEY_UNIQUE_ATTRIBUTES);

                    if (uniqueAttributes instanceof Map && MapUtils.isNotEmpty((Map) uniqueAttributes)) {
                        return true;
                    }
                }
            }

            return false;
        }
    }
}

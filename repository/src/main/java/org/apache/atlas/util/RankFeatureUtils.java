/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.util;

import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Utility class for handling Elasticsearch rank_feature field constraints.
 *
 * Elasticsearch rank_feature fields require positive normal floats.
 * The minimum positive normal float value is Float.MIN_NORMAL (1.17549435E-38).
 * Values below this threshold will cause ES indexing failures.
 */
public class RankFeatureUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RankFeatureUtils.class);

    public static final String RANK_FEATURE_TYPE = "rank_feature";
    public static final String ES_FIELD_TYPE_KEY = "type";

    /**
     * Minimum positive normal float value required by Elasticsearch rank_feature fields.
     */
    public static final float ES_MIN_POSITIVE_NORMAL = Float.MIN_NORMAL; // 1.17549435E-38

    private RankFeatureUtils() {
        // Utility class - prevent instantiation
    }

    /**
     * Checks if an attribute definition has a rank_feature ES field type.
     *
     * @param attrDef the attribute definition to check
     * @return true if the attribute has a rank_feature field in indexTypeESFields
     */
    public static boolean isRankFeatureField(AtlasAttributeDef attrDef) {
        if (attrDef == null) {
            return false;
        }

        HashMap<String, HashMap<String, Object>> indexTypeESFields = attrDef.getIndexTypeESFields();
        if (MapUtils.isEmpty(indexTypeESFields)) {
            return false;
        }

        return indexTypeESFields.values().stream()
                .anyMatch(config -> config != null && RANK_FEATURE_TYPE.equals(config.get(ES_FIELD_TYPE_KEY)));
    }

    /**
     * Gets the minimum allowed value for a rank_feature attribute.
     * Uses the defaultValue from the attribute definition if available,
     * otherwise falls back to Float.MIN_NORMAL.
     *
     * @param attrDef the attribute definition
     * @return the minimum allowed value for the rank_feature field
     */
    public static float getMinimumValue(AtlasAttributeDef attrDef) {
        if (attrDef != null) {
            String defaultValue = attrDef.getDefaultValue();
            if (StringUtils.isNotEmpty(defaultValue)) {
                try {
                    return Float.parseFloat(defaultValue);
                } catch (NumberFormatException e) {
                    LOG.debug("Failed to parse defaultValue '{}' as float, using ES_MIN_POSITIVE_NORMAL", defaultValue);
                }
            }
        }
        return ES_MIN_POSITIVE_NORMAL;
    }

    /**
     * Normalizes a numeric value for a rank_feature field using cached attribute metadata.
     * This is the preferred method for hot paths (e.g., EntityGraphMapper.mapAttribute)
     * as it uses O(1) cached checks instead of iterating through indexTypeESFields.
     *
     * @param value the value to normalize
     * @param attribute the AtlasAttribute with cached rank_feature metadata
     * @return the normalized value, or the original value if not a rank_feature field
     */
    public static Object normalizeValue(Object value, AtlasAttribute attribute) {
        if (value == null || attribute == null) {
            return value;
        }

        if (!attribute.isRankFeatureField()) {
            return value;  // O(1) check
        }

        if (value instanceof Number) {
            float floatValue = ((Number) value).floatValue();
            float minValue = attribute.getRankFeatureMinValue();  // O(1) access

            if (floatValue < minValue) {
                LOG.debug("Normalizing rank_feature value for attribute '{}' from {} to {}",
                        attribute.getName(), floatValue, minValue);
                return minValue;
            }
        }

        return value;
    }

    /**
     * Checks if a numeric value is valid for a rank_feature field.
     *
     * @param value the value to check
     * @param attrDef the attribute definition
     * @return true if the value is valid (>= minimum threshold), false otherwise
     */
    public static boolean isValidRankFeatureValue(Number value, AtlasAttributeDef attrDef) {
        if (value == null || attrDef == null || !isRankFeatureField(attrDef)) {
            return true;
        }

        float floatValue = value.floatValue();
        float minValue = getMinimumValue(attrDef);

        return floatValue >= minValue;
    }
}

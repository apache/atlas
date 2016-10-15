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

import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_PREFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_SUFFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_PREFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_SUFFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_KEY_VAL_SEP;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility methods for AtlasType/AtlasTypeDef.
 */
public class AtlasTypeUtil {
    private static final Set<String> ATLAS_BUILTIN_TYPENAMES = new HashSet<String>();

    static {
        for (String typeName : AtlasBaseTypeDef.ATLAS_BUILTIN_TYPES) {
            ATLAS_BUILTIN_TYPENAMES.add(typeName);
        }
    }

    public static Set<String> getReferencedTypeNames(String typeName) {
        Set<String> ret = new HashSet<String>();

        getReferencedTypeNames(typeName, ret);

        return ret;
    }

    public static boolean isBuiltInType(String typeName) {
        return ATLAS_BUILTIN_TYPENAMES.contains(typeName);
    }

    public static boolean isArrayType(String typeName) {
        return StringUtils.startsWith(typeName, ATLAS_TYPE_ARRAY_PREFIX)
            && StringUtils.endsWith(typeName, ATLAS_TYPE_ARRAY_SUFFIX);
    }

    public static boolean isMapType(String typeName) {
        return StringUtils.startsWith(typeName, ATLAS_TYPE_MAP_PREFIX)
                && StringUtils.endsWith(typeName, ATLAS_TYPE_MAP_SUFFIX);
    }

    public static String getStringValue(Map map, Object key) {
        Object ret = map != null ? map.get(key) : null;

        return ret != null ? ret.toString() : null;
    }

    private static void getReferencedTypeNames(String typeName, Set<String> referencedTypeNames) {
        if (StringUtils.isNotBlank(typeName) && !referencedTypeNames.contains(typeName)) {
            if (typeName.startsWith(ATLAS_TYPE_ARRAY_PREFIX) && typeName.endsWith(ATLAS_TYPE_ARRAY_SUFFIX)) {
                int    startIdx        = ATLAS_TYPE_ARRAY_PREFIX.length();
                int    endIdx          = typeName.length() - ATLAS_TYPE_ARRAY_SUFFIX.length();
                String elementTypeName = typeName.substring(startIdx, endIdx);

                getReferencedTypeNames(elementTypeName, referencedTypeNames);
            } else if (typeName.startsWith(ATLAS_TYPE_MAP_PREFIX) && typeName.endsWith(ATLAS_TYPE_MAP_SUFFIX)) {
                int      startIdx      = ATLAS_TYPE_MAP_PREFIX.length();
                int      endIdx        = typeName.length() - ATLAS_TYPE_MAP_SUFFIX.length();
                String[] keyValueTypes = typeName.substring(startIdx, endIdx).split(ATLAS_TYPE_MAP_KEY_VAL_SEP, 2);
                String   keyTypeName   = keyValueTypes.length > 0 ? keyValueTypes[0] : null;
                String   valueTypeName = keyValueTypes.length > 1 ? keyValueTypes[1] : null;

                getReferencedTypeNames(keyTypeName, referencedTypeNames);
                getReferencedTypeNames(valueTypeName, referencedTypeNames);
            } else {
                referencedTypeNames.add(typeName);
            }
        }

    }
}

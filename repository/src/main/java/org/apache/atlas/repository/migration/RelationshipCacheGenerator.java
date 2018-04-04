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

package org.apache.atlas.repository.migration;

import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RelationshipCacheGenerator {

    public static Map<String, String> get(AtlasTypeRegistry typeRegistry) {
        Map<String, String>               ret               = new HashMap<>();
        Collection<AtlasRelationshipType> relationshipTypes = typeRegistry.getAllRelationshipTypes();

        for (AtlasRelationshipType rt : relationshipTypes) {
            AtlasRelationshipDef rd          = rt.getRelationshipDef();
            String               relTypeName = rt.getTypeName();

            add(ret, getKey(rd.getEndDef1(), rt.getEnd1Type()), relTypeName);
            add(ret, getKey(rd.getEndDef2(), rt.getEnd2Type()), relTypeName);
        }

        return ret;
    }

    private static String getKey(AtlasRelationshipEndDef ed, AtlasEntityType rt) {
        return getKey(ed.getIsLegacyAttribute(), rt.getTypeName(), ed.getName());
    }

    private static String getKey(String lhs, String rhs) {
        return String.format("%s%s.%s", Constants.INTERNAL_PROPERTY_KEY_PREFIX, lhs, rhs);
    }

    private static String getKey(boolean isLegacy, String typeName, String name) {
        if(!isLegacy) {
            return "";
        }

        return getKey(typeName, name);
    }

    private static void add(Map<String, String> map, String key, String value) {
        if(StringUtils.isEmpty(key) || map.containsKey(key)) {
            return;
        }

        map.put(key, value);
    }
}

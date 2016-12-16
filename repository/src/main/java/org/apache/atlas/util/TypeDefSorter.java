/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.util;

import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TypeDefSorter {
    private static final Logger LOG = LoggerFactory.getLogger(TypeDefSorter.class);

    public static <T extends AtlasStructDef> List<T> sortTypes(List<T> types) {
        Map<String, T> typesByName = new HashMap<>();
        for (T type : types) {
            typesByName.put(type.getName(), type);
        }
        List<T> result = new ArrayList<>(types.size());
        Set<T> processed = new HashSet<>();
        for (T type : types) {
            addToResult(type, result, processed, typesByName);
        }
        return result;
    }

    private static <T extends AtlasStructDef> void addToResult(T type, List<T> result,
                                                                 Set<T> processed,
                                                               Map<String, T> typesByName) {
        if (processed.contains(type)) {
            return;
        }
        processed.add(type);
        Set<String> superTypeNames = new HashSet<>();
        if (type.getClass().equals(AtlasClassificationDef.class)) {
            try {
                AtlasClassificationDef classificationDef = AtlasClassificationDef.class.cast(type);
                superTypeNames.addAll(classificationDef.getSuperTypes());
            } catch (ClassCastException ex) {
                LOG.warn("Casting to ClassificationDef failed");
            }
        }
        if (type.getClass().equals(AtlasEntityDef.class)) {
            try {
                AtlasEntityDef entityDef = AtlasEntityDef.class.cast(type);
                superTypeNames.addAll(entityDef.getSuperTypes());
            } catch (ClassCastException ex) {
                LOG.warn("Casting to AtlasEntityDef failed");
            }
        }

        for (String superTypeName : superTypeNames) {
            // Recursively add any supertypes first to the result.
            T superType = typesByName.get(superTypeName);
            if (superType != null) {
                addToResult(superType, result, processed, typesByName);
            }
        }
        result.add(type);
    }
}
